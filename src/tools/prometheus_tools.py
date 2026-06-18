"""
LUMINO MCP Server - CI/CD Performance Baselining Tool

Extracted from server-mcp.py as part of issue #118 (sub-task of #30).

Contains:
  - ci_cd_performance_baselining_tool_impl : implementation of the public @mcp.tool()

Dependencies on module-level server state (k8s_custom_api, k8s_core_api) are
injected at call time to keep this module independently importable and testable.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np

from tools.prometheus_query import _execute_prometheus_query_internal

logger = logging.getLogger(__name__)


async def ci_cd_performance_baselining_tool_impl(
    pipeline_names: Optional[List[str]] = None,
    baseline_period: str = "30d",
    deviation_threshold: float = 2.0,
    include_task_level: bool = True,
    k8s_custom_api: Any = None,
    k8s_core_api: Any = None,
) -> Dict[str, Any]:
    """
    Implementation of ci_cd_performance_baselining_tool.

    Establish performance baselines for pipelines and flag runs deviating from
    historical norms using Prometheus metrics from the Tekton controller.

    Args:
        pipeline_names: Pipelines to analyze (default: all).
        baseline_period: "7d", "30d" (default), or "90d".
        deviation_threshold: Std deviations to trigger alerts (default: 2.0).
        include_task_level: Include task-level analysis (default: True).
        k8s_custom_api: Kubernetes CustomObjectsApi client (injected by wrapper).
        k8s_core_api: Kubernetes CoreV1Api client (injected by wrapper).

    Returns:
        Dict: Baselines, recent runs analysis, trends, and optimization opportunities.
    """
    if not k8s_custom_api or not k8s_core_api:
        return {"error": "Kubernetes client not available."}

    logger.info(
        f"Starting CI/CD performance baselining analysis with period: {baseline_period} "
        "using Prometheus metrics"
    )

    try:
        # Initialize result structure
        result = {
            "pipeline_baselines": [],
            "performance_trends": {
                "improving_pipelines": [],
                "degrading_pipelines": [],
                "stable_pipelines": [],
                "most_variable_pipelines": []
            },
            "optimization_opportunities": [],
            "data_source": "prometheus"
        }

        # Define all Prometheus queries upfront
        duration_count_query = "sum by (namespace, status) (tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count)"
        duration_sum_query = "sum by (namespace, status) (tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum)"
        avg_duration_query = "sum by (namespace) (tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum) / sum by (namespace) (tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count)"
        p16_query = "histogram_quantile(0.16, sum by (namespace, le) (rate(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_bucket[1h])))"
        p84_query = "histogram_quantile(0.84, sum by (namespace, le) (rate(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_bucket[1h])))"
        recent_avg_query = "sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum[24h])) / sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[24h]))"
        historical_avg_query = f"sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum[{baseline_period}])) / sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[{baseline_period}]))"
        recent_success_query = "sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count{status='success'}[24h])) / sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[24h])) * 100"
        historical_success_query = f"sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count{{status='success'}}[{baseline_period}])) / sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[{baseline_period}])) * 100"
        reconcile_query = "sum by (namespace_name, success) (rate(tekton_pipelines_controller_reconcile_count[1h]))"

        logger.info("Querying Prometheus for Tekton pipeline metrics (10 queries in parallel)...")

        # Execute ALL queries in parallel for maximum performance
        (
            count_result,
            sum_result,
            avg_result,
            p16_result,
            p84_result,
            recent_avg_result,
            historical_avg_result,
            recent_success_result,
            historical_success_result,
            reconcile_result
        ) = await asyncio.gather(
            _execute_prometheus_query_internal(duration_count_query),
            _execute_prometheus_query_internal(duration_sum_query),
            _execute_prometheus_query_internal(avg_duration_query),
            _execute_prometheus_query_internal(p16_query),
            _execute_prometheus_query_internal(p84_query),
            _execute_prometheus_query_internal(recent_avg_query),
            _execute_prometheus_query_internal(historical_avg_query),
            _execute_prometheus_query_internal(recent_success_query),
            _execute_prometheus_query_internal(historical_success_query),
            _execute_prometheus_query_internal(reconcile_query)
        )

        logger.info("All Prometheus queries completed")

        if not count_result.get("success") or not sum_result.get("success"):
            logger.warning("Prometheus queries failed, falling back to Kubernetes API")
            result["data_source"] = "kubernetes_api_fallback"
            result["prometheus_error"] = count_result.get("error") or sum_result.get("error")
            return result

        # Parse Prometheus results into namespace-level statistics
        namespace_stats = {}

        # Process count data
        for item in count_result.get("data", []):
            metric = item.get("metric", {})
            namespace = metric.get("namespace", "unknown")
            status = metric.get("status", "unknown")
            count = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

            if namespace not in namespace_stats:
                namespace_stats[namespace] = {
                    "success_count": 0,
                    "failed_count": 0,
                    "total_duration_sum": 0,
                    "total_count": 0
                }

            if status == "success":
                namespace_stats[namespace]["success_count"] = count
            elif status == "failed":
                namespace_stats[namespace]["failed_count"] = count
            namespace_stats[namespace]["total_count"] += count

        # Process duration sum data
        for item in sum_result.get("data", []):
            metric = item.get("metric", {})
            namespace = metric.get("namespace", "unknown")
            duration_sum = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

            if namespace in namespace_stats:
                namespace_stats[namespace]["total_duration_sum"] += duration_sum

        # Process average duration data
        if avg_result.get("success"):
            for item in avg_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                avg_duration = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

                if namespace in namespace_stats and not np.isnan(avg_duration):
                    namespace_stats[namespace]["avg_duration"] = avg_duration

        # Store percentile data for std deviation calculation (std ~ (P84 - P16) / 2)
        percentile_data = {}
        if p16_result.get("success"):
            for item in p16_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                p16_val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in percentile_data:
                    percentile_data[namespace] = {"p16": 0, "p84": 0}
                if not np.isnan(p16_val) and not np.isinf(p16_val):
                    percentile_data[namespace]["p16"] = p16_val

        if p84_result.get("success"):
            for item in p84_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                p84_val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in percentile_data:
                    percentile_data[namespace] = {"p16": 0, "p84": 0}
                if not np.isnan(p84_val) and not np.isinf(p84_val):
                    percentile_data[namespace]["p84"] = p84_val

        # Store trend data for each namespace (recent vs historical comparison)
        trend_data = {}

        if recent_avg_result.get("success"):
            for item in recent_avg_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in trend_data:
                    trend_data[namespace] = {"recent_avg": 0, "historical_avg": 0, "recent_success": 0, "historical_success": 0}
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["recent_avg"] = val

        if historical_avg_result.get("success"):
            for item in historical_avg_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in trend_data:
                    trend_data[namespace] = {"recent_avg": 0, "historical_avg": 0, "recent_success": 0, "historical_success": 0}
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["historical_avg"] = val

        if recent_success_result.get("success"):
            for item in recent_success_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in trend_data:
                    trend_data[namespace] = {"recent_avg": 0, "historical_avg": 0, "recent_success": 0, "historical_success": 0}
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["recent_success"] = val

        if historical_success_result.get("success"):
            for item in historical_success_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in trend_data:
                    trend_data[namespace] = {"recent_avg": 0, "historical_avg": 0, "recent_success": 0, "historical_success": 0}
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["historical_success"] = val

        reconcile_stats = {}
        if reconcile_result.get("success"):
            for item in reconcile_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace_name", "unknown")
                success = metric.get("success", "false")
                rate_val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

                if namespace not in reconcile_stats:
                    reconcile_stats[namespace] = {"success_rate": 0, "failure_rate": 0}

                if success == "true":
                    reconcile_stats[namespace]["success_rate"] = rate_val
                else:
                    reconcile_stats[namespace]["failure_rate"] = rate_val

        # Filter namespaces by pipeline_names if specified
        filtered_namespaces = namespace_stats.keys()
        if pipeline_names:
            filtered_namespaces = [ns for ns in filtered_namespaces if any(pn in ns for pn in pipeline_names)]

        # Build baseline entries for each namespace
        for namespace in filtered_namespaces:
            stats = namespace_stats[namespace]

            if stats["total_count"] == 0:
                continue

            total_count = stats["total_count"]
            success_count = stats["success_count"]
            failed_count = stats["failed_count"]
            avg_duration = stats.get("avg_duration", 0)

            success_rate = (success_count / total_count * 100) if total_count > 0 else 0

            # Calculate std deviation from histogram percentiles (P84 - P16) / 2
            pdata = percentile_data.get(namespace, {"p16": 0, "p84": 0})
            if pdata["p84"] > pdata["p16"] and pdata["p84"] > 0:
                estimated_std = (pdata["p84"] - pdata["p16"]) / 2.0
            else:
                estimated_std = avg_duration * 0.4

            recon = reconcile_stats.get(namespace, {"success_rate": 0, "failure_rate": 0})
            reconcile_health = "healthy"
            if recon["failure_rate"] > recon["success_rate"]:
                reconcile_health = "degraded"
            elif recon["failure_rate"] > 0.5:
                reconcile_health = "warning"

            # Binomial standard error for success rate confidence interval
            p = success_rate / 100.0
            if total_count > 0 and 0 < p < 1:
                success_rate_se = np.sqrt(p * (1 - p) / total_count) * 100
            else:
                success_rate_se = 0

            baseline_metrics = {
                "duration": {
                    "mean_seconds": avg_duration,
                    "std_seconds": estimated_std,
                    "upper_bound": avg_duration + (deviation_threshold * estimated_std),
                    "lower_bound": max(0, avg_duration - (deviation_threshold * estimated_std))
                },
                "success_rate": {
                    "mean_percent": success_rate,
                    "std_percent": success_rate_se,
                    "lower_bound": max(0, success_rate - (deviation_threshold * success_rate_se)),
                    "upper_bound": min(100, success_rate + (deviation_threshold * success_rate_se))
                },
                "reconciliation": {
                    "success_rate_per_second": recon["success_rate"],
                    "failure_rate_per_second": recon["failure_rate"],
                    "health": reconcile_health
                }
            }

            ns_trend = trend_data.get(namespace, {"recent_avg": 0, "historical_avg": 0, "recent_success": 0, "historical_success": 0})
            recent_avg = ns_trend["recent_avg"]
            historical_avg = ns_trend["historical_avg"]
            recent_success = ns_trend["recent_success"]
            historical_success = ns_trend["historical_success"]

            if historical_avg > 0 and recent_avg > 0:
                duration_change_pct = ((recent_avg - historical_avg) / historical_avg) * 100
            else:
                duration_change_pct = 0

            success_change = recent_success - historical_success if (recent_success > 0 or historical_success > 0) else 0

            significance_threshold = 10.0 / deviation_threshold
            has_recent_data = (recent_avg > 0 or recent_success > 0)

            if not has_recent_data:
                trend = "No recent activity (inactive in last 24h)"
                trend_direction = "inactive"
            elif abs(duration_change_pct) < significance_threshold and abs(success_change) < significance_threshold:
                trend = "Stable performance (no significant trend)"
                trend_direction = "stable"
            elif duration_change_pct < -significance_threshold or success_change > significance_threshold:
                trend = f"Performance improving: duration {duration_change_pct:+.1f}%, success rate {success_change:+.1f}%"
                trend_direction = "improving"
            elif duration_change_pct > significance_threshold or success_change < -significance_threshold:
                trend = f"Performance degrading: duration {duration_change_pct:+.1f}%, success rate {success_change:+.1f}%"
                trend_direction = "degrading"
            else:
                trend = f"Slight variation: duration {duration_change_pct:+.1f}%, success rate {success_change:+.1f}%"
                trend_direction = "variable"

            pipeline_baseline = {
                "pipeline_name": namespace,
                "namespace": namespace,
                "cluster": "current-cluster",
                "baseline_metrics": baseline_metrics,
                "data_points": int(total_count),
                "success_count": int(success_count),
                "failed_count": int(failed_count),
                "last_updated": datetime.now().isoformat(),
                "trend": trend,
                "trend_metrics": {
                    "recent_avg_duration": recent_avg,
                    "historical_avg_duration": historical_avg,
                    "duration_change_pct": duration_change_pct,
                    "recent_success_rate": recent_success,
                    "historical_success_rate": historical_success,
                    "success_rate_change": success_change,
                    "comparison_period": f"24h vs {baseline_period}"
                }
            }

            result["pipeline_baselines"].append(pipeline_baseline)

            if trend_direction == "improving":
                result["performance_trends"]["improving_pipelines"].append({
                    "pipeline": namespace,
                    "trend": trend,
                    "avg_duration": avg_duration,
                    "success_rate": success_rate,
                    "duration_change_pct": duration_change_pct,
                    "success_rate_change": success_change
                })
            elif trend_direction == "degrading":
                result["performance_trends"]["degrading_pipelines"].append({
                    "pipeline": namespace,
                    "trend": trend,
                    "avg_duration": avg_duration,
                    "success_rate": success_rate,
                    "duration_change_pct": duration_change_pct,
                    "success_rate_change": success_change
                })
            elif trend_direction in ("stable", "inactive"):
                result["performance_trends"]["stable_pipelines"].append({
                    "pipeline": namespace,
                    "trend": trend,
                    "avg_duration": avg_duration,
                    "success_rate": success_rate
                })

            if recon["failure_rate"] > 1.0:
                result["performance_trends"]["most_variable_pipelines"].append({
                    "pipeline": namespace,
                    "failure_rate": recon["failure_rate"],
                    "avg_duration": avg_duration
                })

            if avg_duration > 600:
                result["optimization_opportunities"].append({
                    "pipeline": namespace,
                    "opportunity": "Long execution time optimization",
                    "potential_improvement": f"Pipeline averages {avg_duration/60:.1f} minutes - consider task parallelization or caching",
                    "complexity": "medium",
                    "avg_duration_seconds": avg_duration
                })

            if success_rate < 80:
                result["optimization_opportunities"].append({
                    "pipeline": namespace,
                    "opportunity": "Reliability improvement",
                    "potential_improvement": f"Success rate is {success_rate:.1f}% - investigate common failure patterns",
                    "complexity": "high",
                    "current_success_rate": success_rate
                })

            if reconcile_health == "degraded":
                result["optimization_opportunities"].append({
                    "pipeline": namespace,
                    "opportunity": "Reconciliation health improvement",
                    "potential_improvement": f"High reconciliation failure rate ({recon['failure_rate']:.2f}/s) - check controller logs and resource limits",
                    "complexity": "high",
                    "failure_rate": recon["failure_rate"]
                })

        # Task-level analysis if requested
        if include_task_level:
            logger.info("Performing task-level analysis...")
            result["task_level_analysis"] = {
                "task_baselines": [],
                "slowest_tasks": [],
                "most_failed_tasks": []
            }

            task_duration_query = f"sum by (task, namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum[{baseline_period}])) / sum by (task, namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[{baseline_period}]))"
            task_count_query = f"sum by (task, namespace, status) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[{baseline_period}]))"

            task_duration_result = await _execute_prometheus_query_internal(task_duration_query)
            task_count_result = await _execute_prometheus_query_internal(task_count_query)

            task_stats = {}

            if task_duration_result.get("success"):
                for item in task_duration_result.get("data", []):
                    metric = item.get("metric", {})
                    task_name = metric.get("task", "unknown")
                    namespace = metric.get("namespace", "unknown")
                    avg_duration = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

                    if np.isnan(avg_duration) or np.isinf(avg_duration):
                        continue

                    key = f"{namespace}/{task_name}"
                    if key not in task_stats:
                        task_stats[key] = {"task": task_name, "namespace": namespace, "avg_duration": 0, "success_count": 0, "failed_count": 0, "total_count": 0}
                    task_stats[key]["avg_duration"] = avg_duration

            if task_count_result.get("success"):
                for item in task_count_result.get("data", []):
                    metric = item.get("metric", {})
                    task_name = metric.get("task", "unknown")
                    namespace = metric.get("namespace", "unknown")
                    status = metric.get("status", "unknown")
                    count = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

                    if np.isnan(count) or np.isinf(count):
                        continue

                    key = f"{namespace}/{task_name}"
                    if key not in task_stats:
                        task_stats[key] = {"task": task_name, "namespace": namespace, "avg_duration": 0, "success_count": 0, "failed_count": 0, "total_count": 0}

                    if status == "success":
                        task_stats[key]["success_count"] = count
                    elif status == "failed":
                        task_stats[key]["failed_count"] = count
                    task_stats[key]["total_count"] += count

            unknown_task_count = 0
            for key, stats in task_stats.items():
                if stats["total_count"] < 1:
                    continue

                if stats["task"] == "unknown":
                    unknown_task_count += 1
                    continue

                task_success_rate = (stats["success_count"] / stats["total_count"] * 100) if stats["total_count"] > 0 else 0

                task_baseline = {
                    "task": stats["task"],
                    "namespace": stats["namespace"],
                    "avg_duration_seconds": stats["avg_duration"],
                    "total_runs": int(stats["total_count"]),
                    "success_count": int(stats["success_count"]),
                    "failed_count": int(stats["failed_count"]),
                    "success_rate": task_success_rate
                }
                result["task_level_analysis"]["task_baselines"].append(task_baseline)

            if unknown_task_count > 0 and len(result["task_level_analysis"]["task_baselines"]) == 0:
                result["task_level_analysis"]["note"] = (
                    f"Task-level analysis unavailable: Prometheus metrics do not include 'task' labels. "
                    f"Found {unknown_task_count} namespace-level aggregations. "
                    "For task-level details, query TaskRun resources directly via Kubernetes API."
                )
                logger.info(f"Task-level analysis: No task labels in Prometheus metrics ({unknown_task_count} namespaces without task granularity)")

            result["task_level_analysis"]["task_baselines"].sort(key=lambda x: x.get("avg_duration_seconds", 0) or 0, reverse=True)
            result["task_level_analysis"]["slowest_tasks"] = result["task_level_analysis"]["task_baselines"][:10]

            failed_tasks = [t for t in result["task_level_analysis"]["task_baselines"] if t["failed_count"] > 0]
            failed_tasks.sort(key=lambda x: x["failed_count"], reverse=True)
            result["task_level_analysis"]["most_failed_tasks"] = failed_tasks[:10]

            logger.info(f"Task-level analysis completed: {len(result['task_level_analysis']['task_baselines'])} tasks analyzed (filtered {unknown_task_count} 'unknown' entries)")

        # Sort results for better presentation
        result["pipeline_baselines"].sort(key=lambda x: x.get("data_points", 0), reverse=True)
        result["performance_trends"]["improving_pipelines"].sort(key=lambda x: x.get("avg_duration", 0))
        result["performance_trends"]["degrading_pipelines"].sort(key=lambda x: x.get("avg_duration", 0), reverse=True)
        result["performance_trends"]["most_variable_pipelines"].sort(key=lambda x: x.get("failure_rate", 0), reverse=True)

        result["summary"] = {
            "total_namespaces_analyzed": len(result["pipeline_baselines"]),
            "total_taskruns_tracked": sum(b.get("data_points", 0) for b in result["pipeline_baselines"]),
            "total_successes": sum(b.get("success_count", 0) for b in result["pipeline_baselines"]),
            "total_failures": sum(b.get("failed_count", 0) for b in result["pipeline_baselines"]),
            "namespaces_needing_attention": len([b for b in result["pipeline_baselines"]
                                                  if b.get("baseline_metrics", {}).get("success_rate", {}).get("mean_percent", 100) < 80]),
            "optimization_opportunities_count": len(result["optimization_opportunities"])
        }

        logger.info(
            f"Performance baselining completed. Analyzed {len(result['pipeline_baselines'])} namespaces, "
            f"tracking {result['summary']['total_taskruns_tracked']} total TaskRuns"
        )

        return result

    except Exception as e:
        logger.error(f"Error in CI/CD performance baselining: {str(e)}", exc_info=True)
        return {
            "pipeline_baselines": [],
            "performance_trends": {
                "improving_pipelines": [],
                "degrading_pipelines": [],
                "stable_pipelines": [],
                "most_variable_pipelines": []
            },
            "optimization_opportunities": [],
            "error": str(e)
        }
