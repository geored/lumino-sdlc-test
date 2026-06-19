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
                "most_variable_pipelines": [],
            },
            "optimization_opportunities": [],
            "data_source": "prometheus",
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

        logger.info(
            "Querying Prometheus for Tekton pipeline metrics (10 queries in parallel)..."
        )

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
            reconcile_result,
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
            _execute_prometheus_query_internal(reconcile_query),
        )

        logger.info("All Prometheus queries completed")

        if not count_result.get("success") or not sum_result.get("success"):
            logger.warning("Prometheus queries failed, falling back to Kubernetes API")
            result["data_source"] = "kubernetes_api_fallback"
            result["prometheus_error"] = count_result.get("error") or sum_result.get(
                "error"
            )
            return result

        # Parse Prometheus results into namespace-level statistics
        namespace_stats = {}

        # Process count data
        for item in count_result.get("data", []):
            metric = item.get("metric", {})
            namespace = metric.get("namespace", "unknown")
            status = metric.get("status", "unknown")
            count = (
                float(item.get("value", [0, 0])[1])
                if isinstance(item.get("value"), list)
                else 0
            )

            if namespace not in namespace_stats:
                namespace_stats[namespace] = {
                    "success_count": 0,
                    "failed_count": 0,
                    "total_duration_sum": 0,
                    "total_count": 0,
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
            duration_sum = (
                float(item.get("value", [0, 0])[1])
                if isinstance(item.get("value"), list)
                else 0
            )

            if namespace in namespace_stats:
                namespace_stats[namespace]["total_duration_sum"] += duration_sum

        # Process average duration data
        if avg_result.get("success"):
            for item in avg_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                avg_duration = (
                    float(item.get("value", [0, 0])[1])
                    if isinstance(item.get("value"), list)
                    else 0
                )

                if namespace in namespace_stats and not np.isnan(avg_duration):
                    namespace_stats[namespace]["avg_duration"] = avg_duration

        # Store percentile data for std deviation calculation (std ~ (P84 - P16) / 2)
        percentile_data = {}
        if p16_result.get("success"):
            for item in p16_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                p16_val = (
                    float(item.get("value", [0, 0])[1])
                    if isinstance(item.get("value"), list)
                    else 0
                )
                if namespace not in percentile_data:
                    percentile_data[namespace] = {"p16": 0, "p84": 0}
                if not np.isnan(p16_val) and not np.isinf(p16_val):
                    percentile_data[namespace]["p16"] = p16_val

        if p84_result.get("success"):
            for item in p84_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                p84_val = (
                    float(item.get("value", [0, 0])[1])
                    if isinstance(item.get("value"), list)
                    else 0
                )
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
                val = (
                    float(item.get("value", [0, 0])[1])
                    if isinstance(item.get("value"), list)
                    else 0
                )
                if namespace not in trend_data:
                    trend_data[namespace] = {
                        "recent_avg": 0,
                        "historical_avg": 0,
                        "recent_success": 0,
                        "historical_success": 0,
                    }
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["recent_avg"] = val

        if historical_avg_result.get("success"):
            for item in historical_avg_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = (
                    float(item.get("value", [0, 0])[1])
                    if isinstance(item.get("value"), list)
                    else 0
                )
                if namespace not in trend_data:
                    trend_data[namespace] = {
                        "recent_avg": 0,
                        "historical_avg": 0,
                        "recent_success": 0,
                        "historical_success": 0,
                    }
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["historical_avg"] = val

        if recent_success_result.get("success"):
            for item in recent_success_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = (
                    float(item.get("value", [0, 0])[1])
                    if isinstance(item.get("value"), list)
                    else 0
                )
                if namespace not in trend_data:
                    trend_data[namespace] = {
                        "recent_avg": 0,
                        "historical_avg": 0,
                        "recent_success": 0,
                        "historical_success": 0,
                    }
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["recent_success"] = val

        if historical_success_result.get("success"):
            for item in historical_success_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = (
                    float(item.get("value", [0, 0])[1])
                    if isinstance(item.get("value"), list)
                    else 0
                )
                if namespace not in trend_data:
                    trend_data[namespace] = {
                        "recent_avg": 0,
                        "historical_avg": 0,
                        "recent_success": 0,
                        "historical_success": 0,
                    }
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["historical_success"] = val

        reconcile_stats = {}
        if reconcile_result.get("success"):
            for item in reconcile_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace_name", "unknown")
                success = metric.get("success", "false")
                rate_val = (
                    float(item.get("value", [0, 0])[1])
                    if isinstance(item.get("value"), list)
                    else 0
                )

                if namespace not in reconcile_stats:
                    reconcile_stats[namespace] = {"success_rate": 0, "failure_rate": 0}

                if success == "true":
                    reconcile_stats[namespace]["success_rate"] = rate_val
                else:
                    reconcile_stats[namespace]["failure_rate"] = rate_val

        # Filter namespaces by pipeline_names if specified
        filtered_namespaces = namespace_stats.keys()
        if pipeline_names:
            filtered_namespaces = [
                ns
                for ns in filtered_namespaces
                if any(pn in ns for pn in pipeline_names)
            ]

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

            recon = reconcile_stats.get(
                namespace, {"success_rate": 0, "failure_rate": 0}
            )
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
                    "lower_bound": max(
                        0, avg_duration - (deviation_threshold * estimated_std)
                    ),
                },
                "success_rate": {
                    "mean_percent": success_rate,
                    "std_percent": success_rate_se,
                    "lower_bound": max(
                        0, success_rate - (deviation_threshold * success_rate_se)
                    ),
                    "upper_bound": min(
                        100, success_rate + (deviation_threshold * success_rate_se)
                    ),
                },
                "reconciliation": {
                    "success_rate_per_second": recon["success_rate"],
                    "failure_rate_per_second": recon["failure_rate"],
                    "health": reconcile_health,
                },
            }

            ns_trend = trend_data.get(
                namespace,
                {
                    "recent_avg": 0,
                    "historical_avg": 0,
                    "recent_success": 0,
                    "historical_success": 0,
                },
            )
            recent_avg = ns_trend["recent_avg"]
            historical_avg = ns_trend["historical_avg"]
            recent_success = ns_trend["recent_success"]
            historical_success = ns_trend["historical_success"]

            if historical_avg > 0 and recent_avg > 0:
                duration_change_pct = (
                    (recent_avg - historical_avg) / historical_avg
                ) * 100
            else:
                duration_change_pct = 0

            success_change = (
                recent_success - historical_success
                if (recent_success > 0 or historical_success > 0)
                else 0
            )

            significance_threshold = 10.0 / deviation_threshold
            has_recent_data = recent_avg > 0 or recent_success > 0

            if not has_recent_data:
                trend = "No recent activity (inactive in last 24h)"
                trend_direction = "inactive"
            elif (
                abs(duration_change_pct) < significance_threshold
                and abs(success_change) < significance_threshold
            ):
                trend = "Stable performance (no significant trend)"
                trend_direction = "stable"
            elif (
                duration_change_pct < -significance_threshold
                or success_change > significance_threshold
            ):
                trend = f"Performance improving: duration {duration_change_pct:+.1f}%, success rate {success_change:+.1f}%"
                trend_direction = "improving"
            elif (
                duration_change_pct > significance_threshold
                or success_change < -significance_threshold
            ):
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
                "last_updated": _datetime.now().isoformat(),
                "trend": trend,
                "trend_metrics": {
                    "recent_avg_duration": recent_avg,
                    "historical_avg_duration": historical_avg,
                    "duration_change_pct": duration_change_pct,
                    "recent_success_rate": recent_success,
                    "historical_success_rate": historical_success,
                    "success_rate_change": success_change,
                    "comparison_period": f"24h vs {baseline_period}",
                },
            }

            result["pipeline_baselines"].append(pipeline_baseline)

            if trend_direction == "improving":
                result["performance_trends"]["improving_pipelines"].append(
                    {
                        "pipeline": namespace,
                        "trend": trend,
                        "avg_duration": avg_duration,
                        "success_rate": success_rate,
                        "duration_change_pct": duration_change_pct,
                        "success_rate_change": success_change,
                    }
                )
            elif trend_direction == "degrading":
                result["performance_trends"]["degrading_pipelines"].append(
                    {
                        "pipeline": namespace,
                        "trend": trend,
                        "avg_duration": avg_duration,
                        "success_rate": success_rate,
                        "duration_change_pct": duration_change_pct,
                        "success_rate_change": success_change,
                    }
                )
            elif trend_direction in ("stable", "inactive"):
                result["performance_trends"]["stable_pipelines"].append(
                    {
                        "pipeline": namespace,
                        "trend": trend,
                        "avg_duration": avg_duration,
                        "success_rate": success_rate,
                    }
                )

            if recon["failure_rate"] > 1.0:
                result["performance_trends"]["most_variable_pipelines"].append(
                    {
                        "pipeline": namespace,
                        "failure_rate": recon["failure_rate"],
                        "avg_duration": avg_duration,
                    }
                )

            if avg_duration > 600:
                result["optimization_opportunities"].append(
                    {
                        "pipeline": namespace,
                        "opportunity": "Long execution time optimization",
                        "potential_improvement": f"Pipeline averages {avg_duration/60:.1f} minutes - consider task parallelization or caching",
                        "complexity": "medium",
                        "avg_duration_seconds": avg_duration,
                    }
                )

            if success_rate < 80:
                result["optimization_opportunities"].append(
                    {
                        "pipeline": namespace,
                        "opportunity": "Reliability improvement",
                        "potential_improvement": f"Success rate is {success_rate:.1f}% - investigate common failure patterns",
                        "complexity": "high",
                        "current_success_rate": success_rate,
                    }
                )

            if reconcile_health == "degraded":
                result["optimization_opportunities"].append(
                    {
                        "pipeline": namespace,
                        "opportunity": "Reconciliation health improvement",
                        "potential_improvement": f"High reconciliation failure rate ({recon['failure_rate']:.2f}/s) - check controller logs and resource limits",
                        "complexity": "high",
                        "failure_rate": recon["failure_rate"],
                    }
                )

        # Task-level analysis if requested
        if include_task_level:
            logger.info("Performing task-level analysis...")
            result["task_level_analysis"] = {
                "task_baselines": [],
                "slowest_tasks": [],
                "most_failed_tasks": [],
            }

            task_duration_query = f"sum by (task, namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum[{baseline_period}])) / sum by (task, namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[{baseline_period}]))"
            task_count_query = f"sum by (task, namespace, status) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[{baseline_period}]))"

            task_duration_result = await _execute_prometheus_query_internal(
                task_duration_query
            )
            task_count_result = await _execute_prometheus_query_internal(
                task_count_query
            )

            task_stats = {}

            if task_duration_result.get("success"):
                for item in task_duration_result.get("data", []):
                    metric = item.get("metric", {})
                    task_name = metric.get("task", "unknown")
                    namespace = metric.get("namespace", "unknown")
                    avg_duration = (
                        float(item.get("value", [0, 0])[1])
                        if isinstance(item.get("value"), list)
                        else 0
                    )

                    if np.isnan(avg_duration) or np.isinf(avg_duration):
                        continue

                    key = f"{namespace}/{task_name}"
                    if key not in task_stats:
                        task_stats[key] = {
                            "task": task_name,
                            "namespace": namespace,
                            "avg_duration": 0,
                            "success_count": 0,
                            "failed_count": 0,
                            "total_count": 0,
                        }
                    task_stats[key]["avg_duration"] = avg_duration

            if task_count_result.get("success"):
                for item in task_count_result.get("data", []):
                    metric = item.get("metric", {})
                    task_name = metric.get("task", "unknown")
                    namespace = metric.get("namespace", "unknown")
                    status = metric.get("status", "unknown")
                    count = (
                        float(item.get("value", [0, 0])[1])
                        if isinstance(item.get("value"), list)
                        else 0
                    )

                    if np.isnan(count) or np.isinf(count):
                        continue

                    key = f"{namespace}/{task_name}"
                    if key not in task_stats:
                        task_stats[key] = {
                            "task": task_name,
                            "namespace": namespace,
                            "avg_duration": 0,
                            "success_count": 0,
                            "failed_count": 0,
                            "total_count": 0,
                        }

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

                task_success_rate = (
                    (stats["success_count"] / stats["total_count"] * 100)
                    if stats["total_count"] > 0
                    else 0
                )

                task_baseline = {
                    "task": stats["task"],
                    "namespace": stats["namespace"],
                    "avg_duration_seconds": stats["avg_duration"],
                    "total_runs": int(stats["total_count"]),
                    "success_count": int(stats["success_count"]),
                    "failed_count": int(stats["failed_count"]),
                    "success_rate": task_success_rate,
                }
                result["task_level_analysis"]["task_baselines"].append(task_baseline)

            if (
                unknown_task_count > 0
                and len(result["task_level_analysis"]["task_baselines"]) == 0
            ):
                result["task_level_analysis"]["note"] = (
                    f"Task-level analysis unavailable: Prometheus metrics do not include 'task' labels. "
                    f"Found {unknown_task_count} namespace-level aggregations. "
                    "For task-level details, query TaskRun resources directly via Kubernetes API."
                )
                logger.info(
                    f"Task-level analysis: No task labels in Prometheus metrics ({unknown_task_count} namespaces without task granularity)"
                )

            result["task_level_analysis"]["task_baselines"].sort(
                key=lambda x: x.get("avg_duration_seconds", 0) or 0, reverse=True
            )
            result["task_level_analysis"]["slowest_tasks"] = result[
                "task_level_analysis"
            ]["task_baselines"][:10]

            failed_tasks = [
                t
                for t in result["task_level_analysis"]["task_baselines"]
                if t["failed_count"] > 0
            ]
            failed_tasks.sort(key=lambda x: x["failed_count"], reverse=True)
            result["task_level_analysis"]["most_failed_tasks"] = failed_tasks[:10]

            logger.info(
                f"Task-level analysis completed: {len(result['task_level_analysis']['task_baselines'])} tasks analyzed (filtered {unknown_task_count} 'unknown' entries)"
            )

        # Sort results for better presentation
        result["pipeline_baselines"].sort(
            key=lambda x: x.get("data_points", 0), reverse=True
        )
        result["performance_trends"]["improving_pipelines"].sort(
            key=lambda x: x.get("avg_duration", 0)
        )
        result["performance_trends"]["degrading_pipelines"].sort(
            key=lambda x: x.get("avg_duration", 0), reverse=True
        )
        result["performance_trends"]["most_variable_pipelines"].sort(
            key=lambda x: x.get("failure_rate", 0), reverse=True
        )

        result["summary"] = {
            "total_namespaces_analyzed": len(result["pipeline_baselines"]),
            "total_taskruns_tracked": sum(
                b.get("data_points", 0) for b in result["pipeline_baselines"]
            ),
            "total_successes": sum(
                b.get("success_count", 0) for b in result["pipeline_baselines"]
            ),
            "total_failures": sum(
                b.get("failed_count", 0) for b in result["pipeline_baselines"]
            ),
            "namespaces_needing_attention": len(
                [
                    b
                    for b in result["pipeline_baselines"]
                    if b.get("baseline_metrics", {})
                    .get("success_rate", {})
                    .get("mean_percent", 100)
                    < 80
                ]
            ),
            "optimization_opportunities_count": len(
                result["optimization_opportunities"]
            ),
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
                "most_variable_pipelines": [],
            },
            "optimization_opportunities": [],
            "error": str(e),
        }


# ============================================================================
# RESOURCE BOTTLENECK FORECASTER HELPERS AND IMPLEMENTATION
# Extracted from server-mcp.py as part of issue #119 (sub-task of #112).
# ============================================================================

from datetime import datetime as _datetime
from typing import Any as _Any
from typing import Dict as _Dict
from typing import List as _List
from typing import Optional as _Optional
from typing import Set as _Set

from helpers.utils import (
    calculate_forecast_intervals,
    parse_time_period,
    simple_linear_forecast,
)

_logger = logging.getLogger(__name__)


async def _get_active_node_names_with_api(core_api: _Any) -> _Set[str]:
    """Get the set of currently active (Ready) node names from Kubernetes API."""
    import asyncio

    try:
        nodes = await asyncio.to_thread(core_api.list_node)
        active_nodes: _Set[str] = set()
        for node in nodes.items:
            is_ready = False
            if node.status and node.status.conditions:
                for condition in node.status.conditions:
                    if condition.type == "Ready" and condition.status == "True":
                        is_ready = True
                        break
            if is_ready:
                node_name = node.metadata.name
                active_nodes.add(node_name)
                if node.status and node.status.addresses:
                    for addr in node.status.addresses:
                        if addr.type in ["InternalIP", "ExternalIP", "Hostname"]:
                            active_nodes.add(addr.address)
                            active_nodes.add(f"{addr.address}:9100")
        return active_nodes
    except Exception as e:
        _logger.warning(f"Could not get active nodes from K8s API: {e}")
        return set()


def _is_node_active(node_identifier: str, active_nodes: _Set[str]) -> bool:
    """Check if a node identifier matches any active node."""
    if not active_nodes:
        return True
    if node_identifier in active_nodes:
        return True
    node_without_port = (
        node_identifier.split(":")[0] if ":" in node_identifier else node_identifier
    )
    if node_without_port in active_nodes:
        return True
    for active_node in active_nodes:
        if node_identifier.startswith(active_node) or active_node.startswith(
            node_identifier
        ):
            return True
        if node_without_port in active_node or active_node in node_without_port:
            return True
    return False


async def _analyze_node_resources_new(
    trend_period: str,
    forecast_horizon: str,
    log,
    core_api: _Any,
    prometheus_query_fn,
) -> _List[_Dict]:
    """Analyze node-level resource utilization using Prometheus query method."""
    from datetime import timedelta

    try:
        active_nodes = await _get_active_node_names_with_api(core_api)
        log.info(f"Found {len(active_nodes)} active nodes from Kubernetes API")

        end_time = _datetime.now()
        start_time = end_time - parse_time_period(trend_period)
        start_time_iso = start_time.isoformat() + "Z"
        end_time_iso = end_time.isoformat() + "Z"

        forecasts = []
        filtered_count = 0
        forecast_points = calculate_forecast_intervals(forecast_horizon)

        cpu_query = 'max by (instance) (100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100))'
        try:
            cpu_result = await prometheus_query_fn(
                query=cpu_query,
                query_type="range",
                start_time=start_time_iso,
                end_time=end_time_iso,
                step="300s",
                limit=100,
            )
            if cpu_result.get("status") == "success" and cpu_result.get("data"):
                for metric in cpu_result["data"]:
                    node = metric.get("metric", {}).get("instance", "unknown")
                    if not _is_node_active(node, active_nodes):
                        filtered_count += 1
                        continue
                    values = [float(point[1]) for point in metric.get("values", [])]
                    if values:
                        forecast_result = simple_linear_forecast(
                            values, forecast_points
                        )
                        current_usage = values[-1]
                        predicted_exhaustion = None
                        if forecast_result["growth_rate"] > 0:
                            points_to_90 = (90 - current_usage) / forecast_result[
                                "growth_rate"
                            ]
                            if points_to_90 > 0:
                                predicted_exhaustion = (
                                    end_time + timedelta(minutes=5 * points_to_90)
                                ).isoformat()
                        forecasts.append(
                            {
                                "resource_type": "cpu",
                                "resource_identifier": {
                                    "node": node,
                                    "metric": "cpu_utilization_percent",
                                },
                                "current_usage": {
                                    "value": current_usage,
                                    "unit": "percent",
                                },
                                "predicted_exhaustion": predicted_exhaustion,
                                "growth_rate": {
                                    "value": forecast_result["growth_rate"],
                                    "unit": "percent_per_5min",
                                },
                                "contributing_factors": [
                                    "workload_scaling",
                                    "baseline_usage_trend",
                                ],
                            }
                        )
        except Exception as e:
            log.warning(f"Error fetching CPU metrics: {str(e)}")

        memory_query = "max by (instance) ((1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100)"
        try:
            memory_result = await prometheus_query_fn(
                query=memory_query,
                query_type="range",
                start_time=start_time_iso,
                end_time=end_time_iso,
                step="300s",
                limit=100,
            )
            if memory_result.get("status") == "success" and memory_result.get("data"):
                for metric in memory_result["data"]:
                    node = metric.get("metric", {}).get("instance", "unknown")
                    if not _is_node_active(node, active_nodes):
                        filtered_count += 1
                        continue
                    values = [float(point[1]) for point in metric.get("values", [])]
                    if values:
                        forecast_result = simple_linear_forecast(
                            values, forecast_points
                        )
                        current_usage = values[-1]
                        predicted_exhaustion = None
                        if forecast_result["growth_rate"] > 0:
                            points_to_90 = (90 - current_usage) / forecast_result[
                                "growth_rate"
                            ]
                            if points_to_90 > 0:
                                predicted_exhaustion = (
                                    end_time + timedelta(minutes=5 * points_to_90)
                                ).isoformat()
                        forecasts.append(
                            {
                                "resource_type": "memory",
                                "resource_identifier": {
                                    "node": node,
                                    "metric": "memory_utilization_percent",
                                },
                                "current_usage": {
                                    "value": current_usage,
                                    "unit": "percent",
                                },
                                "predicted_exhaustion": predicted_exhaustion,
                                "growth_rate": {
                                    "value": forecast_result["growth_rate"],
                                    "unit": "percent_per_5min",
                                },
                                "contributing_factors": [
                                    "memory_leaks",
                                    "workload_growth",
                                    "cache_usage",
                                ],
                            }
                        )
        except Exception as e:
            log.warning(f"Error fetching memory metrics: {str(e)}")

        disk_query = """max by (instance, mountpoint) (
            (1 - (node_filesystem_avail_bytes{fstype!="tmpfs", mountpoint!~"/var/lib/kubelet/pods.*|/run/.*"}
                / node_filesystem_size_bytes{fstype!="tmpfs", mountpoint!~"/var/lib/kubelet/pods.*|/run/.*"})) * 100
        )"""
        try:
            disk_result = await prometheus_query_fn(
                query=disk_query,
                query_type="range",
                start_time=start_time_iso,
                end_time=end_time_iso,
                step="300s",
                limit=200,
            )
            if disk_result.get("status") == "success" and disk_result.get("data"):
                for metric in disk_result["data"]:
                    node = metric.get("metric", {}).get("instance", "unknown")
                    if not _is_node_active(node, active_nodes):
                        filtered_count += 1
                        continue
                    mountpoint = metric.get("metric", {}).get("mountpoint", "unknown")
                    values = [float(point[1]) for point in metric.get("values", [])]
                    if values:
                        forecast_result = simple_linear_forecast(
                            values, forecast_points
                        )
                        current_usage = values[-1]
                        predicted_exhaustion = None
                        if forecast_result["growth_rate"] > 0:
                            points_to_90 = (90 - current_usage) / forecast_result[
                                "growth_rate"
                            ]
                            if points_to_90 > 0:
                                predicted_exhaustion = (
                                    end_time + timedelta(minutes=5 * points_to_90)
                                ).isoformat()
                        forecasts.append(
                            {
                                "resource_type": "disk",
                                "resource_identifier": {
                                    "node": node,
                                    "mountpoint": mountpoint,
                                    "metric": "disk_utilization_percent",
                                },
                                "current_usage": {
                                    "value": current_usage,
                                    "unit": "percent",
                                },
                                "predicted_exhaustion": predicted_exhaustion,
                                "growth_rate": {
                                    "value": forecast_result["growth_rate"],
                                    "unit": "percent_per_5min",
                                },
                                "contributing_factors": [
                                    "log_growth",
                                    "cache_accumulation",
                                    "temporary_files",
                                ],
                            }
                        )
        except Exception as e:
            log.warning(f"Error fetching disk metrics: {str(e)}")

        if filtered_count > 0:
            log.info(
                f"Filtered out {filtered_count} metrics from inactive/historical nodes"
            )

        return forecasts
    except Exception as e:
        log.error(f"Error analyzing node resources: {str(e)}")
        return []


async def _analyze_cluster_capacity_new(
    core_api: _Any,
    log,
    prometheus_query_fn,
) -> _Dict[str, _Any]:
    """Analyze overall cluster capacity and health using Prometheus query method."""
    try:
        nodes = core_api.list_node()
        total_cpu = 0
        total_memory = 0
        total_nodes = len(nodes.items)

        for node in nodes.items:
            if node.status and node.status.capacity:
                cpu_str = node.status.capacity.get("cpu", "0")
                memory_str = node.status.capacity.get("memory", "0Ki")
                if "m" in cpu_str:
                    total_cpu += int(cpu_str.replace("m", "")) / 1000
                else:
                    total_cpu += int(cpu_str)
                if memory_str.endswith("Ki"):
                    total_memory += int(memory_str[:-2]) * 1024
                elif memory_str.endswith("Mi"):
                    total_memory += int(memory_str[:-2]) * 1024 * 1024
                elif memory_str.endswith("Gi"):
                    total_memory += int(memory_str[:-2]) * 1024 * 1024 * 1024

        cpu_usage_percent = 0
        memory_usage_percent = 0

        try:
            cpu_usage_result = await prometheus_query_fn(
                'avg(100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100))'
            )
            if cpu_usage_result.get("status") == "success" and cpu_usage_result.get(
                "data"
            ):
                data = cpu_usage_result["data"]
                if data and len(data) > 0 and "value" in data[0]:
                    cpu_usage_percent = float(data[0]["value"][1])
        except Exception as e:
            log.warning(f"Could not fetch cluster CPU usage: {str(e)}")

        try:
            memory_usage_result = await prometheus_query_fn(
                "avg(100 - (avg by (instance) (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100)"
            )
            if memory_usage_result.get(
                "status"
            ) == "success" and memory_usage_result.get("data"):
                data = memory_usage_result["data"]
                if data and len(data) > 0 and "value" in data[0]:
                    memory_usage_percent = float(data[0]["value"][1])
        except Exception as e:
            log.warning(f"Could not fetch cluster memory usage: {str(e)}")

        overall_health = "healthy"
        if cpu_usage_percent > 90 or memory_usage_percent > 90:
            overall_health = "critical"
        elif cpu_usage_percent > 80 or memory_usage_percent > 80:
            overall_health = "degraded"

        constrained_resources = []
        if cpu_usage_percent > 70:
            constrained_resources.append(f"CPU ({cpu_usage_percent:.1f}%)")
        if memory_usage_percent > 70:
            constrained_resources.append(f"Memory ({memory_usage_percent:.1f}%)")

        return {
            "overall_health": overall_health,
            "total_nodes": total_nodes,
            "total_cpu_cores": total_cpu,
            "total_memory_gb": round(total_memory / (1024**3), 1),
            "current_cpu_usage": f"{cpu_usage_percent:.1f}%",
            "current_memory_usage": f"{memory_usage_percent:.1f}%",
            "most_constrained_resources": constrained_resources,
            "fastest_growing_consumers": [],
            "capacity_runway": {
                "cpu_runway_days": max(
                    0, int((90 - cpu_usage_percent) / max(0.1, cpu_usage_percent / 30))
                ),
                "memory_runway_days": max(
                    0,
                    int(
                        (90 - memory_usage_percent)
                        / max(0.1, memory_usage_percent / 30)
                    ),
                ),
            },
        }
    except Exception as e:
        log.error(f"Error analyzing cluster capacity: {str(e)}")
        return {
            "overall_health": "unknown",
            "total_nodes": 0,
            "total_cpu_cores": 0,
            "total_memory_gb": 0,
            "current_cpu_usage": "unknown",
            "current_memory_usage": "unknown",
            "most_constrained_resources": [],
            "fastest_growing_consumers": [],
            "capacity_runway": {},
        }


async def resource_bottleneck_forecaster_impl(
    forecast_horizon: str = "24h",
    resource_types: _Optional[_List[str]] = None,
    namespaces: _Optional[_List[str]] = None,
    trend_analysis_period: str = "7d",
    k8s_core_api: _Any = None,
    prometheus_query_fn=None,
) -> _Dict[str, _Any]:
    """
    Implementation of resource_bottleneck_forecaster tool.

    Forecast resource bottlenecks by analyzing utilization trends and predicting exhaustion points.

    Args:
        forecast_horizon: Forecast window - "1h", "6h", "24h", "7d", "30d" (default: "24h").
        resource_types: Resources to analyze - cpu, memory, disk, network, pvc (default: all).
        namespaces: Specific namespaces to focus on.
        trend_analysis_period: Historical period for trends (default: "7d").
        k8s_core_api: Kubernetes CoreV1Api client (injected by wrapper).
        prometheus_query_fn: Callable for Prometheus queries (injected by wrapper).

    Returns:
        Dict: Keys: forecasts, capacity_recommendations, cluster_overview, historical_accuracy.
    """
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}
    if prometheus_query_fn is None:
        return {"error": "prometheus_query_fn not provided."}

    _logger.info(
        f"Starting resource bottleneck forecasting for horizon: {forecast_horizon}"
    )

    try:
        if resource_types is None:
            resource_types = ["cpu", "memory", "disk", "network", "pvc"]

        # Test Prometheus connectivity
        try:
            test_query_result = await prometheus_query_fn("up")
            if test_query_result.get("status") != "success":
                _logger.warning(
                    "Could not connect to Prometheus endpoint, using mock data"
                )
                return {
                    "forecasts": [],
                    "capacity_recommendations": [
                        {
                            "resource": "monitoring",
                            "current_capacity": "unavailable",
                            "recommended_capacity": "install_prometheus_or_check_connectivity",
                            "scaling_urgency": "high",
                            "implementation_options": [
                                "Check Prometheus deployment",
                                "Verify RBAC permissions",
                                "Check cluster connectivity",
                            ],
                        }
                    ],
                    "cluster_overview": {
                        "overall_health": "monitoring_unavailable",
                        "most_constrained_resources": [],
                        "fastest_growing_consumers": [],
                        "capacity_runway": {},
                    },
                    "historical_accuracy": {
                        "previous_predictions": 0,
                        "accuracy_rate": 0.0,
                        "last_validation": _datetime.now().isoformat(),
                    },
                }
        except Exception as e:
            _logger.warning(
                f"Error testing Prometheus connectivity: {type(e).__name__}"
            )
            return {
                "forecasts": [],
                "capacity_recommendations": [
                    {
                        "resource": "monitoring",
                        "current_capacity": "error",
                        "recommended_capacity": "fix_monitoring_setup",
                        "scaling_urgency": "high",
                        "implementation_options": [
                            "Check Prometheus deployment",
                            "Verify authentication",
                            "Review cluster configuration",
                        ],
                    }
                ],
                "cluster_overview": {
                    "overall_health": "monitoring_error",
                    "most_constrained_resources": [],
                    "fastest_growing_consumers": [],
                    "capacity_runway": {},
                },
                "historical_accuracy": {
                    "previous_predictions": 0,
                    "accuracy_rate": 0.0,
                    "last_validation": _datetime.now().isoformat(),
                },
            }

        forecasts = []
        if any(r in resource_types for r in ("cpu", "memory", "disk")):
            node_forecasts = await _analyze_node_resources_new(
                trend_analysis_period,
                forecast_horizon,
                _logger,
                k8s_core_api,
                prometheus_query_fn,
            )

            if namespaces:
                MAX_NODES = 5
                node_max_usage: _Dict[str, float] = {}
                node_has_exhaustion: _Dict[str, bool] = {}
                for f in node_forecasts:
                    node = f.get("resource_identifier", {}).get(
                        "node",
                        f.get("resource_identifier", {}).get("instance", "unknown"),
                    )
                    usage = f.get("current_usage", {}).get("value", 0)
                    node_max_usage[node] = max(node_max_usage.get(node, 0), usage)
                    if f.get("predicted_exhaustion"):
                        node_has_exhaustion[node] = True
                sorted_nodes = sorted(
                    node_max_usage.keys(),
                    key=lambda n: (
                        node_has_exhaustion.get(n, False),
                        node_max_usage[n],
                    ),
                    reverse=True,
                )
                keep_nodes = set(sorted_nodes[:MAX_NODES])
                trimmed = [
                    f
                    for f in node_forecasts
                    if f.get("resource_identifier", {}).get(
                        "node", f.get("resource_identifier", {}).get("instance", "")
                    )
                    in keep_nodes
                ]
                forecasts.extend(trimmed)
                if len(node_forecasts) > len(trimmed):
                    _logger.info(
                        f"Trimmed node forecasts from {len(node_forecasts)} entries ({len(node_max_usage)} nodes) "
                        f"to {len(trimmed)} entries ({len(keep_nodes)} nodes)"
                    )
            else:
                forecasts.extend(node_forecasts)

        if namespaces:
            for namespace in namespaces:
                try:
                    namespace_cpu_query = f'sum(rate(container_cpu_usage_seconds_total{{namespace="{namespace}"}}[5m])) * 100'
                    cpu_result = await prometheus_query_fn(namespace_cpu_query)
                    if cpu_result.get("status") == "success" and cpu_result.get("data"):
                        data = cpu_result["data"]
                        if data and len(data) > 0 and "value" in data[0]:
                            cpu_usage = float(data[0]["value"][1])
                            forecasts.append(
                                {
                                    "resource_type": "namespace_cpu",
                                    "resource_identifier": {
                                        "namespace": namespace,
                                        "metric": "cpu_usage_cores",
                                    },
                                    "current_usage": {
                                        "value": cpu_usage,
                                        "unit": "cores",
                                    },
                                    "predicted_exhaustion": None,
                                    "growth_rate": {
                                        "value": 0,
                                        "unit": "cores_per_5min",
                                    },
                                    "contributing_factors": [
                                        "pod_scaling",
                                        "workload_changes",
                                    ],
                                }
                            )

                    memory_queries = [
                        f'sum(container_memory_working_set_bytes{{namespace="{namespace}"}}) / 1024 / 1024 / 1024',
                        f'sum(container_memory_usage_bytes{{namespace="{namespace}"}}) / 1024 / 1024 / 1024',
                    ]
                    memory_usage_gb = 0
                    for memory_query in memory_queries:
                        memory_result = await prometheus_query_fn(memory_query)
                        if memory_result.get(
                            "status"
                        ) == "success" and memory_result.get("data"):
                            data = memory_result["data"]
                            if data and len(data) > 0:
                                raw_val = data[0].get("value", [0, "0"])
                                if isinstance(raw_val, list) and len(raw_val) >= 2:
                                    memory_usage_gb = float(raw_val[1])
                                elif isinstance(raw_val, (str, int, float)):
                                    memory_usage_gb = float(raw_val)
                                if memory_usage_gb > 0:
                                    break
                    if memory_usage_gb > 0:
                        forecasts.append(
                            {
                                "resource_type": "namespace_memory",
                                "resource_identifier": {
                                    "namespace": namespace,
                                    "metric": "memory_usage_gb",
                                },
                                "current_usage": {
                                    "value": memory_usage_gb,
                                    "unit": "GB",
                                },
                                "predicted_exhaustion": None,
                                "growth_rate": {"value": 0, "unit": "GB_per_5min"},
                                "contributing_factors": [
                                    "pod_scaling",
                                    "memory_leaks",
                                    "cache_growth",
                                ],
                            }
                        )
                except Exception as e:
                    _logger.warning(
                        f"Could not analyze namespace {namespace}: {str(e)}"
                    )

        # Generate capacity recommendations
        capacity_recommendations = []
        critical_forecasts = [f for f in forecasts if f.get("predicted_exhaustion")]
        for forecast in critical_forecasts:
            resource_type = forecast["resource_type"]
            current_usage = forecast["current_usage"]["value"]
            urgency = "low"
            try:
                exhaustion_time = _datetime.fromisoformat(
                    forecast["predicted_exhaustion"].replace("Z", "+00:00")
                )
                time_to_exhaustion = exhaustion_time - _datetime.now(
                    exhaustion_time.tzinfo
                )
                if time_to_exhaustion.total_seconds() < 3600:
                    urgency = "critical"
                elif time_to_exhaustion.total_seconds() < 86400:
                    urgency = "high"
                elif time_to_exhaustion.total_seconds() < 604800:
                    urgency = "medium"
            except Exception:
                urgency = "medium"

            if resource_type == "cpu":
                capacity_recommendations.append(
                    {
                        "resource": f"cpu_{forecast['resource_identifier']['node']}",
                        "current_capacity": f"{current_usage:.1f}%",
                        "recommended_capacity": (
                            "scale_up_nodes"
                            if current_usage > 70
                            else "optimize_workloads"
                        ),
                        "scaling_urgency": urgency,
                        "implementation_options": [
                            "Add worker nodes",
                            "Implement CPU limits",
                            "Optimize container resource requests",
                            "Consider pod autoscaling",
                        ],
                    }
                )
            elif resource_type == "memory":
                capacity_recommendations.append(
                    {
                        "resource": f"memory_{forecast['resource_identifier']['node']}",
                        "current_capacity": f"{current_usage:.1f}%",
                        "recommended_capacity": (
                            "increase_memory"
                            if current_usage > 80
                            else "review_memory_usage"
                        ),
                        "scaling_urgency": urgency,
                        "implementation_options": [
                            "Upgrade node memory",
                            "Implement memory limits",
                            "Review memory-intensive workloads",
                            "Enable memory optimization",
                        ],
                    }
                )

        cluster_overview = await _analyze_cluster_capacity_new(
            k8s_core_api, _logger, prometheus_query_fn
        )

        historical_accuracy = {
            "previous_predictions": len(forecasts),
            "accuracy_rate": None,
            "last_validation": None,
            "note": "Prediction validation not implemented - accuracy not tracked",
        }

        result = {
            "forecasts": forecasts,
            "capacity_recommendations": capacity_recommendations,
            "cluster_overview": cluster_overview,
            "historical_accuracy": historical_accuracy,
        }

        _logger.info(
            f"Completed resource bottleneck forecasting. Generated {len(forecasts)} forecasts "
            f"and {len(capacity_recommendations)} recommendations"
        )
        return result

    except Exception as e:
        _logger.error(
            f"Error in resource bottleneck forecasting: {str(e)}", exc_info=True
        )
        return {
            "forecasts": [],
            "capacity_recommendations": [
                {
                    "resource": "error",
                    "current_capacity": "unknown",
                    "recommended_capacity": "check_monitoring_setup",
                    "scaling_urgency": "medium",
                    "implementation_options": [
                        "Verify Prometheus deployment",
                        "Check RBAC permissions",
                    ],
                }
            ],
            "cluster_overview": {
                "overall_health": "error",
                "most_constrained_resources": [],
                "fastest_growing_consumers": [],
                "capacity_runway": {},
            },
            "historical_accuracy": {
                "previous_predictions": 0,
                "accuracy_rate": 0.0,
                "last_validation": _datetime.now().isoformat(),
            },
        }


# ============================================================================
# WHAT-IF SCENARIO SIMULATOR IMPLEMENTATION
# Extracted from server-mcp.py as part of issue #120 (sub-task of #112).
# ============================================================================

import uuid as _uuid

from helpers.failure_analysis import (
    analyze_system_impact,
    calculate_simulation_quality,
    generate_simulation_recommendations,
    perform_risk_assessment,
)
from helpers.resource_topology import identify_affected_components
from helpers.utils import (
    build_system_behavior_models,
    calibrate_simulation_models,
    collect_baseline_system_data,
    convert_duration_to_seconds,
    load_historical_performance_data,
    run_monte_carlo_simulation,
)


async def what_if_scenario_simulator_impl(
    scenario_type: str,
    changes: _Dict[str, _Any],
    scope: _Optional[_Dict[str, _Any]] = None,
    simulation_duration: str = "24h",
    load_profile: str = "current",
    risk_tolerance: str = "moderate",
    k8s_core_api: _Any = None,
    k8s_apps_api: _Any = None,
    list_namespaces_fn=None,
    list_pods_fn=None,
    prometheus_query_fn=None,
) -> _Dict[str, _Any]:
    """
    Implementation of what_if_scenario_simulator tool.

    Simulate impact of configuration changes before applying to live system with risk assessment.
    Uses Monte Carlo simulation and load modeling based on historical data.

    Args:
        scenario_type: Type - "resource_limits", "scaling", "configuration", "deployment".
        changes: Changes to simulate with before/after values.
        scope: Simulation scope - clusters, namespaces, components.
        simulation_duration: Duration - "1h", "24h", "7d" (default: "24h").
        load_profile: Expected load - "current", "peak", "custom" (default: "current").
        risk_tolerance: Risk level - "conservative", "moderate", "aggressive" (default: "moderate").
        k8s_core_api: Kubernetes CoreV1Api client (injected by wrapper).
        k8s_apps_api: Kubernetes AppsV1Api client (injected by wrapper).
        list_namespaces_fn: Callable for listing namespaces (injected by wrapper).
        list_pods_fn: Callable for listing pods (injected by wrapper).
        prometheus_query_fn: Callable for Prometheus queries (injected by wrapper).

    Returns:
        Dict: Keys: simulation_id, impact_analysis, risk_assessment, affected_components, recommendations.
    """
    if not k8s_core_api or not k8s_apps_api:
        return {"error": "Kubernetes client not available."}


    simulation_id = f"sim-{_uuid.uuid4().hex[:8]}-{int(_datetime.now().timestamp())}"

    _logger.info(
        f"Starting what-if scenario simulation {simulation_id} for {scenario_type}"
    )

    try:
        valid_scenario_types = [
            "resource_limits",
            "scaling",
            "configuration",
            "deployment",
        ]
        if scenario_type not in valid_scenario_types:
            return {
                "simulation_id": simulation_id,
                "error": f"Invalid scenario_type '{scenario_type}'. Must be one of: {valid_scenario_types}",
            }

        valid_durations = ["1h", "24h", "7d"]
        if simulation_duration not in valid_durations:
            return {
                "simulation_id": simulation_id,
                "error": f"Invalid simulation_duration '{simulation_duration}'. Must be one of: {valid_durations}",
            }

        valid_load_profiles = ["current", "peak", "custom"]
        if load_profile not in valid_load_profiles:
            return {
                "simulation_id": simulation_id,
                "error": f"Invalid load_profile '{load_profile}'. Must be one of: {valid_load_profiles}",
            }

        valid_risk_levels = ["conservative", "moderate", "aggressive"]
        if risk_tolerance not in valid_risk_levels:
            return {
                "simulation_id": simulation_id,
                "error": f"Invalid risk_tolerance '{risk_tolerance}'. Must be one of: {valid_risk_levels}",
            }

        if not changes or not isinstance(changes, dict):
            return {
                "simulation_id": simulation_id,
                "error": "Changes parameter must be a non-empty dictionary with before/after values",
            }

        if scope is None:
            scope = {
                "clusters": ["current"],
                "namespaces": ["all"],
                "components": ["all"],
            }

        baseline_data = await collect_baseline_system_data(
            scope, k8s_core_api, list_namespaces_fn, list_pods_fn
        )
        behavior_models = await build_system_behavior_models(
            baseline_data, scenario_type
        )
        historical_data = await load_historical_performance_data(
            scope,
            simulation_duration,
            prometheus_query_fn=prometheus_query_fn,
        )
        calibrated_models = calibrate_simulation_models(
            behavior_models, historical_data, load_profile
        )
        simulation_results = await run_monte_carlo_simulation(
            calibrated_models,
            changes,
            scenario_type,
            simulation_duration,
            risk_tolerance,
        )
        impact_analysis = analyze_system_impact(
            simulation_results, baseline_data, scenario_type
        )
        affected_components = await identify_affected_components(
            changes,
            scope,
            scenario_type,
            k8s_core_api,
            k8s_apps_api,
            list_pods_fn,
            list_namespaces_fn,
        )
        risk_assessment = perform_risk_assessment(
            simulation_results, impact_analysis, affected_components, risk_tolerance
        )
        simulation_quality = calculate_simulation_quality(
            baseline_data, historical_data, calibrated_models, _logger
        )
        recommendations = generate_simulation_recommendations(
            impact_analysis, risk_assessment, simulation_quality, scenario_type, _logger
        )

        result = {
            "simulation_id": simulation_id,
            "scenario_description": (
                f"{scenario_type.replace('_', ' ').title()} simulation over {simulation_duration}"
            ),
            "simulation_parameters": {
                "scenario_type": scenario_type,
                "duration": simulation_duration,
                "load_profile": load_profile,
                "risk_tolerance": risk_tolerance,
                "scope": scope,
                "changes": changes,
            },
            "impact_analysis": impact_analysis,
            "affected_components": affected_components,
            "risk_assessment": risk_assessment,
            "simulation_quality": simulation_quality,
            "recommendations": recommendations,
            "timestamp": _datetime.now().isoformat(),
            "simulation_duration_seconds": convert_duration_to_seconds(
                simulation_duration
            ),
        }

        _logger.info(
            f"Completed simulation {simulation_id} with {len(affected_components)} affected components"
        )
        return result

    except Exception as e:
        _logger.error(f"Error in what-if scenario simulation: {str(e)}", exc_info=True)

        return {
            "simulation_id": simulation_id,
            "error": f"Simulation failed: {str(e)}",
            "timestamp": _datetime.now().isoformat(),
        }
