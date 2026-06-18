"""
LUMINO MCP Server - FastMCP Server Module

This module provides the core MCP (Model Context Protocol) server implementation
for Kubernetes, OpenShift, and Tekton monitoring and analysis.
"""

import asyncio
import base64
import logging
import re
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import networkx as nx

# For metrics and analysis
import numpy as np
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Helper imports
from helpers import (  # Log analysis helpers; Advanced log analysis helpers; ML/Data processing helpers for predictive analysis; Token limit truncation helpers; Resource search helpers; Certificate parsing helpers; Performance analysis helpers; Resource topology helpers; Machine config pool helpers; Operator analysis helpers; Topology mapping helpers; Resource forecasting helpers; Semantic search helpers; Simulation helpers; Simulation impact analysis; Simulation affected components; Prometheus result formatters; Log/API helpers
    SMART_EVENTS_CONFIG,
    EventCategory,
    EventSeverity,
    LogAnalysisContext,
    LogAnalysisStrategy,
    LogMetricsIntegrator,
    LogStreamProcessor,
    MLPatternDetector,
    ProgressiveEventAnalyzer,
    RunbookSuggestionEngine,
    StrategySelector,
    _build_log_params,
    _filter_logs_by_time_range,
    _get_logs_with_k8s_client,
    _get_target_namespaces,
    _handle_api_exception,
    _search_events_semantically,
    _search_pod_logs_semantically,
    _search_tekton_resources_semantically,
    analysis_cache,
    analyze_bottlenecks,
    analyze_configuration_issues,
    analyze_generic_failure,
    analyze_labels,
    analyze_log_patterns_for_failure_prediction,
    analyze_machine_config_pool_status,
    analyze_operator_conditions,
    analyze_operator_dependencies,
    analyze_owner_references,
    analyze_pipeline_dependencies,
    analyze_pipeline_failure,
    analyze_pipeline_performance,
    analyze_pod_failure,
    analyze_resource_constraints,
    analyze_service_dependencies,
    analyze_severity_distribution,
    analyze_system_impact,
    analyze_trending_patterns,
    analyze_volume_dependencies,
    assess_failure_severity,
    assess_overall_risk,
    build_advanced_label_selector,
    build_failure_timeline,
    build_system_behavior_models,
    calculate_confidence_score,
    calculate_context_tokens,
    calculate_dependency_weight,
    calculate_duration,
    calculate_duration_seconds,
    calculate_forecast_intervals,
    calculate_namespace_distribution,
    calculate_semantic_relevance,
    calculate_simulation_quality,
    calculate_utilization,
    calibrate_simulation_models,
    categorize_certificate_status,
    categorize_errors,
    clean_etcd_logs,
    clean_pipeline_logs,
    collect_baseline_system_data,
    combine_analysis_results,
    compress_events_for_synthesis,
    convert_duration_to_seconds,
    convert_to_graphviz,
    convert_to_mermaid,
    correlate_pipeline_events,
    detect_anomalies_in_data,
    detect_pool_issues,
    determine_root_cause,
    determine_search_strategy,
    extract_error_patterns,
    extract_k8s_entities,
    extract_log_features,
    extract_log_metadata,
    extract_log_patterns,
    extract_resource_info,
    filter_analysis_for_synthesis,
    find_related_failures,
    find_semantic_matches,
    follow_lifecycle_chain,
    format_as_csv,
    format_as_json,
    format_as_table,
    format_detailed_output,
    format_metric_value,
    format_summary_output,
    format_yaml_output,
    generate_comprehensive_insights,
    generate_failure_predictions,
    generate_focused_summary,
    generate_hybrid_recommendations,
    generate_log_summary,
    generate_node_id,
    generate_query_suggestions,
    generate_related_query_suggestions,
    generate_remediation_plan,
    generate_result_summary,
    generate_semantic_suggestions,
    generate_simulation_recommendations,
    generate_strategic_recommendations,
    generate_streaming_recommendations,
    generate_streaming_summary,
    generate_string_events_insights,
    generate_string_events_recommendations,
    generate_string_events_summary,
    generate_supplementary_insights,
    generate_update_recommendations,
    get_all_pod_logs,
    get_multi_cluster_clients,
    get_multi_cluster_topology_clients,
    get_pipeline_details,
    get_resource_api_info,
    get_resource_metrics,
    get_strategy_selection_reason,
    get_task_details,
    handle_resource_fetch_error,
    identify_affected_components,
    identify_common_patterns,
    identify_critical_issues,
    identify_failure_context,
    identify_match_reasons,
    interpret_semantic_query,
    list_pods,
    load_historical_performance_data,
    parse_certificate,
    parse_time_parameter,
    parse_time_parameters,
    parse_time_period,
    perform_advanced_rca,
    perform_risk_assessment,
    preprocess_log_data,
    rank_results_by_semantic_relevance,
    recommend_actions,
    run_monte_carlo_simulation,
    sample_logs_by_time,
    simple_linear_forecast,
    smart_sample_string_events,
    sort_resources,
    track_artifacts,
    train_anomaly_model,
    train_or_load_model,
    truncate_to_token_limit,
)
from helpers.config import _NAMESPACE_CACHE_TTL, is_running_in_cluster
from helpers.k8s_client import (
    AdaptiveLogProcessor,
    _calculate_adaptive_tail_lines,
    _estimate_pod_log_tokens,
    _prioritize_pipeline_pods,
    _truncate_logs_to_token_limit,
)
from helpers.kubearchive_integration import (
    KubeArchiveEndpointDiscovery,
    check_kubearchive_availability,
    normalize_to_rfc3339,
    query_kubearchive_resources,
    setup_kubearchive_client,
)

# Logging, MCP instance, and tool decorator are initialized in middleware
from middleware import log_tool_execution, logger, mcp
from tools.event_rca_tools import (
    _get_namespace_events_as_dicts as _get_namespace_events_as_dicts_impl,
)
from tools.event_rca_tools import (
    _get_namespace_events_internal as _get_namespace_events_internal_impl,
)
from tools.event_rca_tools import (
    advanced_event_analytics_impl,
    automated_triage_rca_report_generator_impl,
    progressive_event_analysis_impl,
    smart_get_namespace_events_impl,
)

# Health check functionality will be handled by the MCP server itself
# The FastMCP framework provides its own health endpoints


# Configure Kubernetes client
try:
    config.load_incluster_config()
    logger.info("Loaded Kubernetes configuration from cluster")
except config.ConfigException:
    try:
        config.load_kube_config()
        logger.info("Loaded Kubernetes configuration from local kubeconfig")
    except config.ConfigException:
        logger.warning("No Kubernetes configuration found. Some tools may not work.")

# Initialize Kubernetes API clients
try:
    k8s_core_api = client.CoreV1Api()
    k8s_apps_api = client.AppsV1Api()
    k8s_custom_api = client.CustomObjectsApi()
    k8s_batch_api = client.BatchV1Api()
    k8s_storage_api = client.StorageV1Api()
    k8s_autoscaling_api = client.AutoscalingV2Api()
except Exception as e:
    logger.warning(f"Failed to initialize Kubernetes API clients: {e}")
    k8s_core_api = None
    k8s_apps_api = None
    k8s_custom_api = None
    k8s_batch_api = None
    k8s_storage_api = None
    k8s_autoscaling_api = None

# Initialize NetworkingV1Api for Ingress support (for KubeArchive discovery on plain Kubernetes)
try:
    k8s_networking_api = client.NetworkingV1Api()
except Exception as e:
    logger.warning(f"Failed to initialize NetworkingV1Api: {e}")
    k8s_networking_api = None

if k8s_core_api is not None and k8s_custom_api is not None:
    kubearchive_endpoint_discovery = KubeArchiveEndpointDiscovery(
        k8s_core_api=k8s_core_api,
        k8s_custom_api=k8s_custom_api,
        k8s_networking_api=k8s_networking_api,
        auto_port_forward=True,
    )
else:
    kubearchive_endpoint_discovery = None


def _is_running_in_cluster() -> bool:
    """Check if we're running inside a Kubernetes cluster."""
    return is_running_in_cluster()


# ============================================================================
# MCP TOOLS
# ============================================================================


@mcp.tool()
async def list_namespaces() -> List[str]:
    """
    List all namespaces in the Kubernetes cluster.

    Returns:
        List[str]: Alphabetically sorted namespace names. Empty list if access denied or cluster unreachable.
    """
    global _namespace_cache

    if not k8s_core_api:
        logger.warning("Kubernetes client not available, cannot list namespaces.")
        return []

    current_time = time.time()
    if (
        _namespace_cache["namespaces"] is not None
        and current_time - _namespace_cache["timestamp"] < _NAMESPACE_CACHE_TTL
    ):
        logger.debug("Returning cached namespace list")
        return _namespace_cache["namespaces"]

    try:
        logger.info("Retrieving all namespaces from Kubernetes cluster")
        namespaces = await asyncio.to_thread(k8s_core_api.list_namespace)
        ns_names = sorted(
            [
                ns.metadata.name
                for ns in namespaces.items
                if ns.metadata and ns.metadata.name
            ]
        )

        _namespace_cache["namespaces"] = ns_names
        _namespace_cache["timestamp"] = current_time

        logger.info(f"Successfully retrieved {len(ns_names)} namespaces")
        return ns_names

    except ApiException as e:
        if e.status == 403:
            logger.warning(
                f"Insufficient permissions to list namespaces: {e.reason}. Check RBAC configuration."
            )
        elif e.status == 401:
            logger.error(
                f"Authentication failed while listing namespaces: {e.reason}. Check kubeconfig."
            )
        else:
            logger.error(f"API error while listing namespaces: {e.status} - {e.reason}")
        return []

    except Exception as e:
        logger.error(
            f"Unexpected error while listing namespaces: {str(e)}", exc_info=True
        )
        return []


async def detect_tekton_namespaces() -> Dict[str, List[str]]:
    """
    Intelligently identifies and categorizes namespaces related to Tekton/CI-CD ecosystems.

    This tool performs advanced pattern matching to detect and classify namespaces that are part of
    or related to Tekton-based CI/CD systems. It uses a hierarchical classification
    system to organize namespaces by their functional role within the CI/CD pipeline infrastructure.

    The detection algorithm uses pattern matching against namespace names to identify:
    - Core Tekton components and services
    - Tekton pipeline and task execution environments
    - Build and compilation workspaces
    - Integration and deployment namespaces
    - Supporting infrastructure and tooling

    Returns:
        Dict[str, List[str]]: Categorized namespace collections with the following structure:
            - "core_tekton": Namespaces containing "tekton" (primary system components)
            - "tekton_related": Namespaces containing tekton-related patterns
            - "pipeline_related": Namespaces containing "pipeline" (CI/CD workflows)
            - "build_related": Namespaces containing "build" (compilation and packaging)
            - "other_relevant": Namespaces matching other CI/CD ecosystem patterns
    """
    try:
        logger.info("Starting Tekton/CI-CD namespace detection and classification")
        all_namespaces = await list_namespaces()

        if not all_namespaces:
            logger.warning(
                "No namespaces retrieved from cluster - returning empty classification"
            )
            return {
                "core_tekton": [],
                "tekton_related": [],
                "pipeline_related": [],
                "build_related": [],
                "other_relevant": [],
            }

        # Define comprehensive patterns for CI/CD ecosystem detection
        cicd_patterns = [
            "tekton",
            "pipeline",
            "build",
            "ci",
            "cd",
            "openshift-pipelines",
            "build-service",
            "release-service",
            "image-controller",
            "integration-service",
            "namespace-lister",
            "pipelines-as-code",
            "smee-client",
            "tekton-operator",
            "user-ns",
            "tekton-chains",
            "tekton-results",
            "tekton-triggers",
        ]

        result = {
            "core_tekton": [],
            "tekton_related": [],
            "pipeline_related": [],
            "build_related": [],
            "other_relevant": [],
        }

        # Classification counters for logging
        classification_stats = {category: 0 for category in result.keys()}
        unclassified_count = 0

        logger.info(
            f"Classifying {len(all_namespaces)} namespaces using {len(cicd_patterns)} patterns"
        )

        for ns in all_namespaces:
            ns_lower = ns.lower()
            classified = False

            # Priority-based classification (order matters)
            if "tekton" in ns_lower:
                result["core_tekton"].append(ns)
                classification_stats["core_tekton"] += 1
                classified = True
            elif "pipeline" in ns_lower:
                result["pipeline_related"].append(ns)
                classification_stats["pipeline_related"] += 1
                classified = True
            elif "build" in ns_lower:
                result["build_related"].append(ns)
                classification_stats["build_related"] += 1
                classified = True
            elif any(pattern in ns_lower for pattern in cicd_patterns):
                result["other_relevant"].append(ns)
                classification_stats["other_relevant"] += 1
                classified = True

            if not classified:
                unclassified_count += 1

        # Sort results within each category for consistent output
        for category in result:
            result[category].sort()

        # Log classification statistics
        total_classified = sum(classification_stats.values())
        logger.info(
            f"Namespace classification complete: {total_classified} CI/CD-related, "
            f"{unclassified_count} other namespaces"
        )

        for category, count in classification_stats.items():
            if count > 0:
                logger.info(f"  {category}: {count} namespaces")

        return result

    except Exception as e:
        logger.error(
            f"Unexpected error during Tekton namespace detection: {str(e)}",
            exc_info=True,
        )
        # Return empty but consistent structure on error
        return {
            "core_tekton": [],
            "tekton_related": [],
            "pipeline_related": [],
            "build_related": [],
            "other_relevant": [],
        }


@mcp.tool()
async def list_pipelineruns(
    namespace: str, limit: Optional[int] = 200
) -> List[Dict[str, Any]]:
    """
    List Tekton PipelineRuns in a namespace with status and timing details.

    Args:
        namespace: Kubernetes namespace to query.
        limit: Maximum number of PipelineRuns to return (default: 200). Set to 0 for no limit.

    Returns:
        List[Dict]: PipelineRuns with keys: name, pipeline, status, started_at, completed_at, duration.
                    Empty list if none found. [{"error": "msg"}] on failure.
    """
    try:
        if not k8s_custom_api:
            return [{"error": "Kubernetes client not available."}]

        logger.info(f"Retrieving PipelineRuns from namespace: {namespace}")

        # Validate namespace parameter
        if not namespace or not isinstance(namespace, str):
            error_msg = (
                f"Invalid namespace parameter: {namespace}. Must be a non-empty string."
            )
            logger.error(error_msg)
            return [{"error": error_msg}]

        # Query Tekton PipelineRuns using Kubernetes Custom Resource API
        list_kwargs = {
            "group": "tekton.dev",
            "version": "v1",
            "namespace": namespace,
            "plural": "pipelineruns",
        }
        if limit:
            list_kwargs["limit"] = limit

        pipeline_runs = await asyncio.to_thread(
            k8s_custom_api.list_namespaced_custom_object, **list_kwargs
        )

        pipeline_run_items = pipeline_runs.get("items", [])
        logger.info(
            f"Found {len(pipeline_run_items)} PipelineRuns in namespace '{namespace}'"
        )

        if not pipeline_run_items:
            logger.info(f"No PipelineRuns found in namespace '{namespace}'")
            return []

        result = []
        processed_count = 0
        error_count = 0

        for pr in pipeline_run_items:
            try:
                # Extract metadata with null safety
                metadata = pr.get("metadata", {})
                spec = pr.get("spec", {})
                status = pr.get("status", {})

                # Get pipeline reference from multiple possible sources
                # Priority: pipelineRef.name > labels > pipelineSpec metadata > unknown
                pipeline_name = "unknown"

                # 1. Check spec.pipelineRef.name (direct reference to named Pipeline)
                pipeline_ref = spec.get("pipelineRef", {})
                if pipeline_ref and pipeline_ref.get("name"):
                    pipeline_name = pipeline_ref.get("name")

                # 2. Check common Tekton labels (used by Konflux and other platforms)
                if pipeline_name == "unknown":
                    labels = metadata.get("labels", {})
                    # Try multiple common label keys
                    pipeline_name = (
                        labels.get("tekton.dev/pipeline")
                        or labels.get("pipelines.tekton.dev/pipeline")
                        or labels.get("pipelines.openshift.io/pipeline")
                        or "unknown"
                    )

                # 3. Check inline pipelineSpec for name/displayName
                if pipeline_name == "unknown":
                    pipeline_spec = spec.get("pipelineSpec", {})
                    if pipeline_spec:
                        # Some inline specs may have displayName or name metadata
                        pipeline_name = (
                            pipeline_spec.get("displayName")
                            or pipeline_spec.get("name")
                            or "inline-pipeline"
                        )

                # Extract status information
                conditions = status.get("conditions", [])
                current_status = "Unknown"
                if conditions:
                    # Get the latest condition (Tekton uses last condition as current status)
                    latest_condition = conditions[-1]
                    current_status = latest_condition.get("reason", "Unknown")

                # Extract timing information
                start_time = status.get("startTime")
                completion_time = status.get("completionTime")

                # Determine if pipeline is still running
                is_running = current_status in (
                    "Running",
                    "Started",
                    "Pending",
                    "PipelineRunPending",
                )

                # Calculate duration using helper function
                # For running pipelines, calculate elapsed time from start
                duration = "unknown"
                duration_seconds = None
                try:
                    duration = calculate_duration(
                        start_time, completion_time, use_current_if_missing=is_running
                    )
                    duration_seconds = calculate_duration_seconds(
                        start_time, completion_time, use_current_if_missing=is_running
                    )
                except Exception as e:
                    logger.debug(
                        f"Duration calculation failed for PipelineRun {metadata.get('name', 'unknown')}: {e}"
                    )
                    duration = "calculation_error"

                pipeline_run_info = {
                    "name": metadata.get("name", "unknown"),
                    "pipeline": pipeline_name,
                    "status": current_status,
                    "started_at": start_time,
                    "completed_at": completion_time,
                    "duration": duration,
                    "duration_seconds": duration_seconds,
                }

                result.append(pipeline_run_info)
                processed_count += 1

            except Exception as e:
                error_count += 1
                logger.warning(f"Error processing individual PipelineRun: {e}")
                # Continue processing other PipelineRuns instead of failing completely
                continue

        logger.info(
            f"Successfully processed {processed_count} PipelineRuns from namespace '{namespace}' "
            f"({error_count} errors encountered)"
        )
        return result

    except ApiException as e:
        if e.status == 404:
            logger.warning(
                f"Namespace '{namespace}' not found or no PipelineRuns accessible"
            )
            return []
        elif e.status == 403:
            error_msg = (
                f"Insufficient permissions to list PipelineRuns in namespace '{namespace}'. "
                f"Required RBAC: pipelineruns.tekton.dev/list"
            )
            logger.error(error_msg)
            return [{"error": error_msg}]
        elif e.status == 401:
            error_msg = f"Authentication failed while accessing namespace '{namespace}'. Check kubeconfig."
            logger.error(error_msg)
            return [{"error": error_msg}]
        else:
            error_msg = f"API error listing PipelineRuns in namespace '{namespace}': {e.status} - {e.reason}"
            logger.error(error_msg)
            return [{"error": error_msg}]

    except Exception as e:
        error_msg = f"Unexpected error listing PipelineRuns in namespace '{namespace}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        return [{"error": error_msg}]


@mcp.tool()
async def list_taskruns(
    namespace: str, pipeline_run: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    List Tekton TaskRuns in a namespace, optionally filtered by a specific PipelineRun.

    Args:
        namespace: Kubernetes namespace to query.
        pipeline_run: Optional PipelineRun name to filter by.

    Returns:
        List[Dict]: TaskRuns with keys: name, task, pipeline_run, status, started_at, completed_at, duration.
    """
    try:
        if not k8s_custom_api:
            return [{"error": "Kubernetes client not available."}]

        logger.info(
            f"Retrieving TaskRuns from namespace: {namespace}"
            + (f" (filtered by PipelineRun: {pipeline_run})" if pipeline_run else "")
        )

        label_selector = (
            f"tekton.dev/pipelineRun={pipeline_run}" if pipeline_run else None
        )

        # When filtering by pipeline_run, the label_selector narrows the result set
        # so no limit is needed. Without a filter, limit to prevent fetching all
        # TaskRuns in large namespaces (can be 2000+ objects, ~97MB response).
        list_kwargs = {
            "group": "tekton.dev",
            "version": "v1",
            "namespace": namespace,
            "plural": "taskruns",
        }
        if label_selector:
            list_kwargs["label_selector"] = label_selector
        else:
            list_kwargs["limit"] = 200

        task_runs = await asyncio.to_thread(
            k8s_custom_api.list_namespaced_custom_object, **list_kwargs
        )

        result = []
        for tr in task_runs.get("items", []):
            # Skip if filtering by pipeline_run and this task doesn't match
            if (
                pipeline_run
                and tr.get("metadata", {})
                .get("labels", {})
                .get("tekton.dev/pipelineRun")
                != pipeline_run
            ):
                continue

            metadata = tr.get("metadata", {})
            spec = tr.get("spec", {})
            status = tr.get("status", {})
            labels = metadata.get("labels", {})

            conditions = status.get("conditions", [])
            current_status = (
                conditions[0].get("reason", "Unknown") if conditions else "Unknown"
            )

            # Determine if task is still running
            is_running = current_status in (
                "Running",
                "Started",
                "Pending",
                "TaskRunPending",
            )

            start_time = status.get("startTime")
            completion_time = status.get("completionTime")

            # Get task name from multiple possible sources
            # Priority: taskRef.name > labels > pipelineTask label > extract from taskrun name
            task_name = None

            # 1. Check spec.taskRef.name (direct reference to named Task)
            task_ref = spec.get("taskRef", {})
            if task_ref and task_ref.get("name"):
                task_name = task_ref.get("name")

            # 2. Check common Tekton labels
            if not task_name:
                task_name = (
                    labels.get("tekton.dev/task")
                    or labels.get("tekton.dev/pipelineTask")
                    or labels.get("pipelines.tekton.dev/task")
                )

            # 3. Try to extract from TaskRun name (format: pipelinerun-taskname-suffix)
            if not task_name:
                tr_name = metadata.get("name", "")
                pr_name = labels.get("tekton.dev/pipelineRun", "")
                if pr_name and tr_name.startswith(pr_name + "-"):
                    # Remove pipelinerun prefix and random suffix
                    remaining = tr_name[len(pr_name) + 1 :]
                    # Task name is everything except the last random suffix (usually 5-6 chars)
                    parts = remaining.rsplit("-", 1)
                    if len(parts) > 1 and len(parts[-1]) <= 6:
                        task_name = parts[0]

            result.append(
                {
                    "name": metadata.get("name"),
                    "task": task_name,
                    "pipeline_run": labels.get("tekton.dev/pipelineRun"),
                    "status": current_status,
                    "started_at": start_time,
                    "completed_at": completion_time,
                    "duration": calculate_duration(
                        start_time, completion_time, use_current_if_missing=is_running
                    ),
                    "duration_seconds": calculate_duration_seconds(
                        start_time, completion_time, use_current_if_missing=is_running
                    ),
                }
            )

        logger.info(f"Found {len(result)} TaskRuns in namespace '{namespace}'")
        return result

    except ApiException as e:
        logger.error(f"Error listing TaskRuns in namespace {namespace}: {e}")
        return [{"error": str(e)}]


@mcp.tool()
async def list_pods_in_namespace(namespace: str) -> List[Dict[str, Any]]:
    """
    List all pods in a Kubernetes namespace with status and placement info.

    Args:
        namespace: Kubernetes namespace to query.

    Returns:
        List[Dict]: Pods with keys: name, status, ip, node_name, creation_timestamp,
                    restart_count, container_states (list of waiting/terminated reasons).
    """
    if not k8s_core_api:
        return [{"error": "Kubernetes client not available."}]

    pods_info = []
    try:
        logger.info(f"Listing pods in namespace: {namespace}")
        pod_list_resp = await asyncio.to_thread(
            k8s_core_api.list_namespaced_pod, namespace=namespace
        )
        pod_list = pod_list_resp.items
        for pod in pod_list:
            # Extract container status information for better prioritization
            total_restart_count = 0
            container_states = []

            # Guard against pod.status being None (pods in early creation)
            if pod.status and pod.status.container_statuses:
                for cs in pod.status.container_statuses:
                    if cs.restart_count:
                        total_restart_count += cs.restart_count

                    # Capture waiting state reasons (CrashLoopBackOff, ImagePullBackOff, etc.)
                    if cs.state:
                        if cs.state.waiting and cs.state.waiting.reason:
                            container_states.append(cs.state.waiting.reason)
                        elif cs.state.terminated and cs.state.terminated.reason:
                            container_states.append(cs.state.terminated.reason)

            # Check init container statuses (common failure point in Tekton)
            if pod.status and pod.status.init_container_statuses:
                for ics in pod.status.init_container_statuses:
                    if ics.restart_count:
                        total_restart_count += ics.restart_count
                    if ics.state:
                        if ics.state.waiting and ics.state.waiting.reason:
                            container_states.append(f"Init:{ics.state.waiting.reason}")
                        elif (
                            ics.state.terminated
                            and ics.state.terminated.reason
                            and ics.state.terminated.reason != "Completed"
                        ):
                            container_states.append(
                                f"Init:{ics.state.terminated.reason}"
                            )

            pods_info.append(
                {
                    "name": pod.metadata.name,
                    "status": pod.status.phase if pod.status else "Unknown",
                    "ip": pod.status.pod_ip if pod.status else None,
                    "node_name": pod.spec.node_name if pod.spec else "N/A",
                    "creation_timestamp": (
                        pod.metadata.creation_timestamp.isoformat()
                        if pod.metadata.creation_timestamp
                        else "N/A"
                    ),
                    "restart_count": total_restart_count,
                    "container_states": container_states,
                }
            )
        logger.info(f"Found {len(pods_info)} pods in namespace '{namespace}'.")
        return pods_info
    except ApiException as e:
        logger.error(f"API error listing pods in namespace '{namespace}': {e}")
        return [{"error": f"API Error: {e.reason}", "namespace": namespace}]
    except Exception as e:
        logger.error(
            f"Unexpected error listing pods in namespace '{namespace}': {e}",
            exc_info=True,
        )
        return [{"error": f"Unexpected Error: {str(e)}", "namespace": namespace}]


@mcp.tool()
async def get_kubernetes_resource(
    resource_type: str,
    name: str,
    namespace: str = "default",
    output_format: str = "summary",
) -> str:
    """
    Retrieve details about a Kubernetes/Tekton resource.

    Args:
        resource_type: Resource type. Supported: pod, service, configmap, secret, pvc, namespace, node,
                       serviceaccount, endpoints, event, persistentvolume, resourcequota, limitrange,
                       deployment, replicaset, daemonset, statefulset, job, cronjob, ingress,
                       storageclass, hpa (horizontalpodautoscaler),
                       pipelinerun, taskrun, pipeline, task, clustertask,
                       triggertemplate, triggerbinding, eventlistener,
                       podmonitor, servicemonitor, prometheusrule, alertmanager,
                       application, component, snapshot, release, releaseplan,
                       releaseplanadmission, integrationtestscenario.
        name: Resource name.
        namespace: Namespace (default: "default").
        output_format: "summary", "detailed", or "yaml" (default: "summary").

    Returns:
        str: Formatted resource information.
    """
    if not k8s_core_api:
        return "Error: Kubernetes client not available."

    try:
        resource_type = resource_type.lower().strip()

        # Define resource mappings
        core_resources = {
            "pod": ("pods", "v1"),
            "service": ("services", "v1"),
            "configmap": ("config_maps", "v1"),
            "secret": ("secrets", "v1"),
            "pvc": ("persistent_volume_claims", "v1"),
            "persistentvolumeclaim": ("persistent_volume_claims", "v1"),
            "namespace": ("namespaces", "v1"),
            "node": ("nodes", "v1"),
            "serviceaccount": ("service_accounts", "v1"),
            "endpoints": ("endpoints", "v1"),
            "event": ("events", "v1"),
            "persistentvolume": ("persistent_volumes", "v1"),
            "pv": ("persistent_volumes", "v1"),
            "resourcequota": ("resource_quotas", "v1"),
            "limitrange": ("limit_ranges", "v1"),
        }

        apps_resources = {
            "deployment": ("deployments", "apps/v1"),
            "replicaset": ("replica_sets", "apps/v1"),
            "daemonset": ("daemon_sets", "apps/v1"),
            "statefulset": ("stateful_sets", "apps/v1"),
        }

        batch_resources = {
            "job": ("jobs", "batch/v1"),
            "cronjob": ("cron_jobs", "batch/v1"),
        }

        networking_resources = {"ingress": ("ingresses", "networking.k8s.io/v1")}

        storage_resources = {
            "storageclass": ("storage_classes", "storage.k8s.io/v1"),
            "sc": ("storage_classes", "storage.k8s.io/v1"),
        }

        autoscaling_resources = {
            "horizontalpodautoscaler": ("horizontal_pod_autoscalers", "autoscaling/v2"),
            "hpa": ("horizontal_pod_autoscalers", "autoscaling/v2"),
        }

        tekton_resources = {
            "pipelinerun": ("pipelineruns", "tekton.dev/v1"),
            "taskrun": ("taskruns", "tekton.dev/v1"),
            "pipeline": ("pipelines", "tekton.dev/v1"),
            "task": ("tasks", "tekton.dev/v1"),
            "clustertask": (
                "clustertasks",
                "tekton.dev/v1beta1",
            ),  # ClusterTask deprecated, stays v1beta1
        }

        tekton_triggers_resources = {
            "triggertemplate": ("triggertemplates", "triggers.tekton.dev/v1beta1"),
            "triggerbinding": ("triggerbindings", "triggers.tekton.dev/v1beta1"),
            "eventlistener": ("eventlisteners", "triggers.tekton.dev/v1beta1"),
        }

        monitoring_resources = {
            "podmonitor": ("podmonitors", "monitoring.coreos.com/v1"),
            "servicemonitor": ("servicemonitors", "monitoring.coreos.com/v1"),
            "prometheusrule": ("prometheusrules", "monitoring.coreos.com/v1"),
            "alertmanager": ("alertmanagers", "monitoring.coreos.com/v1"),
        }

        admission_resources = {
            "validatingadmissionwebhook": (
                "validatingadmissionwebhooks",
                "admissionregistration.k8s.io/v1",
            ),
            "mutatingadmissionwebhook": (
                "mutatingadmissionwebhooks",
                "admissionregistration.k8s.io/v1",
            ),
        }

        konflux_resources = {
            "application": ("applications", "appstudio.redhat.com/v1alpha1"),
            "component": ("components", "appstudio.redhat.com/v1alpha1"),
            "snapshot": ("snapshots", "appstudio.redhat.com/v1alpha1"),
            "release": ("releases", "appstudio.redhat.com/v1alpha1"),
            "releaseplan": ("releaseplans", "appstudio.redhat.com/v1alpha1"),
            "releaseplanadmission": (
                "releaseplanadmissions",
                "appstudio.redhat.com/v1alpha1",
            ),
            "integrationtestscenario": (
                "integrationtestscenarios",
                "appstudio.redhat.com/v1beta2",
            ),
        }

        resource_obj = None
        api_version = None

        # Fetch resource based on type
        if resource_type in core_resources:
            method_name, api_version = core_resources[resource_type]
            if resource_type in ["namespace", "node", "persistentvolume", "pv"]:
                # Cluster-scoped resources
                method = getattr(k8s_core_api, f"read_{method_name[:-1]}")
                resource_obj = method(name=name)
            elif resource_type == "endpoints":
                # Endpoints uses plural form in method name
                resource_obj = await asyncio.to_thread(
                    k8s_core_api.read_namespaced_endpoints,
                    name=name,
                    namespace=namespace,
                )
            else:
                # Namespaced resources
                method = getattr(k8s_core_api, f"read_namespaced_{method_name[:-1]}")
                resource_obj = method(name=name, namespace=namespace)

        elif resource_type in storage_resources:
            # Cluster-scoped storage resources
            resource_obj = await asyncio.to_thread(
                k8s_storage_api.read_storage_class, name=name
            )

        elif resource_type in autoscaling_resources:
            method_name, api_version = autoscaling_resources[resource_type]
            method = getattr(k8s_autoscaling_api, f"read_namespaced_{method_name[:-1]}")
            resource_obj = method(name=name, namespace=namespace)

        elif resource_type in apps_resources:
            method_name, api_version = apps_resources[resource_type]
            method = getattr(k8s_apps_api, f"read_namespaced_{method_name[:-1]}")
            resource_obj = method(name=name, namespace=namespace)

        elif resource_type in batch_resources:
            method_name, _ = batch_resources[resource_type]
            method = getattr(k8s_batch_api, f"read_namespaced_{method_name[:-1]}")
            resource_obj = method(name=name, namespace=namespace)

        elif resource_type in networking_resources:
            method_name, api_version = networking_resources[resource_type]
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group="networking.k8s.io",
                version="v1",
                namespace=namespace,
                plural="ingresses",
                name=name,
            )

        elif resource_type in monitoring_resources:
            method_name, api_version = monitoring_resources[resource_type]
            group, version = api_version.split("/")
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group=group,
                version=version,
                namespace=namespace,
                plural=method_name,
                name=name,
            )

        elif resource_type in admission_resources:
            method_name, api_version = admission_resources[resource_type]
            group, version = api_version.split("/")
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_cluster_custom_object,
                group=group,
                version=version,
                plural=method_name,
                name=name,
            )

        elif resource_type in tekton_resources:
            method_name, api_version = tekton_resources[resource_type]
            group, version = api_version.split("/")

            if resource_type == "clustertask":
                # Cluster-scoped Tekton resource
                resource_obj = await asyncio.to_thread(
                    k8s_custom_api.get_cluster_custom_object,
                    group=group,
                    version=version,
                    plural=method_name,
                    name=name,
                )
            else:
                # Namespaced Tekton resource
                resource_obj = await asyncio.to_thread(
                    k8s_custom_api.get_namespaced_custom_object,
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=method_name,
                    name=name,
                )

        elif resource_type in tekton_triggers_resources:
            method_name, api_version = tekton_triggers_resources[resource_type]
            group, version = api_version.split("/")
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group=group,
                version=version,
                namespace=namespace,
                plural=method_name,
                name=name,
            )

        elif resource_type in konflux_resources:
            method_name, api_version = konflux_resources[resource_type]
            group, version = api_version.split("/")
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group=group,
                version=version,
                namespace=namespace,
                plural=method_name,
                name=name,
            )

        else:
            supported_types = (
                list(core_resources.keys())
                + list(apps_resources.keys())
                + list(batch_resources.keys())
                + list(networking_resources.keys())
                + list(storage_resources.keys())
                + list(autoscaling_resources.keys())
                + list(tekton_resources.keys())
                + list(tekton_triggers_resources.keys())
                + list(monitoring_resources.keys())
                + list(admission_resources.keys())
                + list(konflux_resources.keys())
            )
            return f"Error: Unsupported resource type '{resource_type}'. Supported types: {', '.join(sorted(supported_types))}"

        if not resource_obj:
            return f"Error: Resource '{name}' of type '{resource_type}' not found in namespace '{namespace}'"

        # Format output based on requested format
        if output_format.lower() == "yaml":
            return format_yaml_output(resource_obj, resource_type, name, namespace)
        elif output_format.lower() == "detailed":
            return format_detailed_output(resource_obj, resource_type, name, namespace)
        else:  # summary
            return format_summary_output(resource_obj, resource_type, name, namespace)

    except ApiException as e:
        if e.status == 404:
            return f"Error: Resource '{name}' of type '{resource_type}' not found in namespace '{namespace}'"
        else:
            return f"Kubernetes API Error: {e.status} - {e.reason}"
    except Exception as e:
        return f"Error retrieving resource: {str(e)}"


@mcp.tool()
async def get_pipelinerun_logs(
    pipelinerun_name: str,
    namespace: str,
    clean_logs: bool = True,
    tail_lines: Optional[int] = None,
    since_seconds: Optional[int] = None,
    since_time: Optional[str] = None,
    timestamps: bool = True,
    previous: bool = False,
    max_token_budget: int = 120000,
) -> Dict[str, Any]:
    """
    Fetch logs from all pods in a Tekton PipelineRun with adaptive volume management.

    Prioritizes failed pods and manages token budgets automatically when no time/line filters specified.

    Args:
        pipelinerun_name: PipelineRun name.
        namespace: Kubernetes namespace.
        clean_logs: Clean and format logs (default: True).
        tail_lines: Lines from end (optional).
        since_seconds: Logs newer than N seconds (optional).
        since_time: Logs newer than RFC3339 timestamp (optional).
        timestamps: Include timestamps (default: True).
        previous: Get logs from previous container instance (default: False).
        max_token_budget: Maximum tokens for output (default: 120000). Applies to both adaptive and manual modes.

    Returns:
        Dict[str, Any]: Pod names as keys, logs as values. Includes "_metadata" with processing info.
        Returns {"info": "No pods found..."} if pods are garbage collected - use query_kubearchive tool.
    """
    from tools.tekton_tools import get_pipelinerun_logs_impl

    return await get_pipelinerun_logs_impl(
        pipelinerun_name=pipelinerun_name,
        namespace=namespace,
        clean_logs=clean_logs,
        tail_lines=tail_lines,
        since_seconds=since_seconds,
        since_time=since_time,
        timestamps=timestamps,
        previous=previous,
        max_token_budget=max_token_budget,
        k8s_core_api=k8s_core_api,
        k8s_custom_api=k8s_custom_api,
        adaptive_processor_cls=AdaptiveLogProcessor,
        prioritize_pods_fn=_prioritize_pipeline_pods,
        estimate_tokens_fn=_estimate_pod_log_tokens,
        calculate_tail_lines_fn=_calculate_adaptive_tail_lines,
        truncate_logs_fn=_truncate_logs_to_token_limit,
        clean_pipeline_logs_fn=clean_pipeline_logs,
    )


@mcp.tool()
async def check_resource_constraints(namespace: str) -> Dict[str, Any]:
    """
    Check for resource constraints in a namespace that may impact pipelines.

    Identifies: pending/unschedulable pods, OOMKilled containers, CrashLoopBackOff,
    ImagePullBackOff, high restart counts, and resource quota utilization.

    Args:
        namespace: Kubernetes namespace to inspect.

    Returns:
        Dict[str, Any]: Keys: status (Healthy/Warning/Critical/Error), summary, resource_quotas,
                        pending_pods_due_to_resources, oom_killed_containers, container_issues,
                        high_utilization_quotas, recommendations.
    """
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}

    try:
        # Get pods in the namespace
        pods = await list_pods(namespace, k8s_core_api, logger)

        # Get resource quotas
        resource_quotas = await asyncio.to_thread(
            k8s_core_api.list_namespaced_resource_quota, namespace
        )

        # Check for resource problems in pod status
        resource_issues = []
        pending_pods = []
        oom_killed_pods = []

        for pod in pods:
            pod_name = pod.get("name")
            pod_status = pod.get("status")

            # Fetch detailed pod info once per pod that needs inspection
            if pod_status in ["Failed", "Pending", "Running"]:
                detailed_pod = await asyncio.to_thread(
                    k8s_core_api.read_namespaced_pod, name=pod_name, namespace=namespace
                )

                # Check for pending pods (potential scheduling issues)
                if (
                    pod_status == "Pending"
                    and detailed_pod.status
                    and detailed_pod.status.conditions
                ):
                    for condition in detailed_pod.status.conditions:
                        if (
                            condition.type == "PodScheduled"
                            and condition.status == "False"
                        ):
                            pending_pods.append(
                                {
                                    "pod": pod_name,
                                    "issue": "Unschedulable",
                                    "reason": condition.reason or "Unknown",
                                    "message": condition.message or "",
                                }
                            )
                            break
                    else:
                        pending_pods.append(
                            {
                                "pod": pod_name,
                                "issue": "Pending",
                                "reason": "Unknown",
                                "message": "Pod is pending without specific reason",
                            }
                        )
                elif pod_status == "Pending":
                    pending_pods.append(
                        {
                            "pod": pod_name,
                            "issue": "Pending",
                            "reason": "Unknown",
                            "message": "Pod is pending without specific reason",
                        }
                    )

                # Check container statuses for issues
                def _check_container_statuses(statuses, prefix=""):
                    if not statuses:
                        return
                    for container_status in statuses:
                        cname = (
                            f"{prefix}{container_status.name}"
                            if prefix
                            else container_status.name
                        )
                        # Check current state for waiting issues
                        if (
                            hasattr(container_status, "state")
                            and container_status.state
                        ):
                            if container_status.state.waiting:
                                reason = container_status.state.waiting.reason
                                if reason in [
                                    "CrashLoopBackOff",
                                    "OOMKilled",
                                    "ImagePullBackOff",
                                    "ErrImagePull",
                                    "CreateContainerError",
                                    "CreateContainerConfigError",
                                    "ContainerCreating",
                                ]:
                                    resource_issues.append(
                                        {
                                            "pod": pod_name,
                                            "container": cname,
                                            "issue": reason,
                                            "message": container_status.state.waiting.message
                                            or "",
                                        }
                                    )

                        # Check last_state for OOMKilled (container restarted after OOM)
                        if (
                            hasattr(container_status, "last_state")
                            and container_status.last_state
                        ):
                            if container_status.last_state.terminated:
                                if (
                                    container_status.last_state.terminated.reason
                                    == "OOMKilled"
                                ):
                                    oom_killed_pods.append(
                                        {
                                            "pod": pod_name,
                                            "container": cname,
                                            "issue": "OOMKilled",
                                            "restart_count": container_status.restart_count,
                                            "message": f"Container was OOMKilled and restarted {container_status.restart_count} times",
                                        }
                                    )

                        # Check for high restart counts (potential resource issues)
                        if (
                            container_status.restart_count
                            and container_status.restart_count > 5
                        ):
                            resource_issues.append(
                                {
                                    "pod": pod_name,
                                    "container": cname,
                                    "issue": "HighRestartCount",
                                    "restart_count": container_status.restart_count,
                                    "message": f"Container has restarted {container_status.restart_count} times",
                                }
                            )

                if detailed_pod.status:
                    _check_container_statuses(detailed_pod.status.container_statuses)
                    _check_container_statuses(
                        detailed_pod.status.init_container_statuses, prefix="init:"
                    )

        # Format resource quotas
        quota_data = []
        for quota in resource_quotas.items:
            if quota.status.hard and quota.status.used:
                quota_item = {"name": quota.metadata.name, "resources": {}}

                for resource, hard_limit in quota.status.hard.items():
                    used = quota.status.used.get(resource, "0")
                    quota_item["resources"][resource] = {
                        "limit": hard_limit,
                        "used": used,
                        "utilization": calculate_utilization(used, hard_limit),
                    }

                quota_data.append(quota_item)

        # Check for high utilization quotas
        high_utilization = [
            quota_item
            for quota_item in quota_data
            if any(
                resource.get("utilization", 0) > 80
                for resource in quota_item.get("resources", {}).values()
            )
        ]

        # Determine overall status
        status = "Healthy"
        summary_parts = []

        len(resource_issues) + len(pending_pods) + len(oom_killed_pods)

        if oom_killed_pods:
            status = "Critical"
            summary_parts.append(f"{len(oom_killed_pods)} OOMKilled containers")
        if pending_pods:
            status = "Critical" if status != "Critical" else status
            summary_parts.append(f"{len(pending_pods)} pending/unschedulable pods")
        if resource_issues:
            status = "Warning" if status == "Healthy" else status
            summary_parts.append(f"{len(resource_issues)} container issues")
        if high_utilization:
            status = "Warning" if status == "Healthy" else status
            summary_parts.append(
                f"{len(high_utilization)} quotas with high utilization"
            )

        if summary_parts:
            summary = f"Found: {', '.join(summary_parts)}"
        else:
            summary = "No significant resource constraints detected"

        # Generate recommendations
        recommendations = []
        if oom_killed_pods:
            recommendations.append("Increase memory limits for OOMKilled containers")
            recommendations.append("Review application memory usage patterns")
        if pending_pods:
            unschedulable = [
                p for p in pending_pods if p.get("issue") == "Unschedulable"
            ]
            if unschedulable:
                recommendations.append(
                    "Check node resources - pods cannot be scheduled due to insufficient resources"
                )
            recommendations.append("Review pending pods and their resource requests")
        if resource_issues:
            crash_loops = [
                i for i in resource_issues if i.get("issue") == "CrashLoopBackOff"
            ]
            image_issues = [
                i
                for i in resource_issues
                if i.get("issue") in ["ImagePullBackOff", "ErrImagePull"]
            ]
            config_errors = [
                i
                for i in resource_issues
                if i.get("issue")
                in ["CreateContainerError", "CreateContainerConfigError"]
            ]
            high_restarts = [
                i for i in resource_issues if i.get("issue") == "HighRestartCount"
            ]
            if crash_loops:
                recommendations.append(
                    "Investigate CrashLoopBackOff containers - check logs for errors"
                )
            if image_issues:
                recommendations.append(
                    "Fix image pull issues - verify image names and registry access"
                )
            if config_errors:
                recommendations.append(
                    "Fix container configuration errors - check secrets, configmaps, and volume mounts"
                )
            if high_restarts:
                recommendations.append(
                    "Investigate containers with high restart counts"
                )
        if high_utilization:
            recommendations.append(
                "Monitor resource quota usage and consider increasing limits"
            )

        return {
            "status": status,
            "summary": summary,
            "resource_quotas": quota_data,
            "pending_pods_due_to_resources": pending_pods,
            "oom_killed_containers": oom_killed_pods,
            "container_issues": resource_issues,
            "high_utilization_quotas": high_utilization,
            "recommendations": recommendations,
        }

    except ApiException as e:
        logger.error(
            f"Kubernetes API error checking resource constraints in namespace {namespace}: {e}"
        )
        return {
            "status": "Error",
            "summary": f"Kubernetes API error: {str(e)}",
            "resource_quotas": [],
            "pending_pods_due_to_resources": [],
            "oom_killed_containers": [],
            "container_issues": [],
            "high_utilization_quotas": [],
            "recommendations": ["Check cluster connectivity and permissions"],
            "error": str(e),
        }
    except Exception as e:
        logger.error(
            f"Unexpected error checking resource constraints in namespace {namespace}: {e}"
        )
        return {
            "status": "Error",
            "summary": f"Unexpected error: {str(e)}",
            "resource_quotas": [],
            "pending_pods_due_to_resources": [],
            "oom_killed_containers": [],
            "container_issues": [],
            "high_utilization_quotas": [],
            "recommendations": ["Review logs for detailed error information"],
            "error": str(e),
        }


@mcp.tool()
async def detect_anomalies(
    namespace: str, limit: int = 50
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Detect anomalies in Tekton PipelineRuns/TaskRuns using z-score statistical analysis.

    Identifies unusually long execution times (threshold: 2.5 standard deviations from mean).

    Args:
        namespace: Kubernetes namespace to analyze.
        limit: Max recent PipelineRuns to analyze (default: 50).

    Returns:
        Dict: Keys: pipeline_anomalies, task_anomalies (lists with anomaly details).
    """
    if not k8s_custom_api:
        return {"pipeline_anomalies": [], "task_anomalies": []}

    try:
        # Get pipeline runs
        pipeline_runs = await list_pipelineruns(namespace)

        # Limit to the most recent runs
        # Use 'or ""' to handle None values (not just missing keys)
        pipeline_runs = sorted(
            pipeline_runs, key=lambda x: x.get("started_at") or "", reverse=True
        )[:limit]

        # Get ALL task runs in one API call (bulk fetch instead of N+1)
        all_task_runs = await list_taskruns(namespace, pipeline_run=None)

        # Create a set of pipeline run names for fast lookup
        pr_names = {pr.get("name") for pr in pipeline_runs}

        # Collect durations for anomaly detection
        pipeline_data = []
        task_data = []

        # Process pipeline runs
        for pr in pipeline_runs:
            # Parse pipeline duration
            if (
                pr.get("status") == "Succeeded"
                and pr.get("duration")
                and pr.get("duration") != "unknown"
            ):
                try:
                    value = pr.get("duration").split()[0]
                    if value.replace(".", "", 1).isdigit():
                        pipeline_data.append(
                            {"name": pr.get("name"), "duration": float(value)}
                        )
                except (ValueError, IndexError):
                    continue

        # Process task runs (filter in memory - much faster than N API calls)
        for tr in all_task_runs:
            # Only include tasks belonging to our selected pipeline runs
            tr_pipeline = tr.get("pipeline_run")
            if tr_pipeline not in pr_names:
                continue

            if (
                tr.get("status") == "Succeeded"
                and tr.get("duration")
                and tr.get("duration") != "unknown"
            ):
                try:
                    value = tr.get("duration").split()[0]
                    if value.replace(".", "", 1).isdigit():
                        task_data.append(
                            {
                                "name": tr.get("name"),
                                "duration": float(value),
                                "pipeline_run": tr_pipeline,
                            }
                        )
                except (ValueError, IndexError):
                    continue

        # Detect anomalies
        pipeline_anomaly_result = detect_anomalies_in_data(
            [d["duration"] for d in pipeline_data], pipeline_data
        )
        task_anomaly_result = detect_anomalies_in_data(
            [d["duration"] for d in task_data], task_data
        )

        # Extract anomaly lists from helper function results
        pipeline_anomalies = []
        if pipeline_anomaly_result.get(
            "anomalies_detected"
        ) and pipeline_anomaly_result.get("anomaly_details"):
            for anomaly in pipeline_anomaly_result["anomaly_details"].get(
                "anomalies", []
            ):
                original_data = anomaly.get("original_data", {})
                stats = pipeline_anomaly_result["anomaly_details"]["statistics"]
                pipeline_anomalies.append(
                    {
                        "name": original_data.get("name", "unknown"),
                        "reason": f"Unusually long duration (z-score: {anomaly.get('z_score', 0):.2f})",
                        "actual_value": anomaly.get("value"),
                        "expected_range": (
                            max(0, stats["mean"] - 2.5 * stats["std_dev"]),
                            stats["mean"] + 2.5 * stats["std_dev"],
                        ),
                    }
                )

        task_anomalies = []
        if task_anomaly_result.get("anomalies_detected") and task_anomaly_result.get(
            "anomaly_details"
        ):
            for anomaly in task_anomaly_result["anomaly_details"].get("anomalies", []):
                original_data = anomaly.get("original_data", {})
                stats = task_anomaly_result["anomaly_details"]["statistics"]
                task_anomalies.append(
                    {
                        "name": original_data.get("name", "unknown"),
                        "pipeline_run": original_data.get("pipeline_run", "unknown"),
                        "reason": f"Unusually long duration (z-score: {anomaly.get('z_score', 0):.2f})",
                        "actual_value": anomaly.get("value"),
                        "expected_range": (
                            max(0, stats["mean"] - 2.5 * stats["std_dev"]),
                            stats["mean"] + 2.5 * stats["std_dev"],
                        ),
                    }
                )

        return {
            "pipeline_anomalies": pipeline_anomalies,
            "task_anomalies": task_anomalies,
        }

    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        return {"pipeline_anomalies": [], "task_anomalies": [], "error": str(e)}


# ============================================================================
# INTERNAL HELPER FUNCTIONS
# ============================================================================


async def _get_namespace_events_internal(
    namespace: str,
    last_n_events: Optional[int] = None,
    time_period: Optional[str] = None,
    max_fetch_limit: int = 5000,
) -> Dict[str, Any]:
    """Fetch namespace events. Delegates to event_rca_tools."""
    return await _get_namespace_events_internal_impl(
        namespace=namespace,
        last_n_events=last_n_events,
        time_period=time_period,
        max_fetch_limit=max_fetch_limit,
        k8s_core_api=k8s_core_api,
    )


async def _get_namespace_events_as_dicts(
    namespace: str, limit: int = 100, time_period: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Fetch events as dicts. Delegates to event_rca_tools."""
    return await _get_namespace_events_as_dicts_impl(
        namespace=namespace,
        limit=limit,
        time_period=time_period,
        k8s_core_api=k8s_core_api,
    )


@mcp.tool()
async def smart_get_namespace_events(
    namespace: str,
    last_n_events: Optional[int] = None,
    time_period: Optional[str] = None,
    strategy: str = "auto",
    focus_areas: Optional[List[str]] = None,
    max_context_tokens: int = 8000,
    include_summary: bool = True,
) -> Dict[str, Any]:
    """Adaptive event analysis for a namespace with automatic volume management."""
    return await smart_get_namespace_events_impl(
        namespace=namespace,
        last_n_events=last_n_events,
        time_period=time_period,
        strategy=strategy,
        focus_areas=focus_areas,
        max_context_tokens=max_context_tokens,
        include_summary=include_summary,
        k8s_core_api=k8s_core_api,
        smart_sample_string_events_fn=smart_sample_string_events,
        generate_string_events_summary_fn=generate_string_events_summary,
        generate_string_events_insights_fn=generate_string_events_insights,
        generate_string_events_recommendations_fn=generate_string_events_recommendations,
    )


# @mcp.tool()  # Commented out - Konflux-specific tool
async def get_konflux_components_status() -> Dict[str, Any]:
    """
    Retrieves a comprehensive status overview of all Konflux components across all accessible Kubernetes namespaces.

    This asynchronous function provides a high-level health check and status report for the entire
    Konflux ecosystem deployed within the Kubernetes cluster. It performs:

    1. Discovery of Konflux-related namespaces using pattern matching
    2. Collection of deployment statuses (replicas, availability)
    3. Aggregation of PipelineRun statistics by status
    4. Resource quota usage analysis

    Returns:
        Dict[str, Any]: A dictionary containing comprehensive Konflux status:
            - namespaces: Categorized list of Konflux-related namespaces
            - components: Deployment statuses organized by namespace
            - pipeline_stats: PipelineRun counts and status breakdown per namespace
            - resource_usage: Resource quota utilization per namespace

    Example output structure:
        {
            "namespaces": {
                "core_konflux": ["konflux-ci"],
                "tekton_related": ["tekton-pipelines"],
                ...
            },
            "components": {
                "konflux-ci": {
                    "deployments": [
                        {"name": "controller", "ready": "2/2", ...}
                    ]
                }
            },
            "pipeline_stats": {
                "user-ns-1": {"total": 50, "status_counts": {"Succeeded": 45, "Failed": 5}}
            },
            "resource_usage": {...}
        }
    """
    try:
        if not k8s_apps_api or not k8s_core_api:
            return {"error": "Kubernetes API clients (apps, core) are not available."}

        logger.info("Retrieving Konflux components status across all namespaces")

        # First identify all Konflux namespaces
        tekton_namespaces = await detect_tekton_namespaces()

        # Initialize results
        results = {
            "namespaces": tekton_namespaces,
            "components": {},
            "pipeline_stats": {},
            "resource_usage": {},
        }

        # Count total namespaces for logging
        total_namespaces = sum(len(ns_list) for ns_list in tekton_namespaces.values())
        logger.info(f"Found {total_namespaces} Konflux-related namespaces to analyze")

        # For each Konflux namespace, get key resources
        for namespace_type, namespaces in tekton_namespaces.items():
            for namespace in namespaces:
                # Get deployments
                try:
                    deployments = await asyncio.to_thread(
                        k8s_apps_api.list_namespaced_deployment, namespace
                    )
                    deployment_statuses = []

                    for deployment in deployments.items:
                        deployment_statuses.append(
                            {
                                "name": deployment.metadata.name,
                                "ready": f"{deployment.status.ready_replicas or 0}/{deployment.status.replicas}",
                                "up_to_date": deployment.status.updated_replicas,
                                "available": deployment.status.available_replicas,
                            }
                        )

                    if deployment_statuses:
                        if namespace not in results["components"]:
                            results["components"][namespace] = {}
                        results["components"][namespace][
                            "deployments"
                        ] = deployment_statuses
                        logger.debug(
                            f"Found {len(deployment_statuses)} deployments in {namespace}"
                        )

                except ApiException as e:
                    logger.warning(
                        f"Could not get deployments in namespace {namespace}: {e}"
                    )

                # Get pipeline runs stats
                try:
                    pipeline_runs = await list_pipelineruns(namespace)
                    if (
                        pipeline_runs
                        and isinstance(pipeline_runs, list)
                        and not any(
                            "error" in pr
                            for pr in pipeline_runs
                            if isinstance(pr, dict)
                        )
                    ):
                        # Count by status
                        status_counts = {}
                        for pr in pipeline_runs:
                            status = pr.get("status", "Unknown")
                            status_counts[status] = status_counts.get(status, 0) + 1

                        results["pipeline_stats"][namespace] = {
                            "total": len(pipeline_runs),
                            "status_counts": status_counts,
                        }
                        logger.debug(
                            f"Found {len(pipeline_runs)} pipeline runs in {namespace}"
                        )

                except Exception as e:
                    logger.warning(
                        f"Could not get pipeline runs in namespace {namespace}: {e}"
                    )

                # Get resource quotas
                try:
                    resource_quotas = await asyncio.to_thread(
                        k8s_core_api.list_namespaced_resource_quota, namespace
                    )
                    if resource_quotas.items:
                        results["resource_usage"][namespace] = []
                        for quota in resource_quotas.items:
                            quota_data = {"name": quota.metadata.name, "resources": {}}

                            if quota.status.hard and quota.status.used:
                                for resource, hard_limit in quota.status.hard.items():
                                    used = quota.status.used.get(resource, "0")
                                    quota_data["resources"][resource] = {
                                        "limit": hard_limit,
                                        "used": used,
                                        "utilization": calculate_utilization(
                                            used, hard_limit
                                        ),
                                    }

                            results["resource_usage"][namespace].append(quota_data)

                except ApiException as e:
                    logger.warning(
                        f"Could not get resource quotas in namespace {namespace}: {e}"
                    )

        # Add summary statistics
        total_deployments = sum(
            len(ns_data.get("deployments", []))
            for ns_data in results["components"].values()
        )
        total_pipelines = sum(
            stats.get("total", 0) for stats in results["pipeline_stats"].values()
        )

        results["summary"] = {
            "total_namespaces_analyzed": total_namespaces,
            "namespaces_with_deployments": len(results["components"]),
            "total_deployments": total_deployments,
            "namespaces_with_pipelines": len(results["pipeline_stats"]),
            "total_pipeline_runs": total_pipelines,
        }

        logger.info(
            f"Konflux status complete: {total_deployments} deployments, {total_pipelines} pipeline runs"
        )
        return results

    except Exception as e:
        logger.error(f"Error getting Konflux components status: {e}", exc_info=True)
        return {"error": str(e)}


@mcp.tool()
async def get_pod_logs(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    tail_lines: Optional[int] = None,
    since_seconds: Optional[int] = None,
    since_time: Optional[str] = None,
    timestamps: bool = True,
    previous: bool = False,
) -> Dict[str, Any]:
    """Get logs from a pod. Delegates to log_tools.get_pod_logs_impl."""
    from tools.log_tools import get_pod_logs_impl

    return await get_pod_logs_impl(
        namespace=namespace,
        pod_name=pod_name,
        container_name=container_name,
        tail_lines=tail_lines,
        since_seconds=since_seconds,
        since_time=since_time,
        timestamps=timestamps,
        previous=previous,
        k8s_core_api=k8s_core_api,
    )


@mcp.tool()
async def analyze_logs(log_text: str) -> Dict[str, Any]:
    """Analyze log text to extract error patterns. Delegates to log_tools.analyze_logs_impl."""
    from tools.log_tools import analyze_logs_impl

    return await analyze_logs_impl(log_text)


@mcp.tool()
async def analyze_failed_pipeline(namespace: str, pipeline_run: str) -> Dict[str, Any]:
    """
    Perform root cause analysis on a failed Tekton PipelineRun.

    Fetches pipeline/task details, analyzes logs for errors, and provides remediation recommendations.

    Args:
        namespace: Kubernetes namespace of the PipelineRun.
        pipeline_run: Name of the failed PipelineRun.

    Returns:
        Dict[str, Any]: Keys: pipeline_name, pipeline_status, overall_message, failed_task_count,
                        failed_tasks, probable_root_cause, recommended_actions.
    """
    try:
        if not k8s_custom_api or not k8s_core_api:
            return {"error": "Kubernetes client not available."}

        logger.info(
            f"Analyzing failed pipeline '{pipeline_run}' in namespace '{namespace}'"
        )

        # Get pipeline details
        pipeline_details = await get_pipeline_details(
            namespace,
            pipeline_run,
            k8s_custom_api,
            list_taskruns,
            calculate_duration,
            logger,
        )

        if "error" in pipeline_details:
            return {"error": pipeline_details["error"]}

        # Check if the pipeline actually failed
        if pipeline_details.get("status") == "Succeeded":
            return {
                "error": "Pipeline did not fail, it succeeded",
                "pipeline_status": pipeline_details.get("status"),
            }

        # Find failed tasks
        failed_tasks = [
            task
            for task in pipeline_details.get("task_runs", [])
            if task.get("status") != "Succeeded"
        ]

        results = {
            "pipeline_name": pipeline_details.get("pipeline"),
            "pipeline_status": pipeline_details.get("status"),
            "overall_message": pipeline_details.get("message"),
            "failed_task_count": len(failed_tasks),
            "failed_tasks": [],
        }

        logger.info(
            f"Found {len(failed_tasks)} failed tasks in pipeline '{pipeline_run}'"
        )

        # Detailed analysis of each failed task
        for task in failed_tasks:
            task_name = task.get("name")
            task_details = await get_task_details(
                namespace, task_name, k8s_custom_api, calculate_duration, logger
            )

            # Get logs for the pod associated with this task
            pod_name = task_details.get("pod", "unknown")
            pod_logs_available = True
            log_content = ""
            logs_unavailable_reason = None

            if pod_name == "unknown":
                pod_logs_available = False
                logs_unavailable_reason = "No pod associated with this task"
            else:
                pod_logs = await get_pod_logs(namespace, pod_name)

                # Extract log content as string for analysis
                if isinstance(pod_logs, dict) and "logs" in pod_logs:
                    for container, logs in pod_logs["logs"].items():
                        if isinstance(logs, list):
                            log_content += "\n".join(logs)
                        else:
                            log_content += str(logs)
                elif isinstance(pod_logs, dict) and "error" in pod_logs:
                    pod_logs_available = False
                    error_msg = pod_logs.get("error", "")
                    if "Not Found" in error_msg:
                        logs_unavailable_reason = (
                            "Pod was deleted (normal for completed pipelines)"
                        )
                    else:
                        logs_unavailable_reason = error_msg

            # Build failed step info from TaskRun status as fallback/supplement
            failed_steps = []
            for step in task_details.get("steps", []):
                if step.get("exit_code") is not None and step.get("exit_code") != 0:
                    failed_steps.append(
                        {
                            "step_name": step.get("name"),
                            "exit_code": step.get("exit_code"),
                            "reason": step.get("reason"),
                        }
                    )

            # Analyze logs if available, otherwise use step info for context
            if pod_logs_available and log_content.strip():
                log_analysis = await analyze_logs(log_content)
                error_patterns = log_analysis.get("error_patterns", [])
                error_categories = log_analysis.get("categorized_errors", {})

                # If log analysis found nothing but steps failed, supplement with step info
                if not error_patterns and failed_steps:
                    task_message = task_details.get("message", "")
                    for step in failed_steps:
                        step_msg = f"Step '{step['step_name']}' failed with exit code {step['exit_code']}"
                        if step.get("reason"):
                            step_msg += f" (reason: {step['reason']})"
                        error_patterns.append(step_msg)
                    if task_message:
                        error_patterns.append(f"Task message: {task_message}")
                    error_categories["step_failures"] = len(failed_steps)
            else:
                # Use step failure info when logs unavailable
                error_patterns = []
                error_categories = {}
                for step in failed_steps:
                    error_patterns.append(
                        f"Step '{step['step_name']}' failed with exit code {step['exit_code']}"
                    )
                if failed_steps:
                    error_categories["step_failures"] = len(failed_steps)

            # Build task result
            task_result = {
                "task_name": task.get("task"),
                "task_run": task_name,
                "status": task_details.get("status"),
                "message": task_details.get("message"),
                "error_patterns": error_patterns,
                "error_categories": error_categories,
                "pod": pod_name,
                "failed_steps": failed_steps,
            }

            # Add note if logs were unavailable
            if not pod_logs_available:
                task_result["logs_unavailable"] = True
                task_result["logs_unavailable_reason"] = logs_unavailable_reason

            results["failed_tasks"].append(task_result)

        # Determine root cause and recommend actions
        results["probable_root_cause"] = determine_root_cause(results)
        results["recommended_actions"] = recommend_actions(results)

        logger.info(
            f"Pipeline analysis complete. Root cause: {results['probable_root_cause'][:50]}..."
        )
        return results

    except Exception as e:
        logger.error(
            f"Error analyzing failed pipeline {pipeline_run}: {e}", exc_info=True
        )
        return {"error": str(e)}


@mcp.tool()
async def list_recent_pipeline_runs(limit: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    """
    List recent Tekton PipelineRuns across all accessible namespaces, sorted by start time.

    Args:
        limit: Max PipelineRuns to retrieve (default: 10).

    Returns:
        Dict[str, List[Dict]]: Namespace to PipelineRun list. Each run has: namespace, name,
                               start_time, status, pipeline, labels.
    """
    if not k8s_custom_api:
        return {}

    results: Dict[str, List[Dict[str, Any]]] = {}

    try:
        logger.info(
            f"Listing recent pipeline runs across all namespaces (limit: {limit})"
        )

        # Use cluster-wide query with limit for performance (single API call)
        # Use a fixed fetch limit for consistent results regardless of requested limit
        # The API doesn't sort, so we need to fetch enough to ensure we get the most recent
        fetch_limit = 200  # Fixed limit for consistent results

        pipeline_runs = await asyncio.to_thread(
            k8s_custom_api.list_cluster_custom_object,
            group="tekton.dev",
            version="v1",
            plural="pipelineruns",
            limit=fetch_limit,
        )

        # Collect all pipeline runs
        all_runs: List[Dict[str, Any]] = []

        for pr in pipeline_runs.get("items", []):
            status = pr.get("status", {})
            metadata = pr.get("metadata", {})
            namespace = metadata.get("namespace", "unknown")

            # Get the start time for sorting
            start_time = status.get("startTime")
            if not start_time:
                # If no start time, use creation time
                start_time = metadata.get("creationTimestamp")

            if start_time:
                # Get status from conditions
                conditions = status.get("conditions", [])
                current_status = "Unknown"
                if conditions:
                    current_status = conditions[-1].get("reason", "Unknown")

                # Get pipeline name from multiple sources (same logic as list_pipelineruns)
                spec = pr.get("spec", {})
                labels = metadata.get("labels", {})
                pipeline_name = "unknown"

                # 1. Check spec.pipelineRef.name (direct reference)
                pipeline_ref = spec.get("pipelineRef", {})
                if pipeline_ref and pipeline_ref.get("name"):
                    pipeline_name = pipeline_ref.get("name")

                # 2. Check common Tekton labels (used by Konflux)
                if pipeline_name == "unknown":
                    pipeline_name = (
                        labels.get("tekton.dev/pipeline")
                        or labels.get("pipelines.tekton.dev/pipeline")
                        or labels.get("pipelines.openshift.io/pipeline")
                        or "unknown"
                    )

                # 3. Check inline pipelineSpec
                if pipeline_name == "unknown":
                    pipeline_spec = spec.get("pipelineSpec", {})
                    if pipeline_spec:
                        pipeline_name = (
                            pipeline_spec.get("displayName")
                            or pipeline_spec.get("name")
                            or "inline-pipeline"
                        )

                all_runs.append(
                    {
                        "namespace": namespace,
                        "name": metadata.get("name", "unknown"),
                        "start_time": start_time,
                        "status": current_status,
                        "pipeline": pipeline_name,
                        "labels": labels,
                    }
                )

        logger.info(f"Found {len(all_runs)} pipeline runs from cluster-wide query")

        # Sort by start time (most recent first)
        # Use 'or ""' to handle None values
        all_runs.sort(key=lambda x: x.get("start_time") or "", reverse=True)

        # Group by namespace (limited to top N)
        for run in all_runs[:limit]:
            namespace = run["namespace"]
            if namespace not in results:
                results[namespace] = []
            results[namespace].append(run)

        return results

    except Exception as e:
        logger.error(f"Error listing recent pipeline runs: {e}", exc_info=True)
        return {"error": str(e)}


# @mcp.tool()  # Commented out - Konflux-specific tool
async def track_pipeline_across_namespaces(pipeline_id: str) -> Dict[str, Any]:
    """
    Tracks a specific Konflux pipeline and its associated components across all accessible namespaces.

    This tool provides a comprehensive, holistic view of a Konflux pipeline identified by a unique
    pipeline_id, regardless of which namespace its various execution components reside in.
    Konflux pipelines can span multiple namespaces in multi-tenant or complex deployment scenarios.

    The tracking process involves:
    1. Iterating through all accessible Konflux-related namespaces
    2. Searching for Tekton resources (PipelineRuns, TaskRuns) associated with the pipeline_id
    3. Aggregating status, logs, and metadata of all found components
    4. Constructing a coherent view of the pipeline's execution flow across namespaces

    Args:
        pipeline_id: The unique identifier for the Konflux pipeline to track.
                    This could be a PipelineRun name, Application name, or other identifier
                    that links related resources via labels or naming conventions.

    Returns:
        Dict[str, Any]: Aggregated status and details containing:
            - pipeline_id: The identifier being tracked
            - pipeline_runs: List of associated PipelineRun details with namespace info
            - task_runs: List of associated TaskRun details with namespace info
            - pods: List of related pods with log summaries
            - related_resources: Other resources linked to this pipeline
    """
    try:
        logger.info(f"Tracking pipeline '{pipeline_id}' across all namespaces")

        # Get all relevant namespaces
        tekton_namespaces = await detect_tekton_namespaces()
        all_namespaces = []
        for ns_list in tekton_namespaces.values():
            all_namespaces.extend(ns_list)

        logger.info(
            f"Searching {len(all_namespaces)} namespaces for pipeline '{pipeline_id}'"
        )

        # Track pipeline components
        results = {
            "pipeline_id": pipeline_id,
            "pipeline_runs": [],
            "task_runs": [],
            "pods": [],
            "related_resources": [],
        }

        # Look for pipeline runs in all namespaces
        for namespace in all_namespaces:
            # Look for exact pipeline run by name
            try:
                pipeline_run = await get_pipeline_details(
                    namespace,
                    pipeline_id,
                    k8s_custom_api,
                    list_taskruns,
                    calculate_duration,
                    logger,
                )
                if "error" not in pipeline_run:
                    results["pipeline_runs"].append(
                        {"namespace": namespace, "details": pipeline_run}
                    )

                    # Get related task runs
                    task_runs = await list_taskruns(namespace, pipeline_id)
                    for task_run in task_runs:
                        task_details = await get_task_details(
                            namespace,
                            task_run["name"],
                            k8s_custom_api,
                            calculate_duration,
                            logger,
                        )
                        results["task_runs"].append(
                            {"namespace": namespace, "details": task_details}
                        )

                        # Get related pod
                        pod_name = task_details.get("pod")
                        if pod_name and pod_name != "unknown":
                            pod_logs_result = await get_pod_logs(namespace, pod_name)

                            # Extract log content as string for analysis
                            if (
                                isinstance(pod_logs_result, dict)
                                and "logs" in pod_logs_result
                            ):
                                log_content = ""
                                for pod, logs in pod_logs_result["logs"].items():
                                    if isinstance(logs, list):
                                        log_content += "\n".join(logs)
                                    else:
                                        log_content += str(logs)
                            else:
                                log_content = (
                                    str(pod_logs_result)
                                    if pod_logs_result
                                    else "No pod logs available"
                                )

                            log_analysis = await analyze_logs(log_content)

                            results["pods"].append(
                                {
                                    "namespace": namespace,
                                    "name": pod_name,
                                    "log_summary": generate_log_summary(
                                        log_content,
                                        log_analysis.get("error_patterns", []),
                                        log_analysis.get("categorized_errors", {}),
                                    ),
                                }
                            )
            except Exception as e:
                logger.warning(f"Error tracking pipeline in namespace {namespace}: {e}")

        # Check for pipeline related resources by labels
        for namespace in all_namespaces:
            try:
                # Look for resources with labels related to this pipeline
                pods = await list_pods(namespace, k8s_core_api, logger)
                for pod in pods:
                    labels = pod.get("labels", {})
                    # Check if this pod is related to our pipeline
                    if labels and (
                        labels.get("tekton.dev/pipelineRun") == pipeline_id
                        or labels.get("konflux.pipeline") == pipeline_id
                        or pipeline_id in labels.get("tekton.dev/pipelineRun", "")
                        or pipeline_id in pod.get("name", "")
                    ):
                        results["related_resources"].append(
                            {
                                "kind": "Pod",
                                "namespace": namespace,
                                "name": pod.get("name"),
                                "status": pod.get("status"),
                            }
                        )
            except Exception as e:
                logger.warning(
                    f"Error finding related resources in namespace {namespace}: {e}"
                )

        # Add summary
        results["summary"] = {
            "pipeline_runs_found": len(results["pipeline_runs"]),
            "task_runs_found": len(results["task_runs"]),
            "pods_found": len(results["pods"]),
            "related_resources_found": len(results["related_resources"]),
            "namespaces_searched": len(all_namespaces),
        }

        logger.info(f"Pipeline tracking complete: {results['summary']}")
        return results

    except Exception as e:
        logger.error(f"Error tracking pipeline across namespaces: {e}", exc_info=True)
        return {"error": str(e)}


@mcp.tool()
async def find_pipeline(
    pipeline_id_pattern: str,
    include_taskruns: bool = False,
    max_results: int = 100,
    namespaces: Optional[List[str]] = None,
    pipeline_runs_limit: int = 1000,
    task_runs_limit: int = 500,
) -> Dict[str, Any]:
    """
    Find Tekton pipelines matching a pattern across all accessible namespaces.

    Searches PipelineRuns/TaskRuns by name, labels, or annotations using cluster-wide queries.

    Args:
        pipeline_id_pattern: Pattern to match (partial name, label value, or substring).
        include_taskruns: Include TaskRuns in search results (default: False for performance).
        max_results: Maximum matching results to return per resource type (default: 100).
        namespaces: Optional list of namespaces to search (default: all namespaces).
        pipeline_runs_limit: Max PipelineRuns to fetch from API (default: 1000).
        task_runs_limit: Max TaskRuns to fetch from API if include_taskruns=True (default: 500).

    Returns:
        Dict[str, Any]: Keys: pipeline_runs, task_runs, pipelines_as_code, all_namespaces_checked,
                        diagnostic_info, substring_matches.
    """
    if not k8s_custom_api or not k8s_core_api:
        return {"error": "Kubernetes client not available."}
    from concurrent.futures import ThreadPoolExecutor

    results = {
        "pipeline_runs": [],
        "task_runs": [],
        "all_namespaces_checked": [],
        "diagnostic_info": {},
    }

    try:
        logger.info(
            f"Searching for pipeline pattern '{pipeline_id_pattern}' (include_taskruns={include_taskruns}, max_results={max_results})"
        )
        pattern_lower = pipeline_id_pattern.lower()

        # Use ThreadPoolExecutor for parallel API calls
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=3)

        async def fetch_pipelineruns_namespaced(ns: str):
            try:
                return await asyncio.to_thread(
                    k8s_custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=ns,
                    plural="pipelineruns",
                    limit=pipeline_runs_limit,
                )
            except ApiException as e:
                return {"error": str(e), "items": []}

        async def fetch_pipelineruns_cluster():
            try:
                # Cap at 200 to avoid multi-MB responses causing IncompleteRead
                safe_limit = min(pipeline_runs_limit, 200)
                return await asyncio.to_thread(
                    k8s_custom_api.list_cluster_custom_object,
                    group="tekton.dev",
                    version="v1",
                    plural="pipelineruns",
                    limit=safe_limit,
                )
            except ApiException as e:
                return {"error": str(e), "items": []}

        async def fetch_taskruns_namespaced(ns: str):
            try:
                return await asyncio.to_thread(
                    k8s_custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=ns,
                    plural="taskruns",
                    limit=task_runs_limit,
                )
            except ApiException as e:
                return {"error": str(e), "items": []}

        async def fetch_taskruns_cluster():
            try:
                # Cap at 100 -- cluster-wide TaskRun LIST is the most expensive
                # call (~97MB response). Prefer namespace-scoped queries instead.
                safe_limit = min(task_runs_limit, 100)
                return await asyncio.to_thread(
                    k8s_custom_api.list_cluster_custom_object,
                    group="tekton.dev",
                    version="v1",
                    plural="taskruns",
                    limit=safe_limit,
                )
            except ApiException as e:
                return {"error": str(e), "items": []}

        async def fetch_repositories():
            try:
                return await asyncio.to_thread(
                    k8s_custom_api.list_cluster_custom_object,
                    group="pipelinesascode.tekton.dev",
                    version="v1alpha1",
                    plural="repositories",
                    limit=500,
                )
            except ApiException as e:
                return {"error": str(e), "items": []}

        # Fetch based on namespace targeting
        if namespaces:
            # Targeted namespace search - fetch from specific namespaces in parallel
            logger.info(f"Searching in {len(namespaces)} specified namespaces")
            pr_futures = [
                loop.run_in_executor(executor, fetch_pipelineruns_namespaced, ns)
                for ns in namespaces
            ]
            pipeline_runs_resps = await asyncio.gather(*pr_futures)
            pipeline_runs_resp = {"items": []}
            for resp in pipeline_runs_resps:
                if "error" not in resp:
                    pipeline_runs_resp["items"].extend(resp.get("items", []))
                else:
                    pipeline_runs_resp["error"] = resp.get("error")

            if include_taskruns:
                tr_futures = [
                    loop.run_in_executor(executor, fetch_taskruns_namespaced, ns)
                    for ns in namespaces
                ]
                task_runs_resps = await asyncio.gather(*tr_futures)
                task_runs_resp = {"items": []}
                for resp in task_runs_resps:
                    if "error" not in resp:
                        task_runs_resp["items"].extend(resp.get("items", []))
                    else:
                        task_runs_resp["error"] = resp.get("error")
            else:
                task_runs_resp = {"items": [], "skipped": True}

            repo_future = loop.run_in_executor(executor, fetch_repositories)
            repositories_resp = await repo_future
        else:
            # Cluster-wide search with limits
            pr_future = loop.run_in_executor(executor, fetch_pipelineruns_cluster)
            repo_future = loop.run_in_executor(executor, fetch_repositories)

            if include_taskruns:
                tr_future = loop.run_in_executor(executor, fetch_taskruns_cluster)
                pipeline_runs_resp, task_runs_resp, repositories_resp = (
                    await asyncio.gather(pr_future, tr_future, repo_future)
                )
            else:
                pipeline_runs_resp, repositories_resp = await asyncio.gather(
                    pr_future, repo_future
                )
                task_runs_resp = {"items": [], "skipped": True}

        # Track namespaces found and counts for sampling info
        namespaces_seen = set()
        pr_total_scanned = 0
        tr_total_scanned = 0
        pr_matches_truncated = False
        tr_matches_truncated = False

        # Process PipelineRuns with max_results limit
        if "error" in pipeline_runs_resp:
            results["diagnostic_info"]["pipelineruns_error"] = pipeline_runs_resp[
                "error"
            ]

        pr_items = pipeline_runs_resp.get("items", [])
        for pr in pr_items:
            pr_total_scanned += 1
            namespace = pr.get("metadata", {}).get("namespace", "")
            namespaces_seen.add(namespace)
            pr_name = pr.get("metadata", {}).get("name", "")
            labels = pr.get("metadata", {}).get("labels", {})

            if pattern_lower in pr_name.lower() or any(
                pattern_lower in str(v).lower() for v in labels.values()
            ):
                if len(results["pipeline_runs"]) >= max_results:
                    pr_matches_truncated = True
                    break  # Stop processing once max_results reached
                status = pr.get("status", {})
                conditions = status.get("conditions", [{}])
                condition = conditions[-1] if conditions else {}

                results["pipeline_runs"].append(
                    {
                        "namespace": namespace,
                        "name": pr_name,
                        "status": condition.get("reason", "Unknown"),
                        "message": condition.get("message", ""),
                        "started_at": status.get("startTime", "unknown"),
                        "completion_time": status.get("completionTime", "unknown"),
                        "labels": labels,
                    }
                )

        # Process TaskRuns only if include_taskruns is True
        if task_runs_resp.get("skipped"):
            results["diagnostic_info"][
                "taskruns_skipped"
            ] = "Set include_taskruns=True to search TaskRuns"
        else:
            if "error" in task_runs_resp:
                results["diagnostic_info"]["taskruns_error"] = task_runs_resp["error"]

            tr_items = task_runs_resp.get("items", [])
            for tr in tr_items:
                tr_total_scanned += 1
                namespace = tr.get("metadata", {}).get("namespace", "")
                namespaces_seen.add(namespace)
                tr_name = tr.get("metadata", {}).get("name", "")
                labels = tr.get("metadata", {}).get("labels", {})
                pipeline_run = labels.get("tekton.dev/pipelineRun", "")

                if (
                    pattern_lower in tr_name.lower()
                    or pattern_lower in pipeline_run.lower()
                    or any(pattern_lower in str(v).lower() for v in labels.values())
                ):
                    if len(results["task_runs"]) >= max_results:
                        tr_matches_truncated = True
                        break  # Stop processing once max_results reached
                    status = tr.get("status", {})
                    conditions = status.get("conditions", [{}])
                    condition = conditions[-1] if conditions else {}

                    results["task_runs"].append(
                        {
                            "namespace": namespace,
                            "name": tr_name,
                            "pipeline_run": pipeline_run,
                            "status": condition.get("reason", "Unknown"),
                            "message": condition.get("message", ""),
                            "pod_name": status.get("podName", "unknown"),
                            "labels": labels,
                        }
                    )

        # Process Repositories
        # When namespaces filter is specified, only include repositories from those namespaces
        if "error" in repositories_resp:
            results["diagnostic_info"]["repositories_error"] = repositories_resp[
                "error"
            ]
        for repo in repositories_resp.get("items", []):
            namespace = repo.get("metadata", {}).get("namespace", "")
            repo_name = repo.get("metadata", {}).get("name", "")

            # Skip repositories not in the specified namespaces filter
            if namespaces and namespace not in namespaces:
                continue

            # Only add to namespaces_seen if we're actually considering this repository
            namespaces_seen.add(namespace)

            if pattern_lower in repo_name.lower():
                spec = repo.get("spec", {})
                status = repo.get("status", {})
                results.setdefault("pipelines_as_code", []).append(
                    {
                        "namespace": namespace,
                        "name": repo_name,
                        "url": spec.get("url", "unknown"),
                        "runs": status.get("runs", []),
                    }
                )

        # Set all_namespaces_checked based on what was actually searched
        # If namespaces filter was provided, show those; otherwise show discovered namespaces
        if namespaces:
            results["all_namespaces_checked"] = sorted(namespaces)
        else:
            results["all_namespaces_checked"] = sorted(namespaces_seen)

        # Add summary with sampling info
        results["summary"] = {
            "pipeline_runs_found": len(results["pipeline_runs"]),
            "task_runs_found": len(results["task_runs"]),
            "namespaces_with_tekton_resources": len(namespaces_seen),
            "pipeline_runs_scanned": pr_total_scanned,
            "task_runs_scanned": tr_total_scanned,
            "pipeline_runs_truncated": pr_matches_truncated,
            "task_runs_truncated": tr_matches_truncated,
            "include_taskruns": include_taskruns,
            "max_results_limit": max_results,
        }

        logger.info(f"Pipeline search complete: {results['summary']}")
        return results

    except Exception as e:
        logger.error(
            f"Error finding pipeline {pipeline_id_pattern}: {e}", exc_info=True
        )
        return {"error": str(e), "diagnostic_info": results.get("diagnostic_info", {})}


@mcp.tool()
async def get_tekton_pipeline_runs_status(
    pipeline_runs_limit: int = 500,
    task_runs_limit_per_namespace: int = 100,
    max_namespaces: int = 20,
    recent_failures_limit: int = 10,
    long_running_limit: int = 5,
) -> Dict[str, Any]:
    """
    Get cluster-wide status summary of all Tekton PipelineRuns and TaskRuns.

    Shows running/succeeded/failed counts, recent failures, and long-running pipelines (>1 hour).

    Args:
        pipeline_runs_limit: Max PipelineRuns to fetch cluster-wide (default: 500).
        task_runs_limit_per_namespace: Max TaskRuns to fetch per namespace (default: 100).
        max_namespaces: Max namespaces to scan for TaskRuns (default: 20).
        recent_failures_limit: Max recent failures to include in output (default: 10).
        long_running_limit: Max long-running pipelines to include (default: 5).

    Returns:
        Dict[str, Any]: Keys: timestamp, sampling_info, pipeline_runs (total, by_status,
                        recent_failures [top N], failures_by_namespace, long_running [top N]),
                        task_runs (total, by_status, recent_failures [top N], failures_by_namespace),
                        insights.
    """
    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}
    try:
        logger.info("Fetching cluster-wide Tekton PipelineRuns and TaskRuns status")

        # Cap the limit to avoid massive responses that cause IncompleteRead errors.
        # Cluster-wide LIST on pipelineruns can return multi-MB responses.
        safe_pr_limit = min(pipeline_runs_limit, 200)

        # First, get namespaces with Tekton activity to scope queries
        # Fetch PipelineRuns per-namespace for reliability on large clusters
        all_namespaces = []
        try:
            ns_list = await asyncio.to_thread(
                k8s_core_api.list_namespace,
                label_selector="toolchain.dev.openshift.com/type=tenant",
            )
            all_namespaces = [ns.metadata.name for ns in ns_list.items]
            logger.info(f"Found {len(all_namespaces)} tenant namespaces")
        except Exception:
            # Fallback: cluster-wide query with safe limit
            logger.info(
                "Namespace label selector failed, falling back to cluster-wide query"
            )

        pipeline_runs_items = []
        active_namespaces = set()

        if all_namespaces:
            # Per-namespace fetch with limit -- avoids 97MB cluster-wide responses
            per_ns_limit = max(
                5, safe_pr_limit // min(len(all_namespaces), max_namespaces)
            )
            for ns in all_namespaces[: max_namespaces * 2]:
                try:
                    ns_prs = await asyncio.to_thread(
                        k8s_custom_api.list_namespaced_custom_object,
                        group="tekton.dev",
                        version="v1",
                        namespace=ns,
                        plural="pipelineruns",
                        limit=per_ns_limit,
                    )
                    items = ns_prs.get("items", [])
                    if items:
                        pipeline_runs_items.extend(items)
                        active_namespaces.add(ns)
                except Exception as e:
                    logger.debug(f"Error fetching PipelineRuns from {ns}: {e}")
                    continue
                if len(pipeline_runs_items) >= safe_pr_limit:
                    break
            pipeline_runs_items = pipeline_runs_items[:safe_pr_limit]
        else:
            # Fallback: cluster-wide with safe limit
            pipeline_runs = await asyncio.to_thread(
                k8s_custom_api.list_cluster_custom_object,
                group="tekton.dev",
                version="v1",
                plural="pipelineruns",
                limit=safe_pr_limit,
            )
            pipeline_runs_items = pipeline_runs.get("items", [])
            for pr in pipeline_runs_items:
                ns = pr.get("metadata", {}).get("namespace")
                if ns:
                    active_namespaces.add(ns)

        pipeline_runs = {"items": pipeline_runs_items}

        # Fetch TaskRuns only from active namespaces with limits
        task_runs_items = []
        for ns in list(active_namespaces)[:max_namespaces]:
            try:
                ns_task_runs = await asyncio.to_thread(
                    k8s_custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=ns,
                    plural="taskruns",
                    limit=task_runs_limit_per_namespace,
                )
                task_runs_items.extend(ns_task_runs.get("items", []))
            except Exception as e:
                logger.debug(f"Error fetching TaskRuns from {ns}: {e}")
                continue

        task_runs = {"items": task_runs_items}

        analysis = {
            "timestamp": datetime.now().isoformat(),
            "sampling_info": {
                "pipeline_runs_limit": pipeline_runs_limit,
                "task_runs_limit_per_namespace": task_runs_limit_per_namespace,
                "max_namespaces": max_namespaces,
                "namespaces_sampled": min(len(active_namespaces), max_namespaces),
                "recent_failures_limit": recent_failures_limit,
                "long_running_limit": long_running_limit,
                "note": "Results are sampled to prevent timeout on large clusters",
            },
            "pipeline_runs": {
                "total": len(pipeline_runs.get("items", [])),
                "by_status": {},
                "recent_failures": [],
                "long_running": [],
            },
            "task_runs": {
                "total": len(task_runs.get("items", [])),
                "by_status": {},
                "recent_failures": [],
            },
            "insights": [],
        }

        logger.info(
            f"Analyzing {analysis['pipeline_runs']['total']} PipelineRuns and {analysis['task_runs']['total']} TaskRuns"
        )

        # Analyze PipelineRuns
        for pr in pipeline_runs.get("items", []):
            status = pr.get("status", {})
            conditions = status.get("conditions", [])

            # Get latest condition
            if conditions:
                latest_condition = conditions[-1]
                condition_type = latest_condition.get("type", "Unknown")
                condition_status = latest_condition.get("status", "Unknown")

                status_key = f"{condition_type}_{condition_status}"
                analysis["pipeline_runs"]["by_status"][status_key] = (
                    analysis["pipeline_runs"]["by_status"].get(status_key, 0) + 1
                )

                # Check for failures
                if condition_type == "Succeeded" and condition_status == "False":
                    failure_info = {
                        "name": pr.get("metadata", {}).get("name", "unknown"),
                        "namespace": pr.get("metadata", {}).get("namespace", "unknown"),
                        "reason": latest_condition.get("reason", "Unknown"),
                        "message": latest_condition.get("message", "No message")[
                            :200
                        ],  # Truncate long messages
                        "start_time": status.get("startTime", "Unknown"),
                    }
                    analysis["pipeline_runs"]["recent_failures"].append(failure_info)

                # Check for long-running pipelines
                start_time_str = status.get("startTime")
                if start_time_str and not status.get("completionTime"):
                    try:
                        start_time = datetime.fromisoformat(
                            start_time_str.replace("Z", "+00:00")
                        )
                        runtime = datetime.now(start_time.tzinfo) - start_time
                        if runtime.total_seconds() > 3600:  # 1 hour
                            long_running_info = {
                                "name": pr.get("metadata", {}).get("name", "unknown"),
                                "namespace": pr.get("metadata", {}).get(
                                    "namespace", "unknown"
                                ),
                                "runtime_hours": round(
                                    runtime.total_seconds() / 3600, 2
                                ),
                                "start_time": start_time_str,
                            }
                            analysis["pipeline_runs"]["long_running"].append(
                                long_running_info
                            )
                    except Exception as e:
                        logger.debug(f"Error parsing start time for PipelineRun: {e}")

        # Analyze TaskRuns
        for tr in task_runs.get("items", []):
            status = tr.get("status", {})
            conditions = status.get("conditions", [])

            # Get latest condition
            if conditions:
                latest_condition = conditions[-1]
                condition_type = latest_condition.get("type", "Unknown")
                condition_status = latest_condition.get("status", "Unknown")

                status_key = f"{condition_type}_{condition_status}"
                analysis["task_runs"]["by_status"][status_key] = (
                    analysis["task_runs"]["by_status"].get(status_key, 0) + 1
                )

                # Check for failures
                if condition_type == "Succeeded" and condition_status == "False":
                    failure_info = {
                        "name": tr.get("metadata", {}).get("name", "unknown"),
                        "namespace": tr.get("metadata", {}).get("namespace", "unknown"),
                        "reason": latest_condition.get("reason", "Unknown"),
                        "message": latest_condition.get("message", "No message")[:200],
                        "start_time": status.get("startTime", "Unknown"),
                    }
                    analysis["task_runs"]["recent_failures"].append(failure_info)

        # Aggregate failures by namespace for summary
        pr_failures_by_namespace: Dict[str, int] = {}
        for f in analysis["pipeline_runs"]["recent_failures"]:
            ns = f.get("namespace", "unknown")
            pr_failures_by_namespace[ns] = pr_failures_by_namespace.get(ns, 0) + 1

        tr_failures_by_namespace: Dict[str, int] = {}
        for f in analysis["task_runs"]["recent_failures"]:
            ns = f.get("namespace", "unknown")
            tr_failures_by_namespace[ns] = tr_failures_by_namespace.get(ns, 0) + 1

        # Store total counts before truncating
        total_pr_failures = len(analysis["pipeline_runs"]["recent_failures"])
        total_tr_failures = len(analysis["task_runs"]["recent_failures"])
        total_long_running = len(analysis["pipeline_runs"]["long_running"])

        # Sort failures by start_time (most recent first) and apply limit
        # Use 'or ""' to handle None values (not just missing keys)
        analysis["pipeline_runs"]["recent_failures"].sort(
            key=lambda x: x.get("start_time") or "", reverse=True
        )
        analysis["pipeline_runs"]["recent_failures"] = analysis["pipeline_runs"][
            "recent_failures"
        ][:recent_failures_limit]

        analysis["task_runs"]["recent_failures"].sort(
            key=lambda x: x.get("start_time") or "", reverse=True
        )
        analysis["task_runs"]["recent_failures"] = analysis["task_runs"][
            "recent_failures"
        ][:recent_failures_limit]

        # Sort long_running by runtime (longest first) and apply limit
        analysis["pipeline_runs"]["long_running"].sort(
            key=lambda x: x.get("runtime_hours", 0), reverse=True
        )
        analysis["pipeline_runs"]["long_running"] = analysis["pipeline_runs"][
            "long_running"
        ][:long_running_limit]

        # Add counts and aggregations
        analysis["pipeline_runs"]["total_failures"] = total_pr_failures
        analysis["pipeline_runs"]["failures_by_namespace"] = pr_failures_by_namespace
        analysis["pipeline_runs"]["total_long_running"] = total_long_running

        analysis["task_runs"]["total_failures"] = total_tr_failures
        analysis["task_runs"]["failures_by_namespace"] = tr_failures_by_namespace

        # Generate insights
        if total_pr_failures > 0:
            shown = min(total_pr_failures, recent_failures_limit)
            analysis["insights"].append(
                f"Found {total_pr_failures} failed PipelineRuns (showing top {shown} most recent)"
            )

        if total_tr_failures > 0:
            shown = min(total_tr_failures, recent_failures_limit)
            analysis["insights"].append(
                f"Found {total_tr_failures} failed TaskRuns (showing top {shown} most recent)"
            )

        if total_long_running > 0:
            shown = min(total_long_running, long_running_limit)
            analysis["insights"].append(
                f"Found {total_long_running} long-running pipelines >1 hour (showing top {shown} longest)"
            )

        # Add summary insight
        succeeded_prs = analysis["pipeline_runs"]["by_status"].get("Succeeded_True", 0)
        analysis["pipeline_runs"]["by_status"].get("Succeeded_Unknown", 0)
        if analysis["pipeline_runs"]["total"] > 0:
            success_rate = (succeeded_prs / analysis["pipeline_runs"]["total"]) * 100
            analysis["insights"].append(f"Pipeline success rate: {success_rate:.1f}%")

        logger.info(
            f"Tekton status analysis complete: {len(analysis['insights'])} insights generated"
        )
        return analysis

    except ApiException as e:
        logger.error(f"API error fetching Tekton resources: {e}")
        return {
            "error": f"Kubernetes API error: {e.reason}",
            "status": e.status,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error fetching Tekton resources: {e}", exc_info=True)
        return {
            "error": f"Failed to fetch Tekton resources: {str(e)}",
            "timestamp": datetime.now().isoformat(),
        }


@mcp.tool()
async def detect_log_anomalies(
    logs: str,
    baseline_patterns: Optional[List[str]] = None,
    severity_threshold: str = "medium",
) -> Dict[str, Any]:
    """Detect anomalies in log data. Delegates to log_tools.detect_log_anomalies_impl."""
    from tools.log_tools import detect_log_anomalies_impl

    return await detect_log_anomalies_impl(
        logs=logs,
        baseline_patterns=baseline_patterns,
        severity_threshold=severity_threshold,
    )


@mcp.tool()
async def search_resources_by_labels(
    resource_types: List[str],
    label_selectors: List[Dict[str, Any]],
    namespaces: Optional[List[str]] = None,
    limit_per_type: int = 100,
    include_metadata_only: bool = False,
    include_status: bool = True,
    sort_by: str = "creation_time",
    sort_order: str = "desc",
) -> Dict[str, Any]:
    """
    Search Kubernetes resources by labels across multiple resource types and namespaces.

    Args:
        resource_types: Types to search (e.g., ["pods", "services", "deployments"]).
        label_selectors: Criteria list [{"key": str, "value": str, "operator": "equals|exists|not_equals|in|not_in"}].
        namespaces: Namespaces to search (default: all).
        limit_per_type: Max results per type (default: 100).
        include_metadata_only: Return only metadata (default: False).
        include_status: Include status info (default: True).
        sort_by: "name", "namespace", "creation_time", or "labels" (default: "creation_time").
        sort_order: "asc" or "desc" (default: "desc").

    Returns:
        Dict: Search results with resource details, analysis, and recommendations.
    """
    if not k8s_core_api or not k8s_apps_api or not k8s_custom_api or not k8s_batch_api:
        return {"error": "Kubernetes client not available."}
    start_time = time.time()
    logger.info(
        f"Starting Kubernetes resource search by labels for types: {resource_types}"
    )

    try:
        # Build label selector string
        label_selector = build_advanced_label_selector(label_selectors)
        logger.info(f"Built label selector: {label_selector}")

        # Get accessible namespaces if not specified
        if namespaces is None:
            try:
                ns_response = await asyncio.to_thread(k8s_core_api.list_namespace)
                accessible_namespaces = [ns.metadata.name for ns in ns_response.items]
                logger.info(f"Found {len(accessible_namespaces)} accessible namespaces")
            except ApiException as e:
                logger.warning(
                    f"Could not list namespaces: {e.reason}. Using default namespace"
                )
                accessible_namespaces = ["default"]
        else:
            accessible_namespaces = namespaces

        all_resources = []
        resource_type_counts = {}
        error_details = []

        # Search each resource type
        for resource_type in resource_types:
            logger.info(f"Searching {resource_type} resources")
            type_count = 0

            try:
                api_info = get_resource_api_info(resource_type)
                if not api_info:
                    error_details.append(
                        {
                            "resource_type": resource_type,
                            "namespace": "all",
                            "error_message": f"Unsupported resource type: {resource_type}",
                            "error_code": "UNSUPPORTED_RESOURCE_TYPE",
                        }
                    )
                    continue

                resources_found = []

                if api_info.get("namespaced", True):
                    # Search namespaced resources
                    for namespace in accessible_namespaces:
                        try:
                            if api_info["api"] == "core_v1":
                                api_client = k8s_core_api
                                method = getattr(api_client, api_info["method"])
                                response = method(
                                    namespace=namespace,
                                    label_selector=label_selector,
                                    limit=limit_per_type,
                                )
                            elif api_info["api"] == "apps_v1":
                                api_client = k8s_apps_api
                                method = getattr(api_client, api_info["method"])
                                response = method(
                                    namespace=namespace,
                                    label_selector=label_selector,
                                    limit=limit_per_type,
                                )
                            elif api_info["api"] == "batch_v1":
                                api_client = k8s_batch_api
                                method = getattr(api_client, api_info["method"])
                                response = method(
                                    namespace=namespace,
                                    label_selector=label_selector,
                                    limit=limit_per_type,
                                )
                            elif api_info["api"] == "custom":
                                response = await asyncio.to_thread(
                                    k8s_custom_api.list_namespaced_custom_object,
                                    group=api_info["group"],
                                    version=api_info["version"],
                                    namespace=namespace,
                                    plural=api_info["plural"],
                                    label_selector=label_selector,
                                    limit=limit_per_type,
                                )

                            # Custom objects return dicts, native K8s objects have items attribute
                            if isinstance(response, dict):
                                items = response.get("items", [])
                            elif hasattr(response, "items"):
                                items = response.items
                            else:
                                items = []

                            for item in items:
                                if hasattr(item, "to_dict"):
                                    resource_dict = item.to_dict()
                                else:
                                    resource_dict = item

                                processed_resource = extract_resource_info(
                                    resource_dict,
                                    not include_metadata_only,
                                    include_status,
                                    resource_type_hint=resource_type,
                                )
                                resources_found.append(processed_resource)
                                type_count += 1

                        except ApiException as e:
                            if e.status not in [403, 404]:
                                error_details.append(
                                    {
                                        "resource_type": resource_type,
                                        "namespace": namespace,
                                        "error_message": f"API error: {e.reason}",
                                        "error_code": str(e.status),
                                    }
                                )
                        except Exception as e:
                            error_details.append(
                                {
                                    "resource_type": resource_type,
                                    "namespace": namespace,
                                    "error_message": str(e),
                                    "error_code": "UNEXPECTED_ERROR",
                                }
                            )
                else:
                    # Search cluster-scoped resources
                    try:
                        if api_info["api"] == "core_v1":
                            api_client = k8s_core_api
                            method = getattr(api_client, api_info["method"])
                            response = method(
                                label_selector=label_selector, limit=limit_per_type
                            )

                        # Custom objects return dicts, native K8s objects have items attribute
                        if isinstance(response, dict):
                            items = response.get("items", [])
                        elif hasattr(response, "items"):
                            items = response.items
                        else:
                            items = []

                        for item in items:
                            if hasattr(item, "to_dict"):
                                resource_dict = item.to_dict()
                            else:
                                resource_dict = item

                            processed_resource = extract_resource_info(
                                resource_dict,
                                not include_metadata_only,
                                include_status,
                                resource_type_hint=resource_type,
                            )
                            resources_found.append(processed_resource)
                            type_count += 1

                    except ApiException as e:
                        error_details.append(
                            {
                                "resource_type": resource_type,
                                "namespace": "cluster-scoped",
                                "error_message": f"API error: {e.reason}",
                                "error_code": str(e.status),
                            }
                        )
                    except Exception as e:
                        error_details.append(
                            {
                                "resource_type": resource_type,
                                "namespace": "cluster-scoped",
                                "error_message": str(e),
                                "error_code": "UNEXPECTED_ERROR",
                            }
                        )

                all_resources.extend(resources_found)
                resource_type_counts[resource_type] = type_count
                logger.info(f"Found {type_count} {resource_type} resources")

            except Exception as e:
                logger.error(f"Error searching {resource_type}: {str(e)}")
                error_details.append(
                    {
                        "resource_type": resource_type,
                        "namespace": "all",
                        "error_message": str(e),
                        "error_code": "SEARCH_ERROR",
                    }
                )
                resource_type_counts[resource_type] = 0

        # Sort resources
        sorted_resources = sort_resources(all_resources, sort_by, sort_order)

        # Perform analysis
        label_analysis = analyze_labels(sorted_resources)
        namespace_distribution = calculate_namespace_distribution(sorted_resources)

        # Generate recommendations
        recommendations = []
        if len(error_details) > 0:
            recommendations.append(
                {
                    "type": "permission_check",
                    "description": "Some resources could not be accessed due to permission errors",
                    "affected_resources": [
                        err["resource_type"] for err in error_details
                    ],
                    "suggested_actions": [
                        "Check RBAC permissions",
                        "Verify cluster connectivity",
                        "Confirm resource types exist",
                    ],
                }
            )

        if len(sorted_resources) == 0:
            recommendations.append(
                {
                    "type": "no_results",
                    "description": "No resources found matching the specified label selectors",
                    "affected_resources": resource_types,
                    "suggested_actions": [
                        "Verify label selector syntax",
                        "Check if resources exist with different labels",
                        "Try broader search criteria",
                    ],
                }
            )

        # Calculate duration
        duration_ms = round((time.time() - start_time) * 1000, 2)

        # Build response
        response = {
            "search_summary": {
                "total_resources_found": len(sorted_resources),
                "resource_type_counts": resource_type_counts,
                "namespaces_searched": accessible_namespaces,
                "search_criteria": {
                    "label_selectors": label_selectors,
                    "resource_types": resource_types,
                },
                "search_duration_ms": duration_ms,
            },
            "resources": sorted_resources,
            "label_analysis": label_analysis,
            "namespace_distribution": namespace_distribution,
            "error_details": error_details,
            "recommendations": recommendations,
        }

        logger.info(
            f"Resource search completed. Found {len(sorted_resources)} resources in {duration_ms}ms"
        )
        return response

    except Exception as e:
        error_msg = f"Unexpected error during resource search: {str(e)}"
        logger.error(error_msg, exc_info=True)

        return {
            "search_summary": {
                "total_resources_found": 0,
                "resource_type_counts": {},
                "namespaces_searched": [],
                "search_criteria": {
                    "label_selectors": label_selectors,
                    "resource_types": resource_types,
                },
                "search_duration_ms": round((time.time() - start_time) * 1000, 2),
            },
            "resources": [],
            "label_analysis": {
                "common_labels": [],
                "unique_labels": [],
                "label_patterns": [],
            },
            "namespace_distribution": [],
            "error_details": [
                {
                    "resource_type": "system",
                    "namespace": "all",
                    "error_message": error_msg,
                    "error_code": "SYSTEM_ERROR",
                }
            ],
            "recommendations": [
                {
                    "type": "system_error",
                    "description": "A system error occurred during the search",
                    "affected_resources": resource_types,
                    "suggested_actions": [
                        "Check system logs",
                        "Verify cluster connectivity",
                        "Retry the search",
                    ],
                }
            ],
        }


# ============================================================================
# PROMETHEUS QUERY HELPERS (extracted to tools/prometheus_query.py - issue #72)
# ============================================================================

from tools.prometheus_query import (
    _execute_prometheus_query_internal as _execute_prometheus_query_internal_impl,
)
from tools.prometheus_query import (
    prometheus_query_impl,
)
from tools.prometheus_tools import (
    ci_cd_performance_baselining_tool_impl,
    resource_bottleneck_forecaster_impl,
    what_if_scenario_simulator_impl,
)


async def _execute_prometheus_query_internal(
    query: str, timeout: int = 30
) -> Dict[str, Any]:
    """Thin wrapper passing module-level k8s clients to the extracted implementation."""
    return await _execute_prometheus_query_internal_impl(
        query,
        timeout,
        k8s_core_api=k8s_core_api,
        k8s_custom_api=k8s_custom_api,
    )


@mcp.tool()
async def prometheus_query(
    query: str,
    query_type: str = "instant",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    step: str = "300s",
    cluster: Optional[str] = None,
    format: str = "json",
    namespace_filter: Optional[str] = None,
    limit: Optional[int] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Execute PromQL queries against Prometheus for cluster metrics.

    Supports instant and range queries with automatic endpoint discovery and authentication.

    Args:
        query: PromQL query string.
        query_type: "instant" or "range" (default: "instant").
        start_time: Start for range queries (ISO 8601 or Unix timestamp).
        end_time: End for range queries (ISO 8601 or Unix timestamp).
        step: Step interval for range queries (default: "300s").
        cluster: Cluster domain override.
        format: "json", "table", or "csv" (default: "json").
        namespace_filter: Regex to filter by namespace.
        limit: Max results to return.
        timeout: Query timeout in seconds (default: 30).

    Returns:
        Dict: Query results, metadata, execution info, and analysis.
    """
    return await prometheus_query_impl(
        query=query,
        query_type=query_type,
        start_time=start_time,
        end_time=end_time,
        step=step,
        cluster=cluster,
        format=format,
        namespace_filter=namespace_filter,
        limit=limit,
        timeout=timeout,
        k8s_core_api=k8s_core_api,
        k8s_custom_api=k8s_custom_api,
    )


# ============================================================================
# SMART LOG ANALYSIS HELPER FUNCTIONS
# ============================================================================


async def _quick_volume_estimate(namespace: str, pod_name: str) -> int:
    """Quick log volume estimate. Delegates to log_tools._quick_volume_estimate_impl."""
    from tools.log_tools import _quick_volume_estimate_impl

    return await _quick_volume_estimate_impl(
        namespace=namespace,
        pod_name=pod_name,
        get_pod_logs_fn=get_pod_logs,
    )


# ============================================================================
# SMART LOG ANALYSIS TOOLS
# ============================================================================


@mcp.tool()
async def smart_summarize_pod_logs(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    summary_level: str = "detailed",
    focus_areas: Optional[List[str]] = None,
    time_segments: int = 5,
    max_context_tokens: int = 10000,
    since_seconds: Optional[int] = None,
    tail_lines: Optional[int] = None,
    time_period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Adaptive pod log analysis with automatic volume management and multi-pass processing.

    When no time constraints specified, automatically estimates volume and selects optimal time windows.

    Args:
        namespace: Kubernetes namespace.
        pod_name: Pod name to analyze.
        container_name: Specific container (if multiple).
        summary_level: "brief", "detailed", or "comprehensive" (default: "detailed").
        focus_areas: Analysis focus (default: ["errors", "warnings", "performance"]).
        time_segments: Time-based segments to analyze (default: 5).
        max_context_tokens: Max tokens for analysis (default: 10000).
        since_seconds: Only if user specifies exact seconds.
        tail_lines: Only if user specifies exact line count.
        time_period: Only if user specifies period (e.g., "1h", "30m").
        start_time: Only if user specifies exact start time.
        end_time: Only if user specifies exact end time.

    Returns:
        Dict[str, Any]: Log analysis with insights, patterns, and recommendations.
    """
    from tools.log_tools import smart_summarize_pod_logs_impl

    return await smart_summarize_pod_logs_impl(
        namespace=namespace,
        pod_name=pod_name,
        container_name=container_name,
        summary_level=summary_level,
        focus_areas=focus_areas,
        time_segments=time_segments,
        max_context_tokens=max_context_tokens,
        since_seconds=since_seconds,
        tail_lines=tail_lines,
        time_period=time_period,
        start_time=start_time,
        end_time=end_time,
        k8s_core_api=k8s_core_api,
        get_pod_logs_fn=get_pod_logs,
    )


@mcp.tool()
async def investigate_tls_certificate_issues(
    time_range: str = "24h",
    max_namespaces: int = 20,
    focus_on_system_namespaces: bool = True,
) -> Dict[str, Any]:
    """
    Investigate TLS/certificate issues across the cluster with targeted search and analysis.

    Searches system namespaces for TLS error patterns and correlates with certificate events.

    Args:
        time_range: Search time range (default: "24h").
        max_namespaces: Max namespaces to search (default: 20).
        focus_on_system_namespaces: Prioritize system namespaces (default: True).

    Returns:
        Dict: TLS issues, affected pods, certificate problems, and remediation suggestions.
    """
    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}
    try:
        tool_name = "investigate_tls_certificate_issues"
        logger.info(f"[{tool_name}] Starting TLS certificate issue investigation")

        # Get namespaces to search, prioritizing system namespaces
        all_namespaces = await list_namespaces()

        if focus_on_system_namespaces:
            # Prioritize system namespaces where TLS issues commonly occur
            system_namespaces = [
                ns
                for ns in all_namespaces
                if any(
                    pattern in ns
                    for pattern in [
                        "openshift-",
                        "kube-",
                        "istio-",
                        "ingress",
                        "cert-",
                        "tls-",
                        "monitoring",
                        "logging",
                        "registry",
                        "authentication",
                    ]
                )
            ]
            # Add some Tekton/CI-CD namespaces
            tekton_ns = await detect_tekton_namespaces()
            for category in tekton_ns.values():
                system_namespaces.extend(category[:3])  # Top 3 from each category

            # Remove duplicates and limit
            target_namespaces = list(set(system_namespaces))[:max_namespaces]
        else:
            target_namespaces = all_namespaces[:max_namespaces]

        logger.info(
            f"[{tool_name}] Searching {len(target_namespaces)} namespaces for TLS issues"
        )

        # Search for TLS issues across target namespaces
        tls_issues = []
        affected_pods = []
        certificate_problems = []

        for namespace in target_namespaces:
            try:
                # Get pods in namespace
                pods_info = await list_pods_in_namespace(namespace)

                if not isinstance(pods_info, list) or not pods_info:
                    continue

                # Search pod logs for TLS patterns
                for pod_info in pods_info[:3]:  # Limit to 3 pods per namespace
                    if isinstance(pod_info, dict) and "error" not in pod_info:
                        pod_name = pod_info.get("name", "")

                        try:
                            # Use conservative log analysis focused on TLS issues
                            pod_analysis = await smart_summarize_pod_logs(
                                namespace=namespace,
                                pod_name=pod_name,
                                summary_level="brief",
                                focus_areas=["errors", "security"],
                                max_context_tokens=5000,
                                tail_lines=500,  # Conservative limit
                            )

                            if "error" not in pod_analysis:
                                # Check for TLS patterns in the analysis
                                patterns = pod_analysis.get("patterns", {})
                                error_patterns = patterns.get("errors", [])

                                tls_related_errors = []
                                for error in error_patterns:
                                    error_content = error.get("content", "").lower()
                                    if any(
                                        tls_pattern in error_content
                                        for tls_pattern in [
                                            "tls",
                                            "certificate",
                                            "x509",
                                            "ssl",
                                            "handshake",
                                            "bad certificate",
                                            "certificate verify failed",
                                            "certificate has expired",
                                            "certificate authority",
                                        ]
                                    ):
                                        tls_related_errors.append(error)

                                if tls_related_errors:
                                    tls_issues.extend(tls_related_errors)
                                    affected_pods.append(
                                        {
                                            "namespace": namespace,
                                            "pod_name": pod_name,
                                            "pod_status": pod_info.get(
                                                "status", "Unknown"
                                            ),
                                            "tls_errors": len(tls_related_errors),
                                            "sample_error": tls_related_errors[0].get(
                                                "content", ""
                                            )[:150]
                                            + "...",
                                        }
                                    )

                                    logger.info(
                                        f"[{tool_name}] Found {len(tls_related_errors)} TLS issues in pod {pod_name}"
                                    )

                        except Exception as e:
                            logger.debug(
                                f"Error analyzing pod {pod_name} in {namespace}: {e}"
                            )
                            continue

                # Also check namespace events for certificate-related events
                try:
                    events_result = await smart_get_namespace_events(
                        namespace=namespace,
                        time_period=time_range,
                        focus_areas=["errors", "warnings"],
                        max_context_tokens=3000,
                    )

                    if "events" in events_result and events_result["events"]:
                        for event in events_result["events"][:5]:  # Top 5 events
                            event_content = event.get("event_string", "").lower()

                            tls_patterns = [
                                "certificate",
                                "tls",
                                "x509",
                                "ssl",
                                "handshake",
                            ]
                            matched_pattern = None
                            for pattern in tls_patterns:
                                if pattern in event_content:
                                    matched_pattern = pattern
                                    break

                            if matched_pattern:
                                certificate_problems.append(
                                    {
                                        "namespace": namespace,
                                        "event_type": "kubernetes_event",
                                        "severity": event.get("severity", "UNKNOWN"),
                                        "content": event.get("event_string", "")[:200]
                                        + "...",
                                        "timestamp": event.get("timestamp", "unknown"),
                                    }
                                )

                except Exception as e:
                    logger.debug(f"Error checking events in {namespace}: {e}")

            except Exception as e:
                logger.debug(f"Error processing namespace {namespace}: {e}")
                continue

        # Generate analysis and recommendations
        total_issues = len(tls_issues)
        total_affected_pods = len(affected_pods)
        total_certificate_events = len(certificate_problems)

        analysis_summary = {
            "time_range": time_range,
            "namespaces_searched": len(target_namespaces),
            "total_tls_issues": total_issues,
            "affected_pods": total_affected_pods,
            "certificate_events": total_certificate_events,
            "investigation_focus": (
                "system_namespaces" if focus_on_system_namespaces else "all_namespaces"
            ),
        }

        # Generate specific recommendations for TLS issues
        recommendations = []
        if total_issues > 0:
            recommendations.append(
                f"Found {total_issues} TLS-related issues across {total_affected_pods} pods"
            )
            recommendations.append(
                "Check certificate expiration dates and CA trust chains"
            )
            recommendations.append("Verify service mesh and ingress TLS configurations")

            if any(
                "expired" in issue.get("content", "").lower() for issue in tls_issues
            ):
                recommendations.append(
                    "Certificate expiration detected - immediate renewal required"
                )

            if any(
                "authority" in issue.get("content", "").lower() for issue in tls_issues
            ):
                recommendations.append(
                    "Certificate authority issues detected - check CA trust store"
                )

        else:
            recommendations.append(
                "No TLS certificate issues found in searched namespaces"
            )

        if total_affected_pods > 5:
            recommendations.append(
                "Multiple pods affected - potential cluster-wide certificate issue"
            )

        return {
            "analysis_summary": analysis_summary,
            "tls_issues": tls_issues[:20],  # Limit to top 20 issues
            "affected_pods": affected_pods,
            "certificate_events": certificate_problems,
            "recommendations": recommendations,
            "search_metadata": {
                "tool_optimized_for": "tls_certificate_investigations",
                "token_budget_used": "conservative",
                "search_efficiency": f"{total_issues} issues found across {len(target_namespaces)} namespaces",
            },
        }

    except Exception as e:
        logger.error(
            f"[{tool_name}] Error in TLS investigation: {str(e)}", exc_info=True
        )
        return {
            "error": f"TLS investigation failed: {str(e)}",
            "suggestion": "Try using direct pod log analysis for specific pods with TLS issues",
        }


@mcp.tool()
async def conservative_namespace_overview(
    namespace: str,
    max_pods: int = 10,
    focus_areas: Optional[List[str]] = None,
    sample_strategy: str = "smart",
) -> Dict[str, Any]:
    """
    Conservative namespace analysis optimized for large namespaces with strict token limits.

    Smart-samples critical pods (failed, high-restart, error states) for rapid issue detection.

    Args:
        namespace: Kubernetes namespace to analyze.
        max_pods: Maximum pods to analyze (default: 10).
        focus_areas: Areas to focus on (default: ["errors", "warnings"]).
        sample_strategy: "smart" for intelligent sampling, "recent" for newest pods.

    Returns:
        Dict: Analysis results with pod health, issues detected, and recommendations.
    """
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}
    # Handle mutable default argument - set default inside function
    if focus_areas is None:
        focus_areas = ["errors", "warnings"]

    try:
        tool_name = "conservative_namespace_overview"
        logger.info(
            f"[{tool_name}] Starting conservative analysis of namespace '{namespace}' (max {max_pods} pods)"
        )

        # Ultra-conservative token budget
        max_total_tokens = 45000  # Well under any limit
        tokens_per_pod = max_total_tokens // max_pods

        # Get all pods
        pods_info = await list_pods_in_namespace(namespace)
        if isinstance(pods_info, list) and pods_info and "error" in pods_info[0]:
            return {"error": f"Failed to discover pods: {pods_info[0]['error']}"}

        total_pods = len(pods_info) if isinstance(pods_info, list) else 0
        logger.info(
            f"[{tool_name}] Found {total_pods} pods, will analyze top {min(max_pods, total_pods)}"
        )

        # Report when no pods are found — may indicate RBAC restrictions
        if total_pods == 0:
            return {
                "overview": {
                    "namespace": namespace,
                    "total_pods": 0,
                    "pods_analyzed": 0,
                    "pods_with_issues": 0,
                    "critical_issues_found": 0,
                    "analysis_strategy": "conservative sampling of 0/0 pods",
                },
                "pod_findings": {},
                "critical_issues": [],
                "recommendations": [
                    f"No pods found in namespace '{namespace}'",
                    "This may indicate RBAC restrictions preventing pod listing, or the namespace has no running workloads",
                    "Verify access with: kubectl auth can-i list pods -n " + namespace,
                ],
                "conservative_metadata": {
                    "token_budget": f"<{max_total_tokens:,} tokens (conservative)",
                    "sampling_strategy": sample_strategy,
                    "coverage_ratio": "0/0",
                    "optimized_for": "large_namespaces",
                    "note": "zero_pods_detected",
                },
            }

        # Smart pod selection based on strategy
        if sample_strategy == "smart" and isinstance(pods_info, list):
            # Prioritize pods likely to have issues
            # Uses container_states (CrashLoopBackOff, ImagePullBackOff, Error, OOMKilled)
            # and restart_count from enhanced list_pods_in_namespace
            error_states = {
                "CrashLoopBackOff",
                "ImagePullBackOff",
                "Error",
                "OOMKilled",
                "ContainerCannotRun",
            }
            prioritized_pods = sorted(
                pods_info,
                key=lambda p: (
                    p.get("status") == "Failed",  # Failed pods first (pod phase)
                    any(
                        state in error_states for state in p.get("container_states", [])
                    ),  # Container error states
                    p.get("restart_count", 0) > 0,  # Pods with restarts
                    p.get("restart_count", 0),  # Higher restart count = higher priority
                    "error" in p.get("name", "").lower(),  # Names suggesting issues
                    "failed" in p.get("name", "").lower(),
                ),
                reverse=True,
            )
        else:
            # Recent pods strategy
            prioritized_pods = sorted(
                pods_info, key=lambda p: p.get("creation_timestamp") or "", reverse=True
            )

        # Analyze selected pods with strict token limits
        findings = {}
        issues_found = []

        for i, pod_info in enumerate(prioritized_pods[:max_pods]):
            pod_name = pod_info.get("name", "")
            pod_status = pod_info.get("status", "Unknown")

            try:
                # Ultra-conservative pod analysis
                pod_analysis = await smart_summarize_pod_logs(
                    namespace=namespace,
                    pod_name=pod_name,
                    summary_level="brief",
                    focus_areas=focus_areas,
                    max_context_tokens=tokens_per_pod,
                    tail_lines=200,  # Conservative line limit
                )

                if "error" not in pod_analysis:
                    # Extract only critical information
                    essential_info = {
                        "status": pod_status,
                        "log_lines": pod_analysis.get("metadata", {})
                        .get("processing_metrics", {})
                        .get("total_log_lines", 0),
                        "patterns_found": pod_analysis.get("metadata", {})
                        .get("processing_metrics", {})
                        .get("patterns_extracted", 0),
                        "has_errors": bool(
                            pod_analysis.get("patterns", {}).get("errors")
                        ),
                        "has_warnings": bool(
                            pod_analysis.get("patterns", {}).get("warnings")
                        ),
                    }

                    # Extract top issue if any
                    if pod_analysis.get("patterns", {}).get("errors"):
                        top_error = pod_analysis["patterns"]["errors"][0]
                        essential_info["top_issue"] = f"{top_error['content'][:80]}..."
                        issues_found.append(
                            f"Pod {pod_name}: {essential_info['top_issue']}"
                        )

                    findings[pod_name] = essential_info

                logger.info(
                    f"[{tool_name}] Analyzed pod {i+1}/{min(max_pods, total_pods)}: {pod_name}"
                )

            except Exception as e:
                logger.warning(f"Failed to analyze pod {pod_name}: {e}")
                findings[pod_name] = {"status": pod_status, "error": str(e)}

        # Generate ultra-compact summary
        summary = {
            "namespace": namespace,
            "total_pods": total_pods,
            "pods_analyzed": len(findings),
            "pods_with_issues": len(
                [
                    f
                    for f in findings.values()
                    if f.get("has_errors") or f.get("has_warnings")
                ]
            ),
            "critical_issues_found": len(issues_found),
            "analysis_strategy": f"conservative sampling of {min(max_pods, total_pods)}/{total_pods} pods",
        }

        # Generate focused recommendations
        recommendations = []
        if issues_found:
            recommendations.append(
                f"Found {len(issues_found)} issues requiring investigation"
            )
            recommendations.extend(issues_found[:5])  # Top 5 issues only
        else:
            recommendations.append("No critical issues detected in sampled pods")

        if total_pods > max_pods:
            recommendations.append(
                f"Analyzed {max_pods}/{total_pods} pods - use focused investigation for complete coverage"
            )

        return {
            "overview": summary,
            "pod_findings": findings,
            "critical_issues": issues_found[:5],  # Top 5 only
            "recommendations": recommendations[:5],  # Top 5 only
            "conservative_metadata": {
                "token_budget": f"<{max_total_tokens:,} tokens (conservative)",
                "sampling_strategy": sample_strategy,
                "coverage_ratio": f"{len(findings)}/{total_pods}",
                "optimized_for": "large_namespaces",
            },
        }

    except Exception as e:
        logger.error(
            f"[{tool_name}] Error in conservative analysis: {str(e)}", exc_info=True
        )
        return {
            "error": f"Conservative analysis failed: {str(e)}",
            "namespace": namespace,
            "suggestion": "Try analyzing individual pods directly",
        }


@mcp.tool()
async def adaptive_namespace_investigation(
    namespace: str,
    investigation_query: str = "investigate all logs and events for potential issues",
    max_pods: int = 20,
    focus_areas: Optional[List[str]] = None,
    token_budget: int = 200000,
) -> Dict[str, Any]:
    """
    Adaptive namespace investigation with progressive analysis and token budget management.

    Best for medium namespaces (5-30 pods). Prioritizes failed/error pods, correlates events.

    Args:
        namespace: Kubernetes namespace to investigate.
        investigation_query: What to investigate (default: "investigate all logs and events for potential issues").
        max_pods: Maximum pods to analyze (default: 20).
        focus_areas: Areas to focus on (default: ["errors", "warnings", "performance"]).
        token_budget: Max tokens for investigation (default: 200000).

    Returns:
        Dict: Pod analysis, event correlation, findings, and recommendations.
    """
    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}
    # Handle mutable default argument - set default inside function
    if focus_areas is None:
        focus_areas = ["errors", "warnings", "performance"]

    # Input validation
    if not namespace or not isinstance(namespace, str):
        return {"error": "Invalid namespace parameter: must be a non-empty string"}
    namespace = namespace.strip()
    if not namespace:
        return {"error": "Namespace cannot be empty or whitespace only"}

    if not isinstance(max_pods, int) or max_pods <= 0:
        max_pods = 20  # Reset to default if invalid

    if not isinstance(token_budget, int) or token_budget <= 0:
        token_budget = 200000  # Reset to default if invalid

    try:
        tool_name = "adaptive_namespace_investigation"
        logger.info(
            f"[{tool_name}] Starting adaptive investigation of namespace '{namespace}'"
        )
        logger.info(f"[{tool_name}] Query: {investigation_query}")
        logger.info(
            f"[{tool_name}] Token budget: {token_budget:,}, Max pods: {max_pods}"
        )

        # Initialize adaptive processor with specified budget
        processor = AdaptiveLogProcessor(max_token_budget=token_budget)

        # Phase 1: Smart Discovery (10% of budget)
        discovery_budget = int(token_budget * 0.1)
        logger.info(
            f"[{tool_name}] Phase 1: Discovery (budget: {discovery_budget:,} tokens)"
        )

        # Get all pods in namespace
        pods_info = await list_pods_in_namespace(namespace)
        if isinstance(pods_info, list) and pods_info and "error" in pods_info[0]:
            return {"error": f"Failed to discover pods: {pods_info[0]['error']}"}

        total_pods = len(pods_info) if isinstance(pods_info, list) else 0
        pods_to_analyze = min(max_pods, total_pods)

        # Early return if no pods found
        if total_pods == 0:
            logger.info(f"[{tool_name}] No pods found in namespace '{namespace}'")
            return {
                "investigation_summary": {
                    "namespace": namespace,
                    "status": "no_pods_found",
                    "message": f"No pods found in namespace '{namespace}'",
                },
                "pod_findings": {},
                "namespace_events": {},
                "critical_issues": [],
                "recommendations": [
                    "Verify namespace exists and has running workloads"
                ],
            }

        # Get namespace events for correlation (compressed for synthesis)
        events_result = await smart_get_namespace_events(
            namespace=namespace,
            strategy="smart_summary",
            focus_areas=focus_areas,
            max_context_tokens=discovery_budget // 2,
        )

        # Validate events_result and compress for synthesis
        if not isinstance(events_result, dict):
            events_result = {"error": "Invalid events result type"}
        compressed_events = compress_events_for_synthesis(events_result)

        # Track actual token usage from events result instead of full budget allocation
        actual_event_tokens = events_result.get("token_usage", {}).get(
            "total_estimated", discovery_budget // 4
        )
        processor.record_usage(actual_event_tokens)

        # Phase 2: Intelligent Analysis (80% of budget)
        analysis_budget = int(token_budget * 0.8)
        per_pod_budget = (
            analysis_budget // pods_to_analyze
            if pods_to_analyze > 0
            else analysis_budget
        )

        logger.info(
            f"[{tool_name}] Phase 2: Analysis (budget: {analysis_budget:,} tokens, {per_pod_budget:,} per pod)"
        )

        findings = {}
        critical_issues = []
        pods_analyzed = 0

        # Prioritize pods for analysis
        if isinstance(pods_info, list) and pods_info:
            # Sort pods by priority (failed, high restart count, container error states)
            # Uses container_states and restart_count from enhanced list_pods_in_namespace
            error_states = {
                "CrashLoopBackOff",
                "ImagePullBackOff",
                "Error",
                "OOMKilled",
                "ContainerCannotRun",
            }
            prioritized_pods = sorted(
                pods_info,
                key=lambda p: (
                    p.get("status") == "Failed",  # Failed pods first (pod phase)
                    any(
                        state in error_states for state in p.get("container_states", [])
                    ),  # Container error states
                    p.get("restart_count", 0) > 0,  # Pods with restarts
                    p.get("restart_count", 0),  # Higher restart count = higher priority
                    p.get("name", "").endswith(
                        ("-failed", "-error")
                    ),  # Names indicating issues
                ),
                reverse=True,
            )

            # Process pods in parallel batches for performance
            # Batch size balances parallelism with token budget checks
            batch_size = 4
            pods_to_process = prioritized_pods[:pods_to_analyze]
            summary_level = "brief" if pods_to_analyze > 10 else "detailed"
            max_tokens_per_pod = min(per_pod_budget, 15000)

            async def analyze_single_pod(pod_info: Dict[str, Any]) -> Dict[str, Any]:
                """Analyze a single pod and return results."""
                pod_name = pod_info.get("name", "")
                pod_status = pod_info.get("status", "Unknown")
                try:
                    pod_analysis = await smart_summarize_pod_logs(
                        namespace=namespace,
                        pod_name=pod_name,
                        summary_level=summary_level,
                        focus_areas=focus_areas,
                        max_context_tokens=max_tokens_per_pod,
                    )
                    return {
                        "pod_name": pod_name,
                        "pod_status": pod_status,
                        "analysis": pod_analysis,
                        "error": None,
                    }
                except Exception as e:
                    logger.warning(f"Failed to analyze pod {pod_name}: {e}")
                    return {
                        "pod_name": pod_name,
                        "pod_status": pod_status,
                        "analysis": None,
                        "error": str(e),
                    }

            # Process in batches
            for batch_start in range(0, len(pods_to_process), batch_size):
                # Check token budget before starting batch
                # GUARANTEE: Always process at least the first batch to ensure meaningful results
                is_first_batch = batch_start == 0
                actual_batch_size = min(batch_size, len(pods_to_process) - batch_start)
                batch_budget_needed = per_pod_budget * actual_batch_size

                if not is_first_batch and not processor.can_process_more(
                    batch_budget_needed
                ):
                    logger.info(
                        f"Token budget exhausted - analyzed {pods_analyzed}/{pods_to_analyze} pods"
                    )
                    break

                batch = pods_to_process[batch_start : batch_start + batch_size]
                logger.info(
                    f"[{tool_name}] Processing batch of {len(batch)} pods in parallel"
                )

                # Run batch in parallel
                batch_results = await asyncio.gather(
                    *[analyze_single_pod(p) for p in batch]
                )

                # Process batch results
                for result in batch_results:
                    pod_name = result["pod_name"]
                    pod_status = result["pod_status"]

                    if result["error"]:
                        findings[pod_name] = {
                            "status": pod_status,
                            "error": result["error"],
                        }
                    elif result["analysis"] and "error" not in result["analysis"]:
                        # INTELLIGENT FILTERING: Only keep essential data to prevent token overflow
                        filtered_analysis = filter_analysis_for_synthesis(
                            result["analysis"], focus_areas
                        )

                        findings[pod_name] = {
                            "status": pod_status,
                            "analysis": filtered_analysis,
                            "priority_reason": (
                                "failed_pod"
                                if pod_status == "Failed"
                                else "normal_processing"
                            ),
                        }

                        # Extract critical issues
                        if result["analysis"].get("patterns", {}).get("errors"):
                            critical_issues.extend(
                                [
                                    f"Pod {pod_name}: {error['content'][:100]}..."
                                    for error in result["analysis"]["patterns"][
                                        "errors"
                                    ][:2]
                                ]
                            )

                    # Track actual tokens used from analysis metadata, not the full budget allocation
                    actual_pod_tokens = 0
                    if result["analysis"]:
                        actual_pod_tokens = (
                            result["analysis"]
                            .get("metadata", {})
                            .get("processing_metrics", {})
                            .get("estimated_tokens_used", per_pod_budget // 4)
                        )
                    processor.record_usage(
                        max(actual_pod_tokens, 100)
                    )  # At least 100 tokens per pod
                    pods_analyzed += 1

                logger.info(
                    f"[{tool_name}] Analyzed {pods_analyzed}/{pods_to_analyze} pods so far"
                )

                # Early termination if many critical issues found
                if len(critical_issues) >= 10:
                    logger.info(
                        f"Early termination: {len(critical_issues)} critical issues found"
                    )
                    break

        # Phase 3: Synthesis (10% of budget)
        synthesis_budget = int(token_budget * 0.1)
        logger.info(
            f"[{tool_name}] Phase 3: Synthesis (budget: {synthesis_budget:,} tokens)"
        )

        # Generate comprehensive summary
        investigation_summary = {
            "namespace": namespace,
            "investigation_query": investigation_query,
            "total_pods_found": total_pods,
            "pods_analyzed": pods_analyzed,
            "critical_issues_found": len(critical_issues),
            "token_budget_used": f"{min(processor.get_usage_percentage(), 100.0):.1f}%",
            "adaptive_strategy": "volume-based time windowing with progressive pod analysis",
        }

        # Generate recommendations based on findings
        recommendations = []
        if critical_issues:
            recommendations.append(
                f"{len(critical_issues)} critical issues require immediate attention"
            )
            recommendations.extend(critical_issues[:5])  # Top 5 issues

        if pods_analyzed < total_pods:
            recommendations.append(
                f"Only analyzed {pods_analyzed}/{total_pods} pods due to token constraints - consider focused investigation of remaining pods"
            )

        if not critical_issues and pods_analyzed > 5:
            recommendations.append(
                "No critical issues detected in analyzed pods - namespace appears healthy"
            )

        # FINAL TOKEN SAFETY: Return compressed results to prevent context overflow
        return {
            "investigation_summary": investigation_summary,
            "pod_findings": findings,  # Already filtered per pod
            "namespace_events": compressed_events,  # Compressed events
            "critical_issues": critical_issues[:10],  # Limit to top 10 critical issues
            "recommendations": recommendations[:8],  # Limit to top 8 recommendations
            "adaptive_metadata": {
                "processing_mode": "adaptive",
                "token_efficiency": f"{(pods_analyzed * 1000 / max(1, processor.used_tokens)):.3f} pods per 1k tokens",
                "tokens_used": processor.used_tokens,
                "coverage": f"{pods_analyzed}/{total_pods} pods analyzed",
                "data_filtering": "applied to prevent token overflow",
                "synthesis_optimized": True,
            },
        }

    except Exception as e:
        logger.error(
            f"[{tool_name}] Error in adaptive investigation: {str(e)}", exc_info=True
        )
        return {
            "error": f"Adaptive investigation failed: {str(e)}",
            "namespace": namespace,
            "suggestion": "Try investigating individual pods or use smaller scope",
        }


# ============================================================================
# ETCD LOGS TOOL
# ============================================================================


@mcp.tool()
async def get_etcd_logs(
    tail_lines: Optional[int] = 200,
    since_seconds: Optional[int] = None,
    since_time: Optional[str] = None,
    until_time: Optional[str] = None,
    follow: bool = False,
    timestamps: bool = True,
    previous: bool = False,
    clean_logs: bool = True,
) -> Dict[str, str]:
    """
    Retrieve etcd pod logs from Kubernetes/OpenShift with flexible time and line filtering.

    Auto-detects cluster type and uses appropriate namespace/label selectors.

    Args:
        tail_lines: Lines from end of logs (default: 200, None for all).
        since_seconds: Logs newer than N seconds (overrides tail_lines).
        since_time: Logs newer than RFC3339 timestamp (overrides since_seconds).
        until_time: Logs older than RFC3339 timestamp (requires since_time or since_seconds).
        follow: Stream logs in real-time (default: False).
        timestamps: Include timestamps (default: True).
        previous: Get logs from previous container instance (default: False).
        clean_logs: Clean/format logs (default: True).

    Returns:
        Dict[str, str]: Pod names as keys, logs as values.
    """
    from tools.log_tools import get_etcd_logs_impl

    return await get_etcd_logs_impl(
        tail_lines=tail_lines,
        since_seconds=since_seconds,
        since_time=since_time,
        until_time=until_time,
        follow=follow,
        timestamps=timestamps,
        previous=previous,
        clean_logs=clean_logs,
        k8s_core_api=k8s_core_api,
    )


@mcp.tool()
async def stream_analyze_pod_logs(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    chunk_size: int = 5000,
    analysis_mode: str = "errors_and_warnings",
    time_window: Optional[str] = None,
    follow: bool = False,
    max_chunks: int = 50,
    since_seconds: Optional[int] = None,
    tail_lines: Optional[int] = None,
    time_period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    max_context_tokens: int = 50000,
) -> Dict[str, Any]:
    """
    Stream and analyze pod logs in chunks with progressive pattern detection.

    Processes logs in manageable chunks for memory efficiency and real-time insights.

    Args:
        namespace: Kubernetes namespace.
        pod_name: Pod name to stream logs from.
        container_name: Specific container (if multiple).
        chunk_size: Lines per chunk (default: 5000).
        analysis_mode: "errors_only", "errors_and_warnings" (default), "full_analysis", or "custom_patterns".
        time_window: Time window for historical logs (e.g., "1h", "6h", "24h").
        follow: Stream logs in real-time (default: False).
        max_chunks: Max chunks to process (default: 50).
        since_seconds: Logs from last N seconds.
        tail_lines: Limit to last N lines.
        time_period: Time period (e.g., "1h", "30m").
        start_time: Start time (ISO format).
        end_time: End time (ISO format).
        max_context_tokens: Maximum tokens for output (default: 50000).

    Returns:
        Dict[str, Any]: Keys: chunks, overall_summary, trending_patterns, recommendations, metadata.
    """
    from tools.log_tools import stream_analyze_pod_logs_impl

    return await stream_analyze_pod_logs_impl(
        namespace=namespace,
        pod_name=pod_name,
        container_name=container_name,
        chunk_size=chunk_size,
        analysis_mode=analysis_mode,
        time_window=time_window,
        follow=follow,
        max_chunks=max_chunks,
        since_seconds=since_seconds,
        tail_lines=tail_lines,
        time_period=time_period,
        start_time=start_time,
        end_time=end_time,
        max_context_tokens=max_context_tokens,
        k8s_core_api=k8s_core_api,
        get_pod_logs_fn=get_pod_logs,
    )


@mcp.tool()
async def analyze_pod_logs_hybrid(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    strategy: str = "auto",
    request_type: str = "investigation",
    urgency: str = "medium",
    use_cache: bool = True,
    custom_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Hybrid log analyzer with intelligent strategy selection and caching.

    Automatically selects best analysis approach based on context and urgency.

    Args:
        namespace: Kubernetes namespace.
        pod_name: Pod name to analyze.
        container_name: Specific container (if multiple).
        strategy: "auto" (default), "smart_summary", "streaming", or "hybrid".
        request_type: "investigation", "troubleshooting", or "monitoring".
        urgency: "low", "medium" (default), "high", or "critical".
        use_cache: Use intelligent caching (default: True).
        custom_params: Custom parameters for strategies.

    Returns:
        Dict[str, Any]: Keys: strategy_used, analysis_results, supplementary_insights,
                        performance_metrics, recommendations, cache_info.
    """
    from tools.log_tools import analyze_pod_logs_hybrid_impl

    return await analyze_pod_logs_hybrid_impl(
        namespace=namespace,
        pod_name=pod_name,
        container_name=container_name,
        strategy=strategy,
        request_type=request_type,
        urgency=urgency,
        use_cache=use_cache,
        custom_params=custom_params,
        k8s_core_api=k8s_core_api,
        analysis_cache=analysis_cache,
        smart_summarize_fn=smart_summarize_pod_logs,
        stream_analyze_fn=stream_analyze_pod_logs,
    )


@mcp.tool()
async def progressive_event_analysis(
    namespace: str,
    analysis_level: str = "overview",
    time_period: Optional[str] = None,
    event_filters: Optional[Dict[str, Any]] = None,
    seed_event_id: Optional[str] = None,
    focus_areas: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Progressive event analysis with multiple detail levels and correlation detection.

    Args:
        namespace: Kubernetes namespace to analyze.
        analysis_level: "overview", "detailed", "correlation", or "deep_dive" (default: "overview").
        time_period: Time window (e.g., "2h", "4h", "1d").
        event_filters: Filters like {"severity": ["CRITICAL"], "category": ["FAILURE"]}.
        seed_event_id: Event ID for correlation analysis.
        focus_areas: Areas to emphasize (default: ["errors", "warnings", "failures"]).

    Returns:
        Dict: Analysis results based on selected level.
    """
    return await progressive_event_analysis_impl(
        namespace=namespace,
        analysis_level=analysis_level,
        time_period=time_period,
        event_filters=event_filters,
        seed_event_id=seed_event_id,
        focus_areas=focus_areas,
        k8s_core_api=k8s_core_api,
        smart_get_namespace_events_fn=smart_get_namespace_events,
        progressive_event_analyzer_cls=ProgressiveEventAnalyzer,
    )


@mcp.tool()
async def advanced_event_analytics(
    namespace: str,
    time_period: Optional[str] = None,
    include_ml_patterns: bool = True,
    include_log_correlation: bool = True,
    include_metrics_correlation: bool = True,
    include_runbook_suggestions: bool = True,
    analysis_depth: str = "comprehensive",
) -> Dict[str, Any]:
    """
    Advanced ML-powered event analytics with log/metrics integration and runbook suggestions.

    Args:
        namespace: Kubernetes namespace to analyze.
        time_period: Time window (e.g., "4h", "1d", "12h").
        include_ml_patterns: Enable ML pattern detection (default: True).
        include_log_correlation: Correlate with log data (default: True).
        include_metrics_correlation: Correlate with metrics (default: True).
        include_runbook_suggestions: Generate runbook suggestions (default: True).
        analysis_depth: "basic", "comprehensive" (default), or "deep".

    Returns:
        Dict: Advanced analytics with ML insights, correlations, and runbook suggestions.
    """
    return await advanced_event_analytics_impl(
        namespace=namespace,
        time_period=time_period,
        include_ml_patterns=include_ml_patterns,
        include_log_correlation=include_log_correlation,
        include_metrics_correlation=include_metrics_correlation,
        include_runbook_suggestions=include_runbook_suggestions,
        analysis_depth=analysis_depth,
        k8s_core_api=k8s_core_api,
        progressive_event_analysis_fn=progressive_event_analysis,
        ml_pattern_detector_cls=MLPatternDetector,
        log_metrics_integrator_cls=LogMetricsIntegrator,
        runbook_suggestion_engine_cls=RunbookSuggestionEngine,
        generate_comprehensive_insights_fn=generate_comprehensive_insights,
        assess_overall_risk_fn=assess_overall_risk,
        generate_strategic_recommendations_fn=generate_strategic_recommendations,
    )


@mcp.tool()
async def automated_triage_rca_report_generator(
    failure_identifier: str,
    namespace: Optional[str] = None,
    investigation_depth: str = "standard",
    include_related_failures: bool = True,
    time_window: str = "2h",
    generate_timeline: bool = True,
    include_remediation: bool = True,
) -> Dict[str, Any]:
    """
    Generate automated Root Cause Analysis (RCA) report for pipeline/pod failures.

    Performs log analysis, resource checks, event correlation, and provides remediation suggestions.

    Args:
        failure_identifier: Pipeline run name, pod name, or failure event ID.
        namespace: Optional namespace where the failure occurred. If not provided, searches across detected CI/CD namespaces.
        investigation_depth: "quick", "standard" (default), or "deep".
        include_related_failures: Analyze related recent failures (default: True).
        time_window: Time window for related events (default: "2h").
        generate_timeline: Generate event timeline (default: True).
        include_remediation: Include remediation steps (default: True).

    Returns:
        Dict: RCA report with summary, timeline, root cause, diagnostics, and remediation.
    """
    return await automated_triage_rca_report_generator_impl(
        failure_identifier,
        namespace=namespace,
        investigation_depth=investigation_depth,
        include_related_failures=include_related_failures,
        time_window=time_window,
        generate_timeline=generate_timeline,
        include_remediation=include_remediation,
        k8s_core_api=k8s_core_api,
        k8s_custom_api=k8s_custom_api,
        detect_tekton_namespaces=detect_tekton_namespaces,
        analyze_failed_pipeline=analyze_failed_pipeline,
        analyze_pipeline_performance=analyze_pipeline_performance,
        get_pod_logs=get_pod_logs,
        analyze_logs=analyze_logs,
        detect_log_anomalies=detect_log_anomalies,
        analyze_pipeline_dependencies=analyze_pipeline_dependencies,
        list_pipelineruns=list_pipelineruns,
        smart_get_namespace_events=smart_get_namespace_events,
        categorize_errors=categorize_errors,
        recommend_actions=recommend_actions,
    )


@mcp.tool()
async def check_cluster_certificate_health(
    warning_threshold_days: int = 30,
    critical_threshold_days: int = 7,
    include_system_certs: bool = True,
    namespaces: Optional[List[str]] = None,
    certificate_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Scan for expiring certificates across the cluster to prevent service disruptions.

    Scans TLS secrets, system certificates, and provides renewal recommendations.

    Args:
        warning_threshold_days: Days before expiration for warning (default: 30).
        critical_threshold_days: Days before expiration for critical alert (default: 7).
        include_system_certs: Include system certificates (default: True).
        namespaces: Namespaces to scan (default: all accessible).
        certificate_types: Types to check: "tls", "ca", "client", "server" (default: all).

    Returns:
        Dict: Certificate health with expiration timeline, recommendations, and security findings.
    """
    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}
    try:
        logger.info(
            f"Starting cluster certificate health scan with thresholds: warning={warning_threshold_days}d, critical={critical_threshold_days}d"
        )

        # Initialize result structure
        result = {
            "scan_summary": {
                "total_certificates": 0,
                "healthy_certificates": 0,
                "warning_certificates": 0,
                "critical_certificates": 0,
                "expired_certificates": 0,
                "scan_timestamp": datetime.utcnow().isoformat(),
                "namespaces_scanned": 0,
                "namespaces_skipped_rbac": 0,
                "namespaces_total": 0,
            },
            "certificate_details": [],
            "system_certificates": [],
            "expiration_timeline": [],
            "renewal_recommendations": [],
            "security_findings": [],
            "certificate_authorities": [],
            "scan_coverage": {"scanned_namespaces": [], "skipped_namespaces_rbac": []},
        }

        # Determine namespaces to scan
        target_namespaces = namespaces or []
        if not target_namespaces:
            # Get all accessible namespaces
            try:
                all_ns = await asyncio.to_thread(k8s_core_api.list_namespace)
                target_namespaces = [
                    ns.metadata.name
                    for ns in all_ns.items
                    if ns.metadata and ns.metadata.name
                ]
                logger.info(
                    f"Scanning all {len(target_namespaces)} accessible namespaces"
                )
            except ApiException as e:
                logger.warning(
                    f"Could not list all namespaces, using default set: {e.reason}"
                )
                target_namespaces = [
                    "default",
                    "kube-system",
                    "openshift-config",
                    "openshift-ingress",
                ]

        # Set default certificate types
        if not certificate_types:
            certificate_types = ["tls", "ca", "client", "server"]

        certificates_found = []
        ca_certificates = {}
        scanned_namespaces = []
        skipped_namespaces_rbac = []

        # Scan for TLS secrets in each namespace
        for namespace in target_namespaces:
            try:
                logger.debug(f"Scanning namespace: {namespace}")
                secrets = await asyncio.to_thread(
                    k8s_core_api.list_namespaced_secret, namespace
                )
                scanned_namespaces.append(namespace)

                for secret in secrets.items:
                    if not secret.data:
                        continue

                    # Check if secret contains certificate data
                    cert_keys = [
                        "tls.crt",
                        "ca.crt",
                        "cert",
                        "certificate",
                        "client.crt",
                        "server.crt",
                    ]

                    for key in cert_keys:
                        if key in secret.data:
                            try:
                                # Decode base64 certificate data
                                cert_data = base64.b64decode(secret.data[key]).decode(
                                    "utf-8"
                                )

                                # Handle certificate chains (multiple certificates)
                                cert_blocks = cert_data.split(
                                    "-----END CERTIFICATE-----"
                                )

                                for i, cert_block in enumerate(cert_blocks):
                                    if "-----BEGIN CERTIFICATE-----" in cert_block:
                                        full_cert = (
                                            cert_block + "-----END CERTIFICATE-----"
                                        )
                                        cert_info = parse_certificate(full_cert)

                                        if cert_info:
                                            cert_details = {
                                                "certificate_info": {
                                                    "name": (
                                                        f"{secret.metadata.name}_{key}_{i}"
                                                        if i > 0
                                                        else f"{secret.metadata.name}_{key}"
                                                    ),
                                                    "namespace": namespace,
                                                    "secret_name": secret.metadata.name,
                                                    "key_name": key,
                                                    "type": secret.type or "Opaque",
                                                },
                                                "certificate_data": cert_info,
                                                "validity": {
                                                    "not_before": cert_info[
                                                        "not_before"
                                                    ],
                                                    "not_after": cert_info["not_after"],
                                                    "days_remaining": cert_info[
                                                        "days_remaining"
                                                    ],
                                                    "status": categorize_certificate_status(
                                                        cert_info["days_remaining"],
                                                        warning_threshold_days,
                                                        critical_threshold_days,
                                                    ),
                                                },
                                                "usage": {
                                                    "is_ca": cert_info.get(
                                                        "is_ca", False
                                                    )
                                                    or "ca" in key.lower(),
                                                    "is_client": "client"
                                                    in key.lower(),
                                                    "is_server": "server" in key.lower()
                                                    or "tls" in key.lower(),
                                                    "san_domains": cert_info.get(
                                                        "san", []
                                                    ),
                                                },
                                                "chain_validation": {
                                                    "is_self_signed": cert_info.get(
                                                        "subject_cn"
                                                    )
                                                    == cert_info.get("issuer_cn"),
                                                    "issuer": cert_info.get(
                                                        "issuer_cn", "Unknown"
                                                    ),
                                                    "chain_length": (
                                                        len(cert_blocks)
                                                        if len(cert_blocks) > 1
                                                        else 1
                                                    ),
                                                },
                                            }

                                            certificates_found.append(cert_details)

                                            # Track CA certificates
                                            if cert_details["usage"]["is_ca"]:
                                                ca_name = cert_info.get(
                                                    "subject_cn", "Unknown CA"
                                                )
                                                if ca_name not in ca_certificates:
                                                    ca_certificates[ca_name] = {
                                                        "ca_name": ca_name,
                                                        "issued_certificates": 0,
                                                        "ca_expiry": cert_info[
                                                            "not_after"
                                                        ],
                                                        "trust_status": (
                                                            "trusted"
                                                            if not cert_details[
                                                                "chain_validation"
                                                            ]["is_self_signed"]
                                                            else "self-signed"
                                                        ),
                                                    }
                                                ca_certificates[ca_name][
                                                    "issued_certificates"
                                                ] += 1

                            except Exception as e:
                                logger.debug(
                                    f"Could not parse certificate {key} in secret {secret.metadata.name}: {e}"
                                )
                                continue

            except ApiException as e:
                if e.status == 403:
                    logger.debug(f"Access denied to namespace {namespace}: {e.reason}")
                    if namespace not in skipped_namespaces_rbac:
                        skipped_namespaces_rbac.append(namespace)
                else:
                    logger.warning(f"Error scanning namespace {namespace}: {e.reason}")
                continue

        # Process OpenShift system certificates if requested
        # Always scan system cert namespaces when include_system_certs=True,
        # even when specific namespaces were provided (they may have been RBAC-blocked)
        if include_system_certs:
            try:
                # Try to get OpenShift cluster certificates
                system_cert_namespaces = [
                    "openshift-config",
                    "openshift-ingress",
                    "openshift-ingress-operator",
                    "openshift-kube-apiserver",
                    "openshift-etcd",
                ]

                for sys_ns in system_cert_namespaces:
                    if sys_ns not in scanned_namespaces:
                        try:
                            secrets = await asyncio.to_thread(
                                k8s_core_api.list_namespaced_secret, sys_ns
                            )
                            scanned_namespaces.append(sys_ns)
                            for secret in secrets.items:
                                if secret.data:
                                    for key in ["tls.crt", "ca.crt"]:
                                        if key in secret.data:
                                            try:
                                                # Properly parse the certificate
                                                cert_data = base64.b64decode(
                                                    secret.data[key]
                                                ).decode("utf-8")
                                                if (
                                                    "-----BEGIN CERTIFICATE-----"
                                                    in cert_data
                                                ):
                                                    cert_info = parse_certificate(
                                                        cert_data
                                                    )
                                                    if cert_info:
                                                        status = categorize_certificate_status(
                                                            cert_info["days_remaining"],
                                                            warning_threshold_days,
                                                            critical_threshold_days,
                                                        )
                                                        result[
                                                            "system_certificates"
                                                        ].append(
                                                            {
                                                                "component": sys_ns.replace(
                                                                    "openshift-", ""
                                                                ),
                                                                "certificate_purpose": secret.metadata.name,
                                                                "subject_cn": cert_info.get(
                                                                    "subject_cn",
                                                                    "Unknown",
                                                                ),
                                                                "expiry_date": cert_info.get(
                                                                    "not_after",
                                                                    "Unknown",
                                                                ),
                                                                "days_remaining": cert_info.get(
                                                                    "days_remaining", 0
                                                                ),
                                                                "status": status,
                                                                "auto_renewal": True,
                                                                "renewal_mechanism": "OpenShift Certificate Operator",
                                                            }
                                                        )
                                            except Exception as parse_err:
                                                logger.debug(
                                                    f"Could not parse system cert {secret.metadata.name}/{key}: {parse_err}"
                                                )
                        except ApiException as e:
                            if e.status == 403:
                                if sys_ns not in skipped_namespaces_rbac:
                                    skipped_namespaces_rbac.append(sys_ns)
                            continue

            except Exception as e:
                logger.debug(f"Could not scan system certificates: {e}")

        # Update scan summary
        total_certs = len(certificates_found)
        healthy_count = len(
            [c for c in certificates_found if c["validity"]["status"] == "healthy"]
        )
        warning_count = len(
            [c for c in certificates_found if c["validity"]["status"] == "warning"]
        )
        critical_count = len(
            [c for c in certificates_found if c["validity"]["status"] == "critical"]
        )
        expired_count = len(
            [c for c in certificates_found if c["validity"]["status"] == "expired"]
        )

        result["scan_summary"].update(
            {
                "total_certificates": total_certs,
                "healthy_certificates": healthy_count,
                "warning_certificates": warning_count,
                "critical_certificates": critical_count,
                "expired_certificates": expired_count,
                "namespaces_scanned": len(scanned_namespaces),
                "namespaces_skipped_rbac": len(skipped_namespaces_rbac),
                "namespaces_total": len(target_namespaces),
            }
        )

        # Update scan coverage
        result["scan_coverage"] = {
            "scanned_namespaces": scanned_namespaces,
            "skipped_namespaces_rbac": skipped_namespaces_rbac[
                :50
            ],  # Limit to first 50 to avoid huge output
        }

        # Add RBAC warning if many namespaces were skipped
        if len(skipped_namespaces_rbac) > len(scanned_namespaces):
            result["security_findings"].append(
                {
                    "type": "rbac_limitation",
                    "severity": "info",
                    "message": f"RBAC restrictions prevented scanning {len(skipped_namespaces_rbac)} namespaces. "
                    f"Only {len(scanned_namespaces)} namespaces were accessible. "
                    "Consider granting 'list secrets' permission for comprehensive certificate scanning.",
                }
            )

        # Filter certificates by type if specified
        if certificate_types and "all" not in certificate_types:
            filtered_certs = []
            for cert in certificates_found:
                cert_usage = cert["usage"]
                if (
                    ("tls" in certificate_types and cert_usage["is_server"])
                    or ("ca" in certificate_types and cert_usage["is_ca"])
                    or ("client" in certificate_types and cert_usage["is_client"])
                    or ("server" in certificate_types and cert_usage["is_server"])
                ):
                    filtered_certs.append(cert)
            certificates_found = filtered_certs

        result["certificate_details"] = certificates_found

        # Generate expiration timeline
        timeline_dict = defaultdict(list)
        for cert in certificates_found:
            if (
                cert["validity"]["days_remaining"] >= 0
            ):  # Don't include expired certs in timeline
                expiry_date = cert["certificate_data"]["not_after"][
                    :10
                ]  # Just the date part
                timeline_dict[expiry_date].append(
                    {
                        "name": cert["certificate_info"]["name"],
                        "namespace": cert["certificate_info"]["namespace"],
                        "days_remaining": cert["validity"]["days_remaining"],
                        "status": cert["validity"]["status"],
                    }
                )

        # Sort timeline by date
        sorted_timeline = []
        for date in sorted(timeline_dict.keys()):
            sorted_timeline.append(
                {"date": date, "certificates_expiring": timeline_dict[date]}
            )

        result["expiration_timeline"] = sorted_timeline[
            :30
        ]  # Limit to next 30 expiration dates

        # Generate renewal recommendations
        for cert in certificates_found:
            if cert["validity"]["status"] in ["critical", "warning", "expired"]:
                urgency = (
                    "immediate"
                    if cert["validity"]["status"] in ["critical", "expired"]
                    else "soon"
                )

                recommendation = {
                    "certificate": cert["certificate_info"]["name"],
                    "namespace": cert["certificate_info"]["namespace"],
                    "urgency": urgency,
                    "renewal_method": "manual",
                    "steps": [
                        f"Generate new certificate for {cert['certificate_data'].get('subject_cn', 'unknown subject')}",
                        f"Update secret {cert['certificate_info']['secret_name']} in namespace {cert['certificate_info']['namespace']}",
                        "Restart affected pods/services",
                    ],
                    "automation_available": cert["certificate_info"][
                        "namespace"
                    ].startswith("openshift-"),
                }

                if cert["certificate_info"]["namespace"].startswith("openshift-"):
                    recommendation["renewal_method"] = "OpenShift Certificate Operator"
                    recommendation["steps"] = [
                        "Certificate should auto-renew via OpenShift Certificate Operator",
                        "If not auto-renewing, check cluster operator status",
                        "Manual intervention may be required",
                    ]

                result["renewal_recommendations"].append(recommendation)

        # Generate security findings
        for cert in certificates_found:
            cert_data = cert["certificate_data"]

            # Check for weak algorithms
            if "sha1" in cert_data.get("signature_algorithm", "").lower():
                result["security_findings"].append(
                    {
                        "certificate": cert["certificate_info"]["name"],
                        "finding_type": "weak_algorithm",
                        "description": "Certificate uses weak SHA-1 signature algorithm",
                        "severity": "medium",
                        "recommendation": "Replace with SHA-256 or stronger algorithm",
                    }
                )

            # Check for self-signed certificates
            if (
                cert["chain_validation"]["is_self_signed"]
                and not cert["usage"]["is_ca"]
            ):
                result["security_findings"].append(
                    {
                        "certificate": cert["certificate_info"]["name"],
                        "finding_type": "self_signed",
                        "description": "Self-signed certificate detected",
                        "severity": "low",
                        "recommendation": "Consider using CA-signed certificate for production",
                    }
                )

            # Check for short validity periods
            if (
                cert["validity"]["days_remaining"] < critical_threshold_days
                and cert["validity"]["status"] != "expired"
            ):
                result["security_findings"].append(
                    {
                        "certificate": cert["certificate_info"]["name"],
                        "finding_type": "short_validity",
                        "description": f"Certificate expires in {cert['validity']['days_remaining']} days",
                        "severity": "high",
                        "recommendation": "Renew certificate immediately",
                    }
                )

        # Add CA information
        result["certificate_authorities"] = list(ca_certificates.values())

        logger.info(
            f"Certificate health scan completed: {total_certs} certificates found, {critical_count + expired_count} require immediate attention"
        )
        return result

    except Exception as e:
        logger.error(f"Error during certificate health check: {str(e)}", exc_info=True)
        return {
            "scan_summary": {
                "total_certificates": 0,
                "healthy_certificates": 0,
                "warning_certificates": 0,
                "critical_certificates": 0,
                "expired_certificates": 0,
                "scan_timestamp": datetime.utcnow().isoformat(),
                "error": str(e),
            },
            "certificate_details": [],
            "system_certificates": [],
            "expiration_timeline": [],
            "renewal_recommendations": [],
            "security_findings": [],
            "certificate_authorities": [],
        }


@mcp.tool()
@log_tool_execution
async def ci_cd_performance_baselining_tool(
    pipeline_names: Optional[List[str]] = None,
    baseline_period: str = "30d",
    deviation_threshold: float = 2.0,
    include_task_level: bool = True,
) -> Dict[str, Any]:
    """
    Establish performance baselines for pipelines and flag runs deviating from historical norms.

    Uses Prometheus metrics from Tekton controller for accurate historical performance data.

    Args:
        pipeline_names: Pipelines to analyze (default: all).
        baseline_period: "7d", "30d" (default), or "90d".
        deviation_threshold: Std deviations to trigger alerts (default: 2.0).
        include_task_level: Include task-level analysis (default: True).

    Returns:
        Dict: Baselines, recent runs analysis, trends, and optimization opportunities.
    """
    return await ci_cd_performance_baselining_tool_impl(
        pipeline_names=pipeline_names,
        baseline_period=baseline_period,
        deviation_threshold=deviation_threshold,
        include_task_level=include_task_level,
        k8s_custom_api=k8s_custom_api,
        k8s_core_api=k8s_core_api,
    )


@mcp.tool()
async def pipeline_tracer(
    trace_identifier: str,
    trace_type: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    include_artifacts: bool = True,
    trace_depth: str = "deep",
    namespaces: Optional[List[str]] = None,
    max_namespaces: int = 50,
) -> Dict[str, Any]:
    """
    Trace a logical operation (commit, PR, image) as it flows through pipelines.

    Correlates pipeline runs using labels, annotations, and artifact references.

    Args:
        trace_identifier: Commit SHA, PR number, image tag, or custom trace ID.
        trace_type: "commit", "pr", "image", or "custom".
        start_time: ISO 8601 start timestamp.
        end_time: ISO 8601 end timestamp.
        include_artifacts: Include artifact details (default: True).
        trace_depth: "shallow" or "deep" (default: "deep").
        namespaces: Specific namespaces to search (skips auto-detection).
        max_namespaces: Maximum namespaces to search when auto-detecting (default: 50).

    Returns:
        Dict: Pipeline flow, artifacts, bottlenecks, and summary.
    """
    if not k8s_core_api or not k8s_custom_api or not k8s_apps_api:
        return {"error": "Kubernetes client not available."}
    try:
        logger.info(f"Starting pipeline trace for {trace_type}: {trace_identifier}")

        # Validate inputs
        valid_trace_types = ["commit", "pr", "image", "custom"]
        if trace_type not in valid_trace_types:
            return {
                "error": f"Invalid trace_type '{trace_type}'. Must be one of: {', '.join(valid_trace_types)}"
            }

        valid_depths = ["shallow", "deep"]
        if trace_depth not in valid_depths:
            return {
                "error": f"Invalid trace_depth '{trace_depth}'. Must be one of: {', '.join(valid_depths)}"
            }

        # Get multi-cluster clients
        cluster_clients = await get_multi_cluster_clients(
            k8s_core_api, k8s_custom_api, k8s_apps_api
        )

        if not cluster_clients:
            return {"error": "No cluster clients available for tracing"}

        # Detect tekton-active namespaces for prioritization (if not user-specified)
        tekton_ns_list = None
        if not namespaces:
            try:
                tekton_ns = await detect_tekton_namespaces()
                tekton_ns_list = []
                for category in tekton_ns.values():
                    tekton_ns_list.extend(category)
                tekton_ns_list = list(set(tekton_ns_list))
                logger.info(
                    f"Detected {len(tekton_ns_list)} tekton-active namespaces for prioritization"
                )
            except Exception as e:
                logger.debug(f"Failed to detect tekton namespaces: {e}")

        # Correlate pipeline events across clusters (parallelized)
        pipeline_flow = await correlate_pipeline_events(
            trace_identifier=trace_identifier,
            trace_type=trace_type,
            cluster_clients=cluster_clients,
            start_time=start_time,
            end_time=end_time,
            namespaces=namespaces,
            max_namespaces=max_namespaces,
            tekton_namespaces=tekton_ns_list,
            logger=logger,
        )

        # Track artifacts if requested
        artifacts = await track_artifacts(pipeline_flow, include_artifacts, logger)

        # Analyze for bottlenecks
        bottlenecks = analyze_bottlenecks(pipeline_flow, logger)

        # Calculate summary metrics
        summary = {
            "total_duration": 0,
            "clusters_traversed": len(set(p["cluster"] for p in pipeline_flow)),
            "pipelines_executed": len(pipeline_flow),
        }

        # Calculate total duration if we have start and end times
        if pipeline_flow:
            first_start = pipeline_flow[0].get("start_time")
            last_completion = None

            for pipeline in reversed(pipeline_flow):
                if pipeline.get("completion_time"):
                    last_completion = pipeline["completion_time"]
                    break

            if first_start and last_completion:
                try:
                    start_dt = datetime.fromisoformat(
                        first_start.replace("Z", "+00:00")
                    )
                    end_dt = datetime.fromisoformat(
                        last_completion.replace("Z", "+00:00")
                    )
                    summary["total_duration"] = (end_dt - start_dt).total_seconds()
                except Exception as e:
                    logger.debug(f"Failed to calculate total duration: {e}")

        # Follow lifecycle chain (snapshots → tests → releases → release pipelines)
        lifecycle = {}
        if pipeline_flow:
            try:
                lifecycle = await follow_lifecycle_chain(
                    pipeline_flow=pipeline_flow,
                    custom_api=k8s_custom_api,
                    core_api=k8s_core_api,
                    trace_depth=trace_depth,
                    logger=logger,
                )
                logger.info(
                    f"Lifecycle chain: {len(lifecycle.get('snapshots', []))} snapshots, "
                    f"{len(lifecycle.get('integration_tests', []))} tests, "
                    f"{len(lifecycle.get('releases', []))} releases, "
                    f"{len(lifecycle.get('release_pipelines', []))} release PLRs, "
                    f"{len(lifecycle.get('nudge_cascade', []))} nudge cascades"
                )
            except Exception as e:
                logger.warning(f"Failed to follow lifecycle chain: {e}")
                lifecycle = {"error": str(e)[:200]}

        # Determine overall status (include lifecycle in assessment)
        if not pipeline_flow:
            overall_status = "not_found"
        else:
            # Check build PLRs
            builds_ok = all(
                p["status"] in ["Succeeded", "Completed"] for p in pipeline_flow
            )
            builds_failed = any(
                p["status"] in ["Failed", "Error"] for p in pipeline_flow
            )

            # Check release status from lifecycle
            releases = lifecycle.get("releases", [])
            release_failed = any(r.get("status") == "Failed" for r in releases)
            release_succeeded = (
                all(r.get("status") == "Succeeded" for r in releases)
                if releases
                else True
            )

            if builds_failed:
                overall_status = "failed"
            elif release_failed:
                overall_status = "release_failed"
            elif builds_ok and release_succeeded:
                overall_status = "succeeded"
            else:
                overall_status = "in_progress"

        # Build stage-level summary
        stage_summary = {}
        if pipeline_flow:
            build_durations = [
                p.get("completion_time")
                for p in pipeline_flow
                if p.get("completion_time")
            ]
            stage_summary["build"] = {
                "count": len(pipeline_flow),
                "status": (
                    "succeeded"
                    if all(
                        p["status"] in ["Succeeded", "Completed"] for p in pipeline_flow
                    )
                    else "failed"
                ),
            }
        if lifecycle.get("integration_tests"):
            tests = lifecycle["integration_tests"]
            stage_summary["integration_tests"] = {
                "count": len(tests),
                "passed": sum(1 for t in tests if t.get("status") == "TestPassed"),
                "failed": sum(1 for t in tests if t.get("status") == "TestFail"),
            }
        if lifecycle.get("releases"):
            rels = lifecycle["releases"]
            stage_summary["releases"] = {
                "count": len(rels),
                "succeeded": sum(1 for r in rels if r.get("status") == "Succeeded"),
                "failed": sum(1 for r in rels if r.get("status") == "Failed"),
            }
        if lifecycle.get("nudge_cascade"):
            stage_summary["nudge_cascade"] = {
                "count": len(lifecycle["nudge_cascade"]),
            }
        summary["stages"] = stage_summary

        result = {
            "trace_id": f"{trace_type}:{trace_identifier}",
            "trace_type": trace_type,
            "start_time": start_time
            or (pipeline_flow[0].get("start_time") if pipeline_flow else None),
            "end_time": end_time
            or (pipeline_flow[-1].get("completion_time") if pipeline_flow else None),
            "overall_status": overall_status,
            "pipeline_flow": pipeline_flow,
            "lifecycle": lifecycle,
            "artifacts": artifacts,
            "bottlenecks": bottlenecks,
            "summary": summary,
        }

        logger.info(
            f"Trace completed: found {len(pipeline_flow)} pipelines across {summary['clusters_traversed']} clusters"
        )

        return result

    except Exception as e:
        logger.error(f"Error in pipeline_tracer: {str(e)}", exc_info=True)
        return {
            "error": f"Failed to trace pipeline: {str(e)}",
            "trace_id": f"{trace_type}:{trace_identifier}",
            "trace_type": trace_type,
            "overall_status": "error",
            "pipeline_flow": [],
            "artifacts": [],
            "bottlenecks": [],
            "summary": {
                "total_duration": 0,
                "clusters_traversed": 0,
                "pipelines_executed": 0,
            },
        }


@mcp.tool()
async def get_machine_config_pool_status(
    pool_names: Optional[List[str]] = None,
    include_node_details: bool = True,
    include_update_history: bool = True,
    filter_updating: bool = False,
) -> Dict[str, Any]:
    """
    Monitor OpenShift Machine Config Pools for node configuration and update rollouts.

    Analyzes pool status, update progress, and configuration drift.

    Args:
        pool_names: Pools to monitor (default: all).
        include_node_details: Include node status per pool (default: True).
        include_update_history: Include update history (default: True).
        filter_updating: Only show updating pools (default: False).

    Returns:
        Dict: Keys: pools_overview, machine_config_pools, recent_config_changes, issues,
              update_recommendations.
    """
    if not k8s_custom_api or not k8s_core_api:
        return {"error": "Kubernetes client not available."}
    logger.info("Starting machine config pool status analysis")

    try:
        # Query MachineConfigPool resources using Kubernetes Custom Resource API
        logger.info(
            "Querying MachineConfigPool resources from OpenShift Machine Config Operator"
        )

        pools_response = await asyncio.to_thread(
            k8s_custom_api.list_cluster_custom_object,
            group="machineconfiguration.openshift.io",
            version="v1",
            plural="machineconfigpools",
        )

        all_pools = pools_response.get("items", [])
        logger.info(f"Found {len(all_pools)} machine config pools in cluster")

        # Filter pools if specific names requested
        if pool_names:
            filtered_pools = []
            for pool in all_pools:
                pool_name = pool.get("metadata", {}).get("name", "")
                if pool_name in pool_names:
                    filtered_pools.append(pool)
            pools_to_analyze = filtered_pools
            logger.info(
                f"Filtered to {len(pools_to_analyze)} requested pools: {pool_names}"
            )
        else:
            pools_to_analyze = all_pools

        # Analyze each pool
        analyzed_pools = []
        for pool in pools_to_analyze:
            pool_analysis = analyze_machine_config_pool_status(pool)
            analyzed_pools.append(pool_analysis)

        # Filter for updating pools if requested
        if filter_updating:
            analyzed_pools = [
                pool
                for pool in analyzed_pools
                if pool.get("update_progress", {}).get("is_updating", False)
            ]
            logger.info(f"Filtered to {len(analyzed_pools)} pools currently updating")

        # Generate pools overview
        total_pools = len(analyzed_pools)
        healthy_pools = len(
            [pool for pool in analyzed_pools if pool.get("status") == "ready"]
        )
        updating_pools = len(
            [
                pool
                for pool in analyzed_pools
                if pool.get("update_progress", {}).get("is_updating", False)
            ]
        )
        degraded_pools = len(
            [pool for pool in analyzed_pools if pool.get("status") == "degraded"]
        )

        pools_overview = {
            "total_pools": total_pools,
            "healthy_pools": healthy_pools,
            "updating_pools": updating_pools,
            "degraded_pools": degraded_pools,
        }

        # Get recent machine config changes if requested
        recent_config_changes = []
        if include_update_history:
            try:
                logger.info("Querying recent MachineConfig changes")
                machine_configs_response = await asyncio.to_thread(
                    k8s_custom_api.list_cluster_custom_object,
                    group="machineconfiguration.openshift.io",
                    version="v1",
                    plural="machineconfigs",
                )

                machine_configs = machine_configs_response.get("items", [])

                # Sort by creation time and get recent ones
                sorted_configs = sorted(
                    machine_configs,
                    key=lambda x: x.get("metadata", {}).get("creationTimestamp", ""),
                    reverse=True,
                )[
                    :10
                ]  # Get last 10 configs

                for config in sorted_configs:
                    metadata = config.get("metadata", {})
                    recent_config_changes.append(
                        {
                            "config_name": metadata.get("name", "unknown"),
                            "created_time": metadata.get(
                                "creationTimestamp", "unknown"
                            ),
                            "changes": [
                                "Configuration details would require detailed diff analysis"
                            ],
                            "affected_pools": metadata.get("labels", {}).get(
                                "machineconfiguration.openshift.io/role", "unknown"
                            ),
                        }
                    )

            except Exception as e:
                logger.warning(f"Could not retrieve machine config history: {e}")
                recent_config_changes = []

        # Detect issues across all pools
        all_issues = []
        for pool in analyzed_pools:
            pool_issues = detect_pool_issues(pool)
            all_issues.extend(pool_issues)

        # Generate recommendations
        update_recommendations = generate_update_recommendations(analyzed_pools)

        # Add node details if requested and include_node_details is True
        if include_node_details:
            logger.info("Adding detailed node status to pool analysis")
            for pool in analyzed_pools:
                try:
                    # Query nodes that belong to this pool based on node selector
                    pool_config = pool.get("configuration", {})
                    node_selector = pool_config.get("node_selector", {})

                    # Get all nodes and filter by labels
                    nodes = await asyncio.to_thread(k8s_core_api.list_node)
                    matching_nodes = []

                    for node in nodes.items:
                        node_labels = node.metadata.labels or {}
                        # Check if node matches the pool's node selector
                        matches = True
                        for key, value in node_selector.items():
                            if node_labels.get(key) != value:
                                matches = False
                                break

                        if matches:
                            node_status = {
                                "name": node.metadata.name,
                                "ready": False,
                                "machine_config": "unknown",
                                "last_update": "unknown",
                            }

                            # Check node readiness
                            for condition in node.status.conditions or []:
                                if condition.type == "Ready":
                                    node_status["ready"] = condition.status == "True"
                                    break

                            # Extract machine config info from annotations
                            annotations = node.metadata.annotations or {}
                            node_status["machine_config"] = annotations.get(
                                "machineconfiguration.openshift.io/currentConfig",
                                "unknown",
                            )
                            node_status["last_update"] = annotations.get(
                                "machineconfiguration.openshift.io/lastAppliedDrift",
                                "unknown",
                            )

                            matching_nodes.append(node_status)

                    pool["node_status"] = matching_nodes

                except Exception as e:
                    logger.warning(
                        f"Could not retrieve node details for pool {pool.get('name')}: {e}"
                    )
                    pool["node_status"] = []

        result = {
            "pools_overview": pools_overview,
            "machine_config_pools": analyzed_pools,
            "recent_config_changes": recent_config_changes,
            "issues": all_issues,
            "update_recommendations": update_recommendations,
        }

        logger.info(
            f"Machine config pool analysis complete: {total_pools} pools analyzed, "
            f"{len(all_issues)} issues found, {len(update_recommendations)} recommendations generated"
        )

        return result

    except ApiException as e:
        error_msg = f"Kubernetes API error while querying machine config pools: {e.status} - {e.reason}"
        logger.error(error_msg)
        return {
            "pools_overview": {
                "total_pools": 0,
                "healthy_pools": 0,
                "updating_pools": 0,
                "degraded_pools": 0,
            },
            "machine_config_pools": [],
            "recent_config_changes": [],
            "issues": [
                {
                    "pool": "api_error",
                    "issue_type": "api_access",
                    "description": error_msg,
                    "affected_nodes": [],
                    "severity": "high",
                    "remediation": "Check RBAC permissions for machineconfiguration.openshift.io resources",
                }
            ],
            "update_recommendations": [],
        }

    except Exception as e:
        error_msg = f"Unexpected error during machine config pool analysis: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "pools_overview": {
                "total_pools": 0,
                "healthy_pools": 0,
                "updating_pools": 0,
                "degraded_pools": 0,
            },
            "machine_config_pools": [],
            "recent_config_changes": [],
            "issues": [
                {
                    "pool": "system_error",
                    "issue_type": "analysis_failure",
                    "description": error_msg,
                    "affected_nodes": [],
                    "severity": "high",
                    "remediation": "Check system logs and OpenShift Machine Config Operator status",
                }
            ],
            "update_recommendations": [],
        }


async def _get_fallback_cluster_health() -> Dict[str, Any]:
    """
    Fallback cluster health analysis using standard Kubernetes resources.
    Used when OpenShift-specific cluster operators are not accessible.

    Note: This returns component health checks (namespaces, nodes) NOT actual
    ClusterOperator resources. The output is structured similarly for compatibility
    but clearly marked as fallback_mode=True.
    """
    logger.info(
        "Performing fallback cluster health analysis using standard Kubernetes resources"
    )

    cluster_info = {}
    component_health = []  # Not operators - these are namespace/node health checks
    critical_issues = []

    try:
        # Get basic cluster information
        try:
            from kubernetes.client import VersionApi

            api_server_url = k8s_core_api.api_client.configuration.host

            # Use the proper VersionApi to get cluster version
            try:
                version_api = VersionApi(k8s_core_api.api_client)
                version_info = await asyncio.to_thread(version_api.get_code)
                cluster_info = {
                    "cluster_version": version_info.git_version or "unknown",
                    "platform": version_info.platform or "unknown",
                    "api_server": api_server_url,
                    "build_date": version_info.build_date or "unknown",
                    "go_version": version_info.go_version or "unknown",
                }
            except Exception as version_error:
                logger.warning(f"Could not get version via VersionApi: {version_error}")
                cluster_info = {
                    "cluster_version": "unknown",
                    "platform": "unknown",
                    "api_server": api_server_url,
                    "build_date": "unknown",
                }
        except Exception as e:
            logger.warning(f"Could not retrieve basic cluster info: {e}")
            cluster_info = {"error": "Could not access cluster information"}

        # Analyze core system components using standard Kubernetes resources
        try:
            # Check system namespaces and their health
            system_namespaces = ["kube-system", "kube-public", "default"]

            for ns_name in system_namespaces:
                try:
                    # Get pods in system namespace
                    pods = await asyncio.to_thread(
                        k8s_core_api.list_namespaced_pod, namespace=ns_name
                    )

                    total_pods = len(pods.items)
                    running_pods = 0
                    failed_pods = 0

                    for pod in pods.items:
                        if pod.status.phase == "Running":
                            running_pods += 1
                        elif pod.status.phase in ["Failed", "CrashLoopBackOff"]:
                            failed_pods += 1

                    if total_pods == 0:
                        # No pods visible — likely managed cluster with restricted access
                        health_ratio = 1.0
                        status = "available"
                    else:
                        health_ratio = running_pods / total_pods
                        status = "available"
                        if health_ratio < 0.8:
                            status = "degraded"
                        elif health_ratio < 0.9:
                            status = "warning"

                    component_health.append(
                        {
                            "name": f"system-namespace:{ns_name}",
                            "type": "namespace_health",  # Clearly indicates this is NOT an operator
                            "namespace": ns_name,
                            "status": status,
                            "available": health_ratio >= 0.8,
                            "degraded": health_ratio < 0.8,
                            "progressing": False,
                            "version": "n/a",
                            "conditions_analysis": {
                                "total_pods": total_pods,
                                "running_pods": running_pods,
                                "failed_pods": failed_pods,
                                "health_ratio": round(health_ratio, 2),
                            },
                        }
                    )

                    if failed_pods > 0:
                        critical_issues.append(
                            {
                                "component": f"system-namespace:{ns_name}",
                                "severity": (
                                    "warning" if failed_pods < 3 else "critical"
                                ),
                                "issue": f"{failed_pods} failed pods in {ns_name} namespace",
                                "impact": f"Potential service disruption in {ns_name}",
                                "recommended_action": f"Check pod logs in {ns_name} namespace",
                            }
                        )

                except Exception as e:
                    logger.warning(f"Could not analyze namespace {ns_name}: {e}")
                    component_health.append(
                        {
                            "name": f"system-namespace:{ns_name}",
                            "type": "namespace_health",
                            "namespace": ns_name,
                            "status": "unknown",
                            "available": False,
                            "degraded": False,
                            "progressing": False,
                            "version": "n/a",
                            "conditions_analysis": {"error": str(e)},
                        }
                    )

        except Exception as e:
            logger.warning(f"Could not analyze system namespaces: {e}")
            critical_issues.append(
                {
                    "component": "namespace-analysis",
                    "severity": "warning",
                    "issue": f"Could not analyze system namespaces: {str(e)}",
                    "impact": "Limited visibility into system component health",
                    "recommended_action": "Check RBAC permissions for pod listing",
                }
            )

        # Check node health
        try:
            nodes = await asyncio.to_thread(k8s_core_api.list_node)
            total_nodes = len(nodes.items)
            ready_nodes = 0

            for node in nodes.items:
                if node.status.conditions:
                    ready_condition = next(
                        (c for c in node.status.conditions if c.type == "Ready"), None
                    )
                    if ready_condition and ready_condition.status == "True":
                        ready_nodes += 1

            node_health_ratio = ready_nodes / total_nodes if total_nodes > 0 else 0
            node_status = "available" if node_health_ratio >= 0.8 else "degraded"

            component_health.append(
                {
                    "name": "cluster-nodes",
                    "type": "node_health",  # Clearly indicates this is NOT an operator
                    "namespace": "cluster-scoped",
                    "status": node_status,
                    "available": node_health_ratio >= 0.8,
                    "degraded": node_health_ratio < 0.8,
                    "progressing": False,
                    "version": "n/a",
                    "conditions_analysis": {
                        "total_nodes": total_nodes,
                        "ready_nodes": ready_nodes,
                        "health_ratio": round(node_health_ratio, 2),
                    },
                }
            )

            if ready_nodes < total_nodes:
                critical_issues.append(
                    {
                        "component": "cluster-nodes",
                        "severity": (
                            "critical" if node_health_ratio < 0.5 else "warning"
                        ),
                        "issue": f"{total_nodes - ready_nodes} of {total_nodes} nodes not ready",
                        "impact": "Reduced cluster capacity and potential service disruption",
                        "recommended_action": "Check node status and system resources",
                    }
                )

        except Exception as e:
            logger.warning(f"Could not analyze node health: {e}")
            critical_issues.append(
                {
                    "component": "cluster-nodes",
                    "severity": "warning",
                    "issue": f"Could not analyze node health: {str(e)}",
                    "impact": "No visibility into node status",
                    "recommended_action": "Check RBAC permissions for node listing",
                }
            )

        # Calculate health summary (for components, not operators)
        total_components = len(component_health)
        healthy_components = len(
            [c for c in component_health if c.get("status") == "available"]
        )
        degraded_components = len(
            [c for c in component_health if c.get("degraded", False)]
        )

        overall_health = "healthy"
        if degraded_components > 0:
            overall_health = "degraded"
        elif healthy_components < total_components:
            overall_health = "warning"

        health_summary = {
            "fallback_mode": True,  # Clearly indicates this is NOT OpenShift operator data
            "total_components": total_components,
            "healthy_components": healthy_components,
            "degraded_components": degraded_components,
            # Keep operator fields for backwards compatibility but set to 0
            "total_operators": 0,
            "healthy_operators": 0,
            "degraded_operators": 0,
            "overall_health": overall_health,
            "note": "ClusterOperator access denied. Showing system component health instead.",
        }

        return {
            "fallback_mode": True,
            "cluster_info": cluster_info,
            "operator_status": [],  # Empty - we don't have operator access
            "component_health": component_health,  # New field with actual health data
            "health_summary": health_summary,
            "critical_issues": critical_issues,
            "dependencies": None,
        }

    except Exception as e:
        logger.error(f"Fallback cluster health analysis failed: {e}")
        return {
            "fallback_mode": True,
            "cluster_info": {"error": "Fallback analysis failed"},
            "operator_status": [],
            "component_health": [],
            "health_summary": {
                "fallback_mode": True,
                "total_components": 0,
                "healthy_components": 0,
                "degraded_components": 0,
                "total_operators": 0,
                "healthy_operators": 0,
                "degraded_operators": 0,
                "overall_health": "unknown",
                "note": "Fallback analysis failed",
            },
            "critical_issues": [
                {
                    "component": "fallback-analysis",
                    "severity": "critical",
                    "issue": f"Fallback cluster health analysis failed: {str(e)}",
                    "impact": "No cluster health information available",
                    "recommended_action": "Check cluster connectivity and basic RBAC permissions",
                }
            ],
            "dependencies": None,
        }


@mcp.tool()
async def get_openshift_cluster_operator_status(
    operator_names: Optional[List[str]] = None,
    include_conditions: bool = True,
    show_version_info: bool = True,
    filter_degraded: bool = False,
    include_dependencies: bool = False,
) -> Dict[str, Any]:
    """
    Check health and status of OpenShift cluster operators for platform functionality.

    Analyzes operator conditions, versions, and dependencies.

    Args:
        operator_names: Operators to check (default: all).
        include_conditions: Include condition details (default: True).
        show_version_info: Include version info (default: True).
        filter_degraded: Only show operators with issues (default: False).
        include_dependencies: Show operator dependencies (default: False).

    Returns:
        Dict: Keys: cluster_info, operator_status, health_summary, critical_issues, dependencies.
    """
    if not k8s_custom_api or not k8s_core_api:
        return {"error": "Kubernetes client not available."}
    logger.info("Starting OpenShift cluster operator status analysis")

    try:
        # Query ClusterOperator resources from OpenShift Config API
        logger.info("Querying ClusterOperator resources from OpenShift Config API")

        operators_response = await asyncio.to_thread(
            k8s_custom_api.list_cluster_custom_object,
            group="config.openshift.io",
            version="v1",
            plural="clusteroperators",
        )

        all_operators = operators_response.get("items", [])
        logger.info(f"Found {len(all_operators)} cluster operators")

        # Filter operators if specific names requested
        if operator_names:
            filtered_operators = []
            for operator in all_operators:
                op_name = operator.get("metadata", {}).get("name", "")
                if op_name in operator_names:
                    filtered_operators.append(operator)
            operators_to_analyze = filtered_operators
            logger.info(
                f"Filtered to {len(operators_to_analyze)} requested operators: {operator_names}"
            )
        else:
            operators_to_analyze = all_operators

        # Get cluster version information
        cluster_info = {}
        try:
            cluster_version_response = await asyncio.to_thread(
                k8s_custom_api.list_cluster_custom_object,
                group="config.openshift.io",
                version="v1",
                plural="clusterversions",
            )
            cluster_versions = cluster_version_response.get("items", [])
            if cluster_versions:
                cv = cluster_versions[0]  # There's typically only one
                cv_status = cv.get("status", {})
                cluster_info = {
                    "cluster_version": cv_status.get("desired", {}).get(
                        "version", "unknown"
                    ),
                    "cluster_id": cv.get("spec", {}).get("clusterID", "unknown"),
                    "infrastructure_status": cv_status.get("infrastructure", {}).get(
                        "status", "unknown"
                    ),
                    "update_available": len(cv_status.get("availableUpdates", [])) > 0,
                    "current_update": (
                        cv_status.get("history", [{}])[0]
                        if cv_status.get("history")
                        else {}
                    ),
                }
        except Exception as e:
            logger.warning(f"Could not retrieve cluster version info: {e}")
            cluster_info = {
                "cluster_version": "unknown",
                "cluster_id": "unknown",
                "infrastructure_status": "unknown",
                "update_available": False,
                "current_update": {},
            }

        # Analyze each operator
        analyzed_operators = []
        for operator in operators_to_analyze:
            metadata = operator.get("metadata", {})
            status = operator.get("status", {})

            operator_analysis = {
                "name": metadata.get("name", "unknown"),
                "namespace": metadata.get("namespace", "cluster-scoped"),
                "status": "unknown",
                "available": False,
                "progressing": False,
                "degraded": False,
            }

            # Analyze conditions - always parse for health assessment, only include raw in output if requested
            conditions = status.get("conditions", [])
            conditions_analysis = analyze_operator_conditions(conditions)
            operator_analysis["available"] = conditions_analysis["available"]
            operator_analysis["progressing"] = conditions_analysis["progressing"]
            operator_analysis["degraded"] = conditions_analysis["degraded"]
            if include_conditions:
                operator_analysis["conditions_analysis"] = conditions_analysis
                operator_analysis["conditions"] = conditions

            # Calculate overall status
            if operator_analysis["degraded"]:
                operator_analysis["status"] = "degraded"
            elif not operator_analysis["available"]:
                operator_analysis["status"] = "unavailable"
            elif operator_analysis["progressing"]:
                operator_analysis["status"] = "progressing"
            else:
                operator_analysis["status"] = "available"

            # Add version information
            if show_version_info:
                versions = status.get("versions", [])
                if versions:
                    # Find operator version (usually the first one or one named 'operator')
                    operator_version = "unknown"
                    for version in versions:
                        if version.get("name") == "operator" or len(versions) == 1:
                            operator_version = version.get("version", "unknown")
                            break
                    operator_analysis["version"] = operator_version
                else:
                    operator_analysis["version"] = "unknown"

            # Add related objects info
            operator_analysis["related_objects"] = status.get("relatedObjects", [])

            analyzed_operators.append(operator_analysis)

        # Calculate health summary from ALL operators before filtering
        total_operators = len(analyzed_operators)
        healthy_operators = len(
            [op for op in analyzed_operators if op.get("status") == "available"]
        )
        degraded_operators = len(
            [op for op in analyzed_operators if op.get("degraded", False)]
        )

        # Filter degraded operators if requested (after counting)
        if filter_degraded:
            analyzed_operators = [
                op
                for op in analyzed_operators
                if op.get("degraded", False) or op.get("status") != "available"
            ]
            logger.info(f"Filtered to {len(analyzed_operators)} operators with issues")

        overall_health = "healthy"
        if degraded_operators > 0:
            overall_health = "degraded"
        elif healthy_operators < total_operators:
            overall_health = "warning"

        health_summary = {
            "total_operators": total_operators,
            "healthy_operators": healthy_operators,
            "degraded_operators": degraded_operators,
            "overall_health": overall_health,
        }

        # Identify critical issues
        critical_issues = identify_critical_issues(analyzed_operators)

        # Build response
        response = {
            "cluster_info": cluster_info,
            "operator_status": analyzed_operators,
            "health_summary": health_summary,
            "critical_issues": critical_issues,
        }

        # Add dependencies if requested
        if include_dependencies:
            dependencies = analyze_operator_dependencies(analyzed_operators)
            response["dependencies"] = dependencies

        logger.info(
            f"Cluster operator analysis complete. Health: {overall_health}, Issues: {len(critical_issues)}"
        )
        return response

    except ApiException as e:
        error_msg = f"API error accessing cluster operators: {e.status} - {e.reason}"
        logger.error(error_msg)

        if e.status == 403:
            error_msg += ". Check RBAC permissions for config.openshift.io resources"
            logger.info(
                "Attempting fallback analysis using standard Kubernetes resources..."
            )

            # Fallback: Use standard Kubernetes resources to provide alternative health info
            try:
                fallback_result = await _get_fallback_cluster_health()
                fallback_result["critical_issues"].insert(
                    0,
                    {
                        "component": "openshift-api-access",
                        "severity": "warning",
                        "issue": "Limited permissions for OpenShift cluster operators. Using fallback analysis.",
                        "impact": "Reduced visibility into OpenShift-specific operator status",
                        "recommended_action": "Grant access to config.openshift.io resources for full OpenShift monitoring",
                    },
                )
                return fallback_result
            except Exception as fallback_error:
                logger.error(f"Fallback analysis also failed: {fallback_error}")

        elif e.status == 404:
            error_msg += (
                ". ClusterOperator resource not found - may not be an OpenShift cluster"
            )
            logger.info("Attempting fallback analysis for non-OpenShift cluster...")

            # Fallback for non-OpenShift clusters
            try:
                fallback_result = await _get_fallback_cluster_health()
                fallback_result["critical_issues"].insert(
                    0,
                    {
                        "component": "cluster-type-detection",
                        "severity": "info",
                        "issue": "Not an OpenShift cluster - using standard Kubernetes health analysis",
                        "impact": "OpenShift-specific operator monitoring not available",
                        "recommended_action": "Use standard Kubernetes monitoring tools for this cluster type",
                    },
                )
                return fallback_result
            except Exception as fallback_error:
                logger.error(f"Fallback analysis failed: {fallback_error}")

        return {
            "cluster_info": {},
            "operator_status": [],
            "health_summary": {
                "total_operators": 0,
                "healthy_operators": 0,
                "degraded_operators": 0,
                "overall_health": "unknown",
            },
            "critical_issues": [
                {
                    "component": "api-access",
                    "severity": "critical",
                    "issue": error_msg,
                    "impact": "Cannot assess cluster operator status",
                    "recommended_action": "Check cluster access and RBAC permissions",
                }
            ],
            "dependencies": [] if include_dependencies else None,
        }

    except Exception as e:
        error_msg = f"Unexpected error analyzing cluster operators: {str(e)}"
        logger.error(error_msg, exc_info=True)

        return {
            "cluster_info": {},
            "operator_status": [],
            "health_summary": {
                "total_operators": 0,
                "healthy_operators": 0,
                "degraded_operators": 0,
                "overall_health": "unknown",
            },
            "critical_issues": [
                {
                    "component": "system-error",
                    "severity": "critical",
                    "issue": error_msg,
                    "impact": "Cannot assess cluster operator status",
                    "recommended_action": "Check system logs and cluster connectivity",
                }
            ],
            "dependencies": [] if include_dependencies else None,
        }


async def _process_namespace_topology(
    namespace: str,
    cluster_name: str,
    component_types: List[str],
    core_api,
    apps_api,
    custom_api,
    include_metrics: bool,
    skip_on_permission_denied: bool,
    logger,
) -> Dict[str, Any]:
    """Process a single namespace and return its topology data."""
    nodes = []
    edges = []
    permissions = {"accessible": [], "denied": [], "errors": []}
    stats = {"nodes": 0, "edges": 0}

    # Pre-fetch pods once to avoid N+1 queries in analyze_service_dependencies
    pods_list = None
    if "pods" in component_types or "services" in component_types:
        try:
            pods_result = await asyncio.to_thread(
                core_api.list_namespaced_pod, namespace=namespace
            )
            pods_list = pods_result.items
        except Exception as e:
            logger.debug(f"Could not pre-fetch pods for {namespace}: {e}")

    try:
        # Process Deployments
        if "deployments" in component_types:
            try:
                deployments = await asyncio.to_thread(
                    apps_api.list_namespaced_deployment, namespace=namespace
                )
                permissions["accessible"].append(
                    f"{cluster_name}/{namespace}/deployments"
                )

                for deployment in deployments.items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "deployment", deployment.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "deployment",
                        "name": deployment.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": (
                            deployment.status.conditions[-1].type
                            if deployment.status.conditions
                            else "Unknown"
                        ),
                        "metadata": {
                            "replicas": deployment.spec.replicas or 0,
                            "ready_replicas": deployment.status.ready_replicas or 0,
                            "labels": deployment.metadata.labels or {},
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name,
                            "deployment",
                            namespace,
                            deployment.metadata.name,
                            logger,
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze dependencies
                    deployment_dict = deployment.to_dict()
                    owner_edges = await analyze_owner_references(
                        deployment_dict, cluster_name, "deployment"
                    )
                    volume_edges = await analyze_volume_dependencies(
                        deployment_dict, cluster_name, "deployment", logger
                    )
                    edges.extend(owner_edges + volume_edges)
                    stats["edges"] += len(owner_edges + volume_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "deployments", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(
                        f"{cluster_name}/{namespace}/deployments"
                    )
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/deployments",
                            "error": error_info["error_message"],
                        }
                    )

        # Process ReplicaSets (needed for complete Deployment→ReplicaSet→Pod ownership chain)
        if "replicasets" in component_types:
            try:
                replicasets = await asyncio.to_thread(
                    apps_api.list_namespaced_replica_set, namespace=namespace
                )
                permissions["accessible"].append(
                    f"{cluster_name}/{namespace}/replicasets"
                )

                for replicaset in replicasets.items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "replicaset", replicaset.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "replicaset",
                        "name": replicaset.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": (
                            "Active"
                            if (replicaset.status.ready_replicas or 0) > 0
                            else "Inactive"
                        ),
                        "metadata": {
                            "replicas": replicaset.spec.replicas or 0,
                            "ready_replicas": replicaset.status.ready_replicas or 0,
                            "labels": replicaset.metadata.labels or {},
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name,
                            "replicaset",
                            namespace,
                            replicaset.metadata.name,
                            logger,
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze dependencies (ReplicaSet→Deployment ownership)
                    replicaset_dict = replicaset.to_dict()
                    owner_edges = await analyze_owner_references(
                        replicaset_dict, cluster_name, "replicaset"
                    )
                    edges.extend(owner_edges)
                    stats["edges"] += len(owner_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "replicasets", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(
                        f"{cluster_name}/{namespace}/replicasets"
                    )
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/replicasets",
                            "error": error_info["error_message"],
                        }
                    )

        # Process Services
        if "services" in component_types:
            try:
                services = await asyncio.to_thread(
                    core_api.list_namespaced_service, namespace=namespace
                )
                permissions["accessible"].append(f"{cluster_name}/{namespace}/services")

                for service in services.items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "service", service.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "service",
                        "name": service.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "type": service.spec.type,
                            "cluster_ip": service.spec.cluster_ip,
                            "ports": [
                                {"port": p.port, "target_port": p.target_port}
                                for p in (service.spec.ports or [])
                            ],
                            "selector": service.spec.selector or {},
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name,
                            "service",
                            namespace,
                            service.metadata.name,
                            logger,
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze service dependencies (pass pre-fetched pods to avoid N+1 queries)
                    service_dict = service.to_dict()
                    service_edges = await analyze_service_dependencies(
                        service_dict,
                        cluster_name,
                        core_api,
                        logger,
                        pods_list=pods_list,
                    )
                    edges.extend(service_edges)
                    stats["edges"] += len(service_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "services", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/services")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/services",
                            "error": error_info["error_message"],
                        }
                    )

        # Process Pods (use pre-fetched pods_list if available)
        if "pods" in component_types:
            try:
                # Use pre-fetched pods if available, otherwise fetch
                if pods_list is not None:
                    pods_items = pods_list
                else:
                    pods_result = await asyncio.to_thread(
                        core_api.list_namespaced_pod, namespace=namespace
                    )
                    pods_items = pods_result.items
                for pod in pods_items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "pod", pod.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "pod",
                        "name": pod.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": pod.status.phase or "Unknown",
                        "metadata": {
                            "node_name": pod.spec.node_name,
                            "labels": pod.metadata.labels or {},
                            "containers": len(pod.spec.containers or []),
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name, "pod", namespace, pod.metadata.name, logger
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze pod dependencies
                    pod_dict = pod.to_dict()
                    owner_edges = await analyze_owner_references(
                        pod_dict, cluster_name, "pod"
                    )
                    volume_edges = await analyze_volume_dependencies(
                        pod_dict, cluster_name, "pod", logger
                    )
                    edges.extend(owner_edges + volume_edges)
                    stats["edges"] += len(owner_edges + volume_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "pods", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/pods")
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/pods",
                            "error": error_info["error_message"],
                        }
                    )

        # Process PVCs
        if "persistentvolumeclaims" in component_types:
            try:
                pvcs = await asyncio.to_thread(
                    core_api.list_namespaced_persistent_volume_claim,
                    namespace=namespace,
                )
                for pvc in pvcs.items:
                    node_id = generate_node_id(
                        cluster_name,
                        namespace,
                        "persistentvolumeclaim",
                        pvc.metadata.name,
                    )

                    node = {
                        "id": node_id,
                        "type": "persistentvolumeclaim",
                        "name": pvc.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": pvc.status.phase or "Unknown",
                        "metadata": {
                            "capacity": (
                                pvc.status.capacity.get("storage")
                                if pvc.status.capacity
                                else None
                            ),
                            "access_modes": pvc.spec.access_modes or [],
                            "storage_class": pvc.spec.storage_class_name,
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name,
                            "persistentvolumeclaim",
                            namespace,
                            pvc.metadata.name,
                            logger,
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e,
                    "persistentvolumeclaims",
                    namespace,
                    skip_on_permission_denied,
                    logger,
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(
                        f"{cluster_name}/{namespace}/persistentvolumeclaims"
                    )
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/persistentvolumeclaims",
                            "error": error_info["error_message"],
                        }
                    )

        # Process ConfigMaps
        if "configmaps" in component_types:
            try:
                configmaps = await asyncio.to_thread(
                    core_api.list_namespaced_config_map, namespace=namespace
                )
                permissions["accessible"].append(
                    f"{cluster_name}/{namespace}/configmaps"
                )

                for cm in configmaps.items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "configmap", cm.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "configmap",
                        "name": cm.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "data_keys": list(cm.data.keys()) if cm.data else []
                        },
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "configmaps", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(
                        f"{cluster_name}/{namespace}/configmaps"
                    )
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/configmaps",
                            "error": error_info["error_message"],
                        }
                    )

        # Process Secrets (NOT included in defaults due to common RBAC restrictions)
        if "secrets" in component_types:
            try:
                secrets = await asyncio.to_thread(
                    core_api.list_namespaced_secret, namespace=namespace
                )
                permissions["accessible"].append(f"{cluster_name}/{namespace}/secrets")

                for secret in secrets.items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "secret", secret.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "secret",
                        "name": secret.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "type": secret.type,
                            "data_keys": (
                                list(secret.data.keys()) if secret.data else []
                            ),
                        },
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "secrets", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/secrets")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/secrets",
                            "error": error_info["error_message"],
                        }
                    )

        # Process Tekton PipelineRuns
        if "pipelineruns" in component_types:
            try:
                pipeline_runs = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="pipelineruns",
                    limit=200,
                )

                for pr in pipeline_runs.get("items", []):
                    node_id = generate_node_id(
                        cluster_name,
                        namespace,
                        "pipelinerun",
                        pr.get("metadata", {}).get("name", ""),
                    )

                    node = {
                        "id": node_id,
                        "type": "pipelinerun",
                        "name": pr.get("metadata", {}).get("name", ""),
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": pr.get("status", {})
                        .get("conditions", [{}])[-1]
                        .get("type", "Unknown"),
                        "metadata": {
                            "pipeline_ref": pr.get("spec", {})
                            .get("pipelineRef", {})
                            .get("name", ""),
                            "labels": pr.get("metadata", {}).get("labels", {}),
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name, "pipelinerun", namespace, node["name"], logger
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Create edge to pipeline if referenced
                    pipeline_ref = pr.get("spec", {}).get("pipelineRef", {}).get("name")
                    if pipeline_ref:
                        pipeline_id = generate_node_id(
                            cluster_name, namespace, "pipeline", pipeline_ref
                        )
                        edges.append(
                            {
                                "source": node_id,
                                "target": pipeline_id,
                                "relationship": "runs",
                                "weight": calculate_dependency_weight(
                                    "pipelinerun", "pipeline", "runs"
                                ),
                            }
                        )
                        stats["edges"] += 1

            except Exception as e:
                logger.debug(f"Could not fetch PipelineRuns in {namespace}: {e}")

        # Process Tekton Pipelines
        if "pipelines" in component_types:
            try:
                pipelines = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="pipelines",
                )

                for pipeline in pipelines.get("items", []):
                    node_id = generate_node_id(
                        cluster_name,
                        namespace,
                        "pipeline",
                        pipeline.get("metadata", {}).get("name", ""),
                    )

                    node = {
                        "id": node_id,
                        "type": "pipeline",
                        "name": pipeline.get("metadata", {}).get("name", ""),
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "tasks": len(pipeline.get("spec", {}).get("tasks", [])),
                            "labels": pipeline.get("metadata", {}).get("labels", {}),
                        },
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                logger.debug(f"Could not fetch Pipelines in {namespace}: {e}")

        # Process Tekton TaskRuns
        if "taskruns" in component_types:
            try:
                task_runs = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="taskruns",
                    limit=500,
                )
                permissions["accessible"].append(f"{cluster_name}/{namespace}/taskruns")

                for tr in task_runs.get("items", []):
                    tr_name = tr.get("metadata", {}).get("name", "")
                    node_id = generate_node_id(
                        cluster_name, namespace, "taskrun", tr_name
                    )

                    # Get status from conditions
                    conditions = tr.get("status", {}).get("conditions", [])
                    status = (
                        conditions[-1].get("reason", "Unknown")
                        if conditions
                        else "Unknown"
                    )

                    node = {
                        "id": node_id,
                        "type": "taskrun",
                        "name": tr_name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": status,
                        "metadata": {
                            "task_ref": tr.get("spec", {})
                            .get("taskRef", {})
                            .get("name", ""),
                            "pipeline_run": tr.get("metadata", {})
                            .get("labels", {})
                            .get("tekton.dev/pipelineRun", ""),
                            "labels": tr.get("metadata", {}).get("labels", {}),
                            "start_time": tr.get("status", {}).get("startTime"),
                        },
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Create edge to PipelineRun if part of one
                    pipeline_run_name = (
                        tr.get("metadata", {})
                        .get("labels", {})
                        .get("tekton.dev/pipelineRun")
                    )
                    if pipeline_run_name:
                        pr_id = generate_node_id(
                            cluster_name, namespace, "pipelinerun", pipeline_run_name
                        )
                        edges.append(
                            {
                                "source": pr_id,
                                "target": node_id,
                                "relationship": "runs_task",
                                "weight": 0.85,
                            }
                        )
                        stats["edges"] += 1

                    # Create edge to Task if referenced
                    task_ref = tr.get("spec", {}).get("taskRef", {}).get("name")
                    if task_ref:
                        task_id = generate_node_id(
                            cluster_name, namespace, "task", task_ref
                        )
                        edges.append(
                            {
                                "source": node_id,
                                "target": task_id,
                                "relationship": "uses",
                                "weight": calculate_dependency_weight(
                                    "taskrun", "task", "uses"
                                ),
                            }
                        )
                        stats["edges"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "taskruns", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/taskruns")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/taskruns",
                            "error": error_info["error_message"],
                        }
                    )

        # Process Tekton Tasks
        if "tasks" in component_types:
            try:
                tasks = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="tasks",
                )
                permissions["accessible"].append(f"{cluster_name}/{namespace}/tasks")

                for task in tasks.get("items", []):
                    task_name = task.get("metadata", {}).get("name", "")
                    node_id = generate_node_id(
                        cluster_name, namespace, "task", task_name
                    )

                    node = {
                        "id": node_id,
                        "type": "task",
                        "name": task_name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "steps": len(task.get("spec", {}).get("steps", [])),
                            "labels": task.get("metadata", {}).get("labels", {}),
                        },
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "tasks", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/tasks")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/tasks",
                            "error": error_info["error_message"],
                        }
                    )

    except Exception as e:
        logger.warning(
            f"Error processing namespace {namespace} in cluster {cluster_name}: {e}"
        )

    return {"nodes": nodes, "edges": edges, "permissions": permissions, "stats": stats}


@mcp.tool()
async def live_system_topology_mapper(
    cluster_names: Optional[List[str]] = None,
    component_types: Optional[List[str]] = None,
    namespace_filter: Optional[str] = None,
    depth_limit: Optional[int] = 5,
    include_metrics: Optional[bool] = False,
    output_format: Optional[str] = "json",
    skip_on_permission_denied: Optional[bool] = True,
) -> Dict[str, Any]:
    """
    Generate real-time dependency graph of Kubernetes/Tekton components and their interconnections.

    Maps Services, Deployments, Pipelines, PVCs, and their relationships via ownerReferences and selectors.

    Args:
        cluster_names: Clusters to map (default: all).
        component_types: Filter by types (services, deployments, pipelines, pvcs, etc.). Note: secrets are NOT included by default.
        namespace_filter: Regex pattern to filter namespaces.
        depth_limit: Max dependency depth (default: 5).
        include_metrics: Include resource metrics (default: False).
        output_format: "json" (default), "graphviz", or "mermaid".
        skip_on_permission_denied: Continue mapping other resources if permission denied (default: True).

    Returns:
        Dict: Topology graph with nodes, edges, summary, metadata, and permission report.
    """
    if (
        not k8s_core_api
        or not k8s_custom_api
        or not k8s_apps_api
        or not k8s_storage_api
        or not k8s_batch_api
    ):
        return {"error": "Kubernetes client not available."}
    try:
        logger.info(
            f"Starting live system topology mapping with filters: clusters={cluster_names}, "
            f"types={component_types}, namespace_filter={namespace_filter}"
        )

        start_time = time.time()

        # Get multi-cluster clients
        cluster_clients = await get_multi_cluster_topology_clients(
            k8s_core_api, k8s_custom_api, k8s_apps_api, k8s_storage_api, k8s_batch_api
        )

        if not cluster_clients:
            return {
                "topology": {"nodes": [], "edges": []},
                "summary": {
                    "total_nodes": 0,
                    "total_relationships": 0,
                    "clusters_mapped": 0,
                    "potential_blast_radius": {},
                },
                "error": "No cluster clients available for topology mapping",
                "last_updated": datetime.now().isoformat(),
            }

        # Filter clusters if specified
        if cluster_names:
            cluster_clients = {
                k: v for k, v in cluster_clients.items() if k in cluster_names
            }

        # Default component types if not specified
        # Note: secrets are NOT included by default due to common RBAC restrictions
        # ReplicaSets are included to show complete Deployment→ReplicaSet→Pod ownership chain
        if not component_types:
            component_types = [
                "deployments",
                "replicasets",
                "services",
                "pods",
                "persistentvolumeclaims",
                "configmaps",
                "pipelineruns",
                "pipelines",
                "taskruns",
                "tasks",
            ]

        nodes = []
        edges = []
        cluster_stats = {}

        # Track permission issues
        permissions_report = {"accessible": [], "denied": [], "errors": []}

        for cluster_name, clients in cluster_clients.items():
            logger.info(f"Mapping topology for cluster: {cluster_name}")
            cluster_stats[cluster_name] = {"nodes": 0, "edges": 0}

            try:
                core_api = clients["core_api"]
                apps_api = clients["apps_api"]
                custom_api = clients["custom_api"]
                clients["storage_api"]

                # Get all namespaces
                all_namespaces = []
                try:
                    ns_list = await asyncio.to_thread(core_api.list_namespace)
                    all_namespaces = [ns.metadata.name for ns in ns_list.items]

                    # Apply namespace filter if specified
                    if namespace_filter:
                        pattern = re.compile(namespace_filter)
                        all_namespaces = [
                            ns for ns in all_namespaces if pattern.search(ns)
                        ]

                except Exception as e:
                    logger.warning(
                        f"Failed to list namespaces in cluster {cluster_name}: {e}"
                    )
                    continue

                logger.info(
                    f"Processing {len(all_namespaces)} namespaces in cluster {cluster_name} in parallel"
                )

                # Process all namespaces in parallel using asyncio.gather
                namespace_tasks = [
                    _process_namespace_topology(
                        namespace=ns,
                        cluster_name=cluster_name,
                        component_types=component_types,
                        core_api=core_api,
                        apps_api=apps_api,
                        custom_api=custom_api,
                        include_metrics=include_metrics,
                        skip_on_permission_denied=skip_on_permission_denied,
                        logger=logger,
                    )
                    for ns in all_namespaces
                ]

                namespace_results = await asyncio.gather(
                    *namespace_tasks, return_exceptions=True
                )

                # Aggregate results from all namespaces
                for i, result in enumerate(namespace_results):
                    if isinstance(result, Exception):
                        logger.warning(
                            f"Error processing namespace {all_namespaces[i]} in cluster {cluster_name}: {result}"
                        )
                        continue

                    nodes.extend(result["nodes"])
                    edges.extend(result["edges"])
                    cluster_stats[cluster_name]["nodes"] += result["stats"]["nodes"]
                    cluster_stats[cluster_name]["edges"] += result["stats"]["edges"]
                    permissions_report["accessible"].extend(
                        result["permissions"]["accessible"]
                    )
                    permissions_report["denied"].extend(result["permissions"]["denied"])
                    permissions_report["errors"].extend(result["permissions"]["errors"])

            except Exception as e:
                logger.error(f"Error processing cluster {cluster_name}: {e}")
                continue

        # Calculate summary statistics
        total_nodes = len(nodes)
        total_edges = len(edges)
        clusters_mapped = len([c for c in cluster_stats.values() if c["nodes"] > 0])

        # Calculate potential blast radius using depth_limit
        blast_radius = {}
        if total_nodes > 0:
            # Create NetworkX graph for analysis
            G = nx.DiGraph()
            for node in nodes:
                G.add_node(node["id"], **node)
            for edge in edges:
                G.add_edge(edge["source"], edge["target"], **edge)

            # Calculate metrics using depth_limit for traversal analysis
            if G.nodes():
                # Find nodes reachable within depth_limit from each node
                max_reachable = 0
                critical_nodes_list = []

                for node_id in G.nodes():
                    # Use BFS with depth limit to find reachable nodes
                    reachable = set()
                    queue = [(node_id, 0)]
                    visited = {node_id}

                    while queue:
                        current, current_depth = queue.pop(0)
                        if current_depth >= depth_limit:
                            continue
                        for neighbor in G.neighbors(current):
                            if neighbor not in visited:
                                visited.add(neighbor)
                                reachable.add(neighbor)
                                queue.append((neighbor, current_depth + 1))

                    if len(reachable) > max_reachable:
                        max_reachable = len(reachable)

                    # Mark as critical if can affect many nodes within depth_limit
                    if len(reachable) > 5:
                        critical_nodes_list.append(
                            {"node_id": node_id, "affected_count": len(reachable)}
                        )

                blast_radius = {
                    "depth_limit_used": depth_limit,
                    "most_connected_components": len(
                        list(nx.connected_components(G.to_undirected()))
                    ),
                    "average_degree": (
                        sum(dict(G.degree()).values()) / len(G.nodes())
                        if G.nodes()
                        else 0
                    ),
                    "critical_nodes": len(critical_nodes_list),
                    "max_blast_radius": max_reachable,
                    "critical_nodes_details": critical_nodes_list[
                        :10
                    ],  # Top 10 critical nodes
                }

        execution_time = time.time() - start_time

        # Deduplicate permission report entries
        permissions_report["accessible"] = list(set(permissions_report["accessible"]))
        permissions_report["denied"] = list(set(permissions_report["denied"]))

        result = {
            "topology": {"nodes": nodes, "edges": edges},
            "summary": {
                "total_nodes": total_nodes,
                "total_relationships": total_edges,
                "clusters_mapped": clusters_mapped,
                "potential_blast_radius": blast_radius,
                "cluster_stats": cluster_stats,
                "execution_time_seconds": round(execution_time, 2),
            },
            "permissions": permissions_report,
            "last_updated": datetime.now().isoformat(),
        }

        # Log permissions summary
        if permissions_report["denied"]:
            logger.warning(
                f"Permission denied for {len(permissions_report['denied'])} resource types"
            )
        if permissions_report["errors"]:
            logger.warning(
                f"Errors encountered for {len(permissions_report['errors'])} resource types"
            )

        logger.info(
            f"Topology mapping completed: {total_nodes} nodes, {total_edges} edges across {clusters_mapped} clusters in {execution_time:.2f}s"
        )

        # Handle different output formats
        if output_format == "graphviz":
            result["graphviz"] = convert_to_graphviz(nodes, edges)
        elif output_format == "mermaid":
            result["mermaid"] = convert_to_mermaid(nodes, edges)

        return result

    except Exception as e:
        logger.error(
            f"Unexpected error during topology mapping: {str(e)}", exc_info=True
        )
        return {
            "topology": {"nodes": [], "edges": []},
            "summary": {
                "total_nodes": 0,
                "total_relationships": 0,
                "clusters_mapped": 0,
                "potential_blast_radius": {},
            },
            "error": f"Failed to generate topology: {str(e)}",
            "last_updated": datetime.now().isoformat(),
        }


@mcp.tool()
@log_tool_execution
async def predictive_log_analyzer(
    prediction_window: str = "6h",
    confidence_threshold: float = 0.75,
    log_sources: Optional[List[str]] = None,
    namespaces: Optional[List[str]] = None,
    max_namespaces: int = 20,
    force_retrain: bool = False,
) -> Dict[str, Any]:
    """
    Predict failures using ML analysis of historical log patterns before critical outages occur.

    Uses anomaly detection algorithms to correlate log patterns with failure events.
    Supports persistent model storage for faster subsequent calls.

    Args:
        prediction_window: Time window - "1h", "6h", "24h", "7d" (default: "6h").
        confidence_threshold: Min confidence for predictions 0.0-1.0 (default: 0.75).
        log_sources: Sources to analyze - pods, services, nodes (default: all).
        namespaces: Specific namespaces to analyze (default: auto-detect active namespaces).
        max_namespaces: Maximum namespaces to scan when auto-detecting (default: 20).
        force_retrain: Force model retraining even if cached model is valid (default: False).

    Returns:
        Dict: Keys: predictions, model_performance, anomaly_scores, trend_analysis, model_info.
    """
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}
    try:
        logger.info(
            f"Starting predictive log analysis with window: {prediction_window}, threshold: {confidence_threshold}"
        )

        # Validate parameters
        valid_windows = ["1h", "6h", "24h", "7d"]
        if prediction_window not in valid_windows:
            raise ValueError(
                f"Invalid prediction_window. Must be one of: {valid_windows}"
            )

        if not 0.0 <= confidence_threshold <= 1.0:
            raise ValueError("confidence_threshold must be between 0.0 and 1.0")

        # Initialize persistence components (lazy loading)
        try:
            from helpers.ml_persistence import (
                FailureEventCollector,
                ModelPersistenceManager,
                ModelVersionManager,
                TrainingDataStore,
                build_labels_from_correlations,
            )

            model_manager = ModelPersistenceManager()
            training_store = TrainingDataStore()
            failure_collector = FailureEventCollector(training_store)
            version_manager = ModelVersionManager(model_manager, training_store)
            persistence_available = True
        except Exception as e:
            logger.warning(
                f"ML persistence not available, using ephemeral training: {e}"
            )
            persistence_available = False
            model_manager = None
            training_store = None
            failure_collector = None
            version_manager = None

        # Initialize result structure
        result = {
            "predictions": [],
            "model_performance": {
                "accuracy": 0.0,
                "precision": 0.0,
                "recall": 0.0,
                "last_training_time": datetime.now().isoformat(),
            },
            "anomaly_scores": [],
            "trend_analysis": {
                "error_rate_trend": "stable",
                "resource_trend": "stable",
                "performance_trend": "stable",
            },
            "model_info": {
                "model_id": None,
                "loaded_from_cache": False,
                "training_samples": 0,
                "has_failure_labels": False,
                "persistence_enabled": persistence_available,
            },
        }

        # Get recent logs from various sources
        log_sources = log_sources or ["pods", "services", "nodes"]
        all_logs = []

        for source in log_sources:
            try:
                if source == "pods":
                    # Determine target namespaces
                    if namespaces:
                        # Use user-provided namespaces
                        target_namespaces = namespaces
                        logger.info(
                            f"Using user-specified namespaces: {target_namespaces}"
                        )
                    else:
                        # Auto-detect active namespaces, prioritizing those with tekton/pipeline activity
                        all_ns = await list_namespaces()
                        try:
                            tekton_ns = await detect_tekton_namespaces()
                            active_ns = []
                            for category in tekton_ns.values():
                                active_ns.extend(category)
                            # Deduplicate and limit
                            target_namespaces = (
                                list(set(active_ns))[:max_namespaces]
                                if active_ns
                                else all_ns[:max_namespaces]
                            )
                        except Exception:
                            # Fallback to alphabetical if tekton detection fails
                            target_namespaces = all_ns[:max_namespaces]
                        logger.info(
                            f"Auto-detected {len(target_namespaces)} active namespaces"
                        )

                    for ns in target_namespaces:
                        try:
                            pods = await asyncio.to_thread(
                                k8s_core_api.list_namespaced_pod, namespace=ns, limit=50
                            )
                            for pod in pods.items:
                                # Include Running pods for proactive analysis, plus Failed/Succeeded for historical
                                if pod.status.phase in [
                                    "Running",
                                    "Failed",
                                    "Succeeded",
                                ]:
                                    try:
                                        pod_logs = await asyncio.to_thread(
                                            k8s_core_api.read_namespaced_pod_log,
                                            name=pod.metadata.name,
                                            namespace=ns,
                                            tail_lines=100,
                                        )
                                        all_logs.extend(pod_logs.split("\n"))
                                    except ApiException:
                                        continue  # Skip pods without accessible logs
                        except ApiException:
                            continue  # Skip inaccessible namespaces

            except Exception as e:
                logger.warning(f"Failed to collect logs from {source}: {str(e)}")
                continue

        # Return early if no logs collected - never fabricate analysis from fake data
        if not all_logs:
            logger.warning(
                "No logs collected from any source - returning insufficient data"
            )
            result["trend_analysis"]["error_rate_trend"] = "no_data"
            result["model_performance"] = {
                "accuracy": None,
                "precision": None,
                "recall": None,
                "note": "No log data available for analysis",
            }
            return result

        # Filter out empty lines
        all_logs = [log for log in all_logs if log.strip()]

        if len(all_logs) < 10:
            logger.warning(f"Insufficient log data for analysis: {len(all_logs)} lines")
            result["trend_analysis"]["error_rate_trend"] = "insufficient_data"
            return result

        logger.info(f"Analyzing {len(all_logs)} log lines for predictive patterns")

        # Preprocess log data
        log_df = preprocess_log_data(all_logs)

        # Extract features for ML analysis
        features = extract_log_features(log_df)

        # Collect failure events and correlate with logs if persistence is available
        labels = None
        if persistence_available and failure_collector and training_store:
            try:
                # Collect failure events from the target namespaces
                for ns in target_namespaces:
                    try:
                        # Collect from Kubernetes events - use dict format for FailureEventCollector
                        events_as_dicts = await _get_namespace_events_as_dicts(
                            ns, limit=100
                        )
                        if events_as_dicts:
                            count = failure_collector.collect_from_events(
                                events_as_dicts, ns
                            )
                            logger.debug(
                                f"Collected {count} failure labels from events in {ns}"
                            )

                        # Collect from pod statuses
                        pods = await asyncio.to_thread(
                            k8s_core_api.list_namespaced_pod, namespace=ns, limit=50
                        )
                        failure_collector.collect_from_pod_status(pods.items, ns)
                    except Exception as e:
                        logger.debug(f"Failed to collect failure events from {ns}: {e}")

                # Get recent failure labels for correlation
                from datetime import timedelta

                historical_failures = training_store.get_failure_labels_in_window(
                    start_time=datetime.now() - timedelta(hours=2),
                    end_time=datetime.now(),
                )

                # Store log samples first so they get database IDs for correlation
                stored_samples = []
                for idx, row in log_df.iterrows():
                    if idx < 500:  # Limit to avoid excessive storage
                        sample_data = {
                            "timestamp": row.get("timestamp"),
                            "namespace": (
                                target_namespaces[0] if target_namespaces else "unknown"
                            ),
                            "features": (
                                features[idx].tolist() if idx < len(features) else []
                            ),
                            "raw_message": str(row.get("raw_message", ""))[:500],
                            "log_level": row.get("log_level"),
                            "error_indicators": int(row.get("error_indicators", 0)),
                            "message_entropy": float(row.get("message_entropy", 0.0)),
                        }
                        sample_id = training_store.store_log_sample(sample_data)
                        if sample_id:
                            stored_samples.append(
                                {
                                    "id": sample_id,
                                    "timestamp": sample_data["timestamp"],
                                    "namespace": sample_data["namespace"],
                                }
                            )

                # Correlate stored log samples with failures using database IDs
                if historical_failures and stored_samples:
                    correlations = failure_collector.correlate_logs_with_failures(
                        stored_samples, historical_failures, time_window_minutes=30
                    )
                    if correlations:
                        labels = build_labels_from_correlations(
                            correlations, len(log_df)
                        )
                        logger.info(
                            f"Created {len(correlations)} log-failure correlations"
                        )

            except Exception as e:
                logger.warning(f"Failed to collect/correlate failure events: {e}")

        # Train or load model with persistence
        if persistence_available and model_manager and version_manager:
            try:
                anomaly_model, model_id, model_metadata = train_or_load_model(
                    features=features,
                    model_manager=model_manager,
                    version_manager=version_manager,
                    labels=labels,
                    force_retrain=force_retrain,
                )

                # Update result with model info
                result["model_info"].update(
                    {
                        "model_id": model_id,
                        "loaded_from_cache": model_metadata.get(
                            "loaded_from_cache", False
                        ),
                        "training_samples": model_metadata.get(
                            "training_samples", len(features)
                        ),
                        "has_failure_labels": labels is not None and len(labels) > 0,
                        "created_at": model_metadata.get("created_at"),
                    }
                )

                # Use performance metrics from model if available
                perf = model_metadata.get("performance_metrics", {})
                if perf:
                    result["model_performance"].update(
                        {
                            "accuracy": perf.get("accuracy", 0.0),
                            "precision": perf.get("precision", 0.0),
                            "recall": perf.get("recall", 0.0),
                            "last_training_time": model_metadata.get(
                                "created_at", datetime.now().isoformat()
                            ),
                        }
                    )
            except Exception as e:
                logger.warning(
                    f"Persistence-based training failed, falling back to ephemeral: {e}"
                )
                anomaly_model = train_anomaly_model(features)
        else:
            # Fallback to ephemeral training
            anomaly_model = train_anomaly_model(features)

        anomaly_scores = anomaly_model.decision_function(features)
        anomaly_predictions = anomaly_model.predict(features)

        # Update model performance if not already set by persistence
        # Note: without labeled validation data, precision/recall cannot be computed
        if result["model_performance"]["accuracy"] == 0.0:
            normal_predictions = anomaly_predictions == 1
            accuracy = (
                np.mean(normal_predictions) if len(normal_predictions) > 0 else 0.0
            )
            result["model_performance"].update(
                {
                    "accuracy": float(accuracy),
                    "precision": None,
                    "recall": None,
                    "note": "Precision/recall require labeled validation data - not available",
                }
            )

        # Generate aggregate anomaly scores per namespace
        # anomaly_scores are per-log-line, so aggregate by namespace using mean score
        if target_namespaces:
            # Calculate per-namespace aggregated scores from per-line scores
            lines_per_ns = max(1, len(anomaly_scores) // max(1, len(target_namespaces)))
            threshold = -0.5  # Typical anomaly threshold for Isolation Forest

            for i, ns in enumerate(
                target_namespaces[: min(10, len(target_namespaces))]
            ):
                start_idx = i * lines_per_ns
                end_idx = min(start_idx + lines_per_ns, len(anomaly_scores))
                if start_idx < len(anomaly_scores):
                    ns_scores = anomaly_scores[start_idx:end_idx]
                    mean_score = float(np.mean(ns_scores))
                    anomalous_lines = int(np.sum(ns_scores < threshold))
                    status = "anomalous" if mean_score < threshold else "normal"

                    result["anomaly_scores"].append(
                        {
                            "component": ns,
                            "score": mean_score,
                            "threshold": threshold,
                            "status": status,
                            "anomalous_log_lines": anomalous_lines,
                            "total_log_lines": len(ns_scores),
                        }
                    )

        # Analyze patterns for failure prediction - pass historical failures for correlation
        historical_failures_for_analysis = []
        if persistence_available and training_store:
            try:
                from datetime import timedelta

                historical_failures_for_analysis = (
                    training_store.get_failure_labels_in_window(
                        start_time=datetime.now() - timedelta(hours=24),
                        end_time=datetime.now(),
                    )
                )
            except Exception as e:
                logger.debug(f"Could not retrieve historical failures: {e}")

        pattern_analysis = analyze_log_patterns_for_failure_prediction(
            log_df, historical_failures_for_analysis
        )

        # Generate predictions using both pattern analysis and labeled data
        predictions = generate_failure_predictions(
            pattern_analysis,
            confidence_threshold,
            prediction_window,
            historical_failures=historical_failures_for_analysis,
            labels=labels,
        )
        result["predictions"] = predictions

        # Analyze trends
        error_logs = log_df[log_df["log_level"].isin(["ERROR", "FATAL", "PANIC"])]
        error_rate = len(error_logs) / len(log_df) if len(log_df) > 0 else 0.0

        if error_rate > 0.15:
            result["trend_analysis"]["error_rate_trend"] = "increasing"
        elif error_rate < 0.05:
            result["trend_analysis"]["error_rate_trend"] = "decreasing"
        else:
            result["trend_analysis"]["error_rate_trend"] = "stable"

        # Resource trend based on log patterns
        resource_indicators = (
            log_df["raw_message"]
            .str.contains(r"memory|cpu|disk|storage|resource", case=False, na=False)
            .sum()
        )

        if resource_indicators > len(log_df) * 0.1:
            result["trend_analysis"]["resource_trend"] = "concerning"
        else:
            result["trend_analysis"]["resource_trend"] = "stable"

        # Performance trend based on response times and timeouts
        performance_indicators = (
            log_df["raw_message"]
            .str.contains(
                r"timeout|slow|latency|performance|delay", case=False, na=False
            )
            .sum()
        )

        if performance_indicators > len(log_df) * 0.08:
            result["trend_analysis"]["performance_trend"] = "degrading"
        else:
            result["trend_analysis"]["performance_trend"] = "stable"

        # Update has_failure_labels to correctly reflect historical failures used
        result["model_info"]["has_failure_labels"] = (
            labels is not None and len(labels) > 0
        ) or (
            historical_failures_for_analysis
            and len(historical_failures_for_analysis) > 0
        )

        logger.info(
            f"Predictive analysis complete: {len(predictions)} predictions generated"
        )
        return result

    except Exception as e:
        logger.error(f"Error in predictive log analysis: {str(e)}", exc_info=True)
        return {
            "predictions": [],
            "model_performance": {
                "accuracy": 0.0,
                "precision": 0.0,
                "recall": 0.0,
                "last_training_time": datetime.now().isoformat(),
            },
            "anomaly_scores": [],
            "trend_analysis": {
                "error_rate_trend": "error",
                "resource_trend": "error",
                "performance_trend": "error",
            },
            "model_info": {
                "model_id": None,
                "loaded_from_cache": False,
                "training_samples": 0,
                "has_failure_labels": False,
                "persistence_enabled": False,
            },
            "error": str(e),
        }


@mcp.tool()
@log_tool_execution
async def manage_prediction_training_data(
    action: str = "stats",
    failure_type: Optional[str] = None,
    namespace: Optional[str] = None,
    resource_name: Optional[str] = None,
    severity: Optional[str] = None,
    collect_from_namespaces: Optional[List[str]] = None,
    max_namespaces: int = 10,
) -> Dict[str, Any]:
    """
    Manage training data for the predictive log analyzer.

    This tool allows viewing, collecting, and managing failure labels used for
    supervised learning in the predictive_log_analyzer.

    Args:
        action: Action to perform:
            - "stats": Get training data statistics (default)
            - "list_failures": List recent failure labels
            - "add_failure": Manually add a failure label
            - "collect": Trigger failure collection from namespaces
            - "cleanup": Remove old training data
        failure_type: For add_failure - type of failure (e.g., "crash", "oom", "image", "timeout")
        namespace: For add_failure/list_failures - namespace filter
        resource_name: For add_failure - name of the affected resource
        severity: For add_failure - severity level ("critical", "high", "medium", "low")
        collect_from_namespaces: For collect - specific namespaces to collect from
        max_namespaces: For collect - maximum namespaces when auto-detecting

    Returns:
        Dict with action results: statistics, failure list, or operation status.
    """
    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}
    try:
        from helpers.ml_persistence import (
            FailureEventCollector,
            ModelPersistenceManager,
            TrainingDataStore,
        )

        training_store = TrainingDataStore()
        model_manager = ModelPersistenceManager()

        result = {
            "action": action,
            "success": True,
            "timestamp": datetime.now().isoformat(),
        }

        if action == "stats":
            # Get comprehensive training data statistics
            stats = training_store.get_statistics()

            # Get model info
            models = model_manager.list_models()
            current_model_id = model_manager.get_current_model_id()

            result["statistics"] = {
                "training_data": stats,
                "models": {
                    "total_models": len(models),
                    "current_model_id": current_model_id,
                    "recent_models": (
                        [
                            {
                                "model_id": m.get("model_id"),
                                "created_at": m.get("created_at"),
                                "training_samples": m.get("training_samples", 0),
                            }
                            for m in models[-5:]
                        ]
                        if models
                        else []
                    ),
                },
                "recommendations": [],
            }

            # Add recommendations
            if stats.get("total_failure_labels", 0) < 10:
                result["statistics"]["recommendations"].append(
                    "Collect more failure labels using action='collect' to improve predictions"
                )
            if stats.get("total_log_samples", 0) < 100:
                result["statistics"]["recommendations"].append(
                    "Run predictive_log_analyzer to collect more log samples"
                )
            if stats.get("total_correlations", 0) == 0:
                result["statistics"]["recommendations"].append(
                    "No log-failure correlations yet. Labels will be correlated during analysis."
                )

        elif action == "list_failures":
            # List recent failure labels
            from datetime import timedelta

            failures = training_store.get_failure_labels_in_window(
                start_time=datetime.now() - timedelta(days=7), end_time=datetime.now()
            )

            # Filter by namespace if specified
            if namespace:
                failures = [f for f in failures if f.get("namespace") == namespace]

            result["failures"] = failures[:50]  # Limit to 50
            result["total_count"] = len(failures)
            result["filter_applied"] = {"namespace": namespace} if namespace else None

        elif action == "add_failure":
            # Manually add a failure label
            if not failure_type:
                result["success"] = False
                result["error"] = "failure_type is required for add_failure action"
                return result

            label = {
                "failure_type": failure_type,
                "severity": severity or "medium",
                "namespace": namespace or "unknown",
                "resource_name": resource_name or "manual_entry",
                "resource_type": "manual",
                "failure_time": datetime.now().isoformat(),
                "detection_source": "manual",
                "error_category": failure_type,
                "metadata": {
                    "source": "manage_prediction_training_data",
                    "added_by": "user",
                },
            }

            label_id = training_store.store_failure_label(label)

            if label_id:
                result["label_id"] = label_id
                result["message"] = (
                    f"Successfully added failure label for '{failure_type}'"
                )
            else:
                result["success"] = False
                result["message"] = "Failed to add label (may be duplicate)"

        elif action == "collect":
            # Trigger failure collection from namespaces
            failure_collector = FailureEventCollector(training_store)
            collected_counts = {
                "from_events": 0,
                "from_pods": 0,
                "from_pipelines": 0,
                "namespaces_scanned": 0,
            }

            # Determine namespaces to scan
            if collect_from_namespaces:
                target_namespaces = collect_from_namespaces
            else:
                # Auto-detect active namespaces
                try:
                    all_ns = await list_namespaces()
                    tekton_ns = await detect_tekton_namespaces()
                    active_ns = []
                    for category in tekton_ns.values():
                        active_ns.extend(category)
                    target_namespaces = (
                        list(set(active_ns))[:max_namespaces]
                        if active_ns
                        else all_ns[:max_namespaces]
                    )
                except Exception:
                    target_namespaces = []

            for ns in target_namespaces:
                try:
                    collected_counts["namespaces_scanned"] += 1

                    # Collect from events - use dict format for FailureEventCollector
                    try:
                        events_as_dicts = await _get_namespace_events_as_dicts(
                            ns, limit=200
                        )
                        if events_as_dicts:
                            count = failure_collector.collect_from_events(
                                events_as_dicts, ns
                            )
                            collected_counts["from_events"] += count
                    except Exception as e:
                        logger.debug(f"Failed to collect events from {ns}: {e}")

                    # Collect from pod statuses
                    try:
                        pods = await asyncio.to_thread(
                            k8s_core_api.list_namespaced_pod, namespace=ns, limit=100
                        )
                        count = failure_collector.collect_from_pod_status(
                            pods.items, ns
                        )
                        collected_counts["from_pods"] += count
                    except Exception as e:
                        logger.debug(f"Failed to collect pod statuses from {ns}: {e}")

                    # Collect from pipeline runs
                    try:
                        prs = await list_pipelineruns(namespace=ns)
                        if prs and isinstance(prs, list):
                            # Filter to failed pipelines
                            failed_prs = [
                                pr
                                for pr in prs
                                if pr.get("status")
                                in ["Failed", "Error", "CouldntGetTask"]
                            ]
                            count = failure_collector.collect_from_pipeline_runs(
                                [
                                    {
                                        "status": {
                                            "conditions": [
                                                {
                                                    "type": "Succeeded",
                                                    "status": "False",
                                                    "message": pr.get("status", ""),
                                                }
                                            ]
                                        },
                                        "metadata": {
                                            "name": pr.get("name"),
                                            "creationTimestamp": pr.get("started_at"),
                                        },
                                        "spec": {
                                            "pipelineRef": {
                                                "name": pr.get("pipeline", "")
                                            }
                                        },
                                    }
                                    for pr in failed_prs
                                ],
                                ns,
                            )
                            collected_counts["from_pipelines"] += count
                    except Exception as e:
                        logger.debug(f"Failed to collect pipeline runs from {ns}: {e}")

                except Exception as e:
                    logger.debug(f"Failed to scan namespace {ns}: {e}")

            result["collected"] = collected_counts
            result["total_collected"] = sum(
                [
                    collected_counts["from_events"],
                    collected_counts["from_pods"],
                    collected_counts["from_pipelines"],
                ]
            )
            result["message"] = (
                f"Collected {result['total_collected']} failure labels from {collected_counts['namespaces_scanned']} namespaces"
            )

        elif action == "cleanup":
            # Clean up old training data
            deleted_data = training_store.cleanup_old_data(max_age_days=90)
            deleted_models = model_manager.cleanup_old_models(
                max_age_days=30, keep_min=3
            )

            result["cleanup_results"] = {
                "training_data_deleted": deleted_data,
                "models_deleted": deleted_models,
            }
            result["message"] = (
                f"Cleaned up {deleted_data} old data records and {deleted_models} old models"
            )

        else:
            result["success"] = False
            result["error"] = (
                f"Unknown action: {action}. Valid actions: stats, list_failures, add_failure, collect, cleanup"
            )

        return result

    except ImportError as e:
        return {
            "action": action,
            "success": False,
            "error": f"ML persistence module not available: {e}",
            "message": "Install required dependencies: pip install joblib scikit-learn",
        }
    except Exception as e:
        logger.error(f"Error in manage_prediction_training_data: {e}", exc_info=True)
        return {"action": action, "success": False, "error": str(e)}


@mcp.tool()
@log_tool_execution
async def resource_bottleneck_forecaster(
    forecast_horizon: str = "24h",
    resource_types: Optional[List[str]] = None,
    namespaces: Optional[List[str]] = None,
    trend_analysis_period: str = "7d",
) -> Dict[str, Any]:
    """
    Forecast resource bottlenecks by analyzing utilization trends and predicting exhaustion points.

    Uses time-series analysis to predict CPU, memory, disk, and network capacity constraints.

    Args:
        forecast_horizon: Forecast window - "1h", "6h", "24h", "7d", "30d" (default: "24h").
        resource_types: Resources to analyze - cpu, memory, disk, network, pvc (default: all).
        namespaces: Specific namespaces to focus on.
        trend_analysis_period: Historical period for trends (default: "7d").

    Returns:
        Dict: Keys: forecasts, capacity_recommendations, cluster_overview, historical_accuracy.
    """
    return await resource_bottleneck_forecaster_impl(
        forecast_horizon=forecast_horizon,
        resource_types=resource_types,
        namespaces=namespaces,
        trend_analysis_period=trend_analysis_period,
        k8s_core_api=k8s_core_api,
        prometheus_query_fn=prometheus_query,
    )


# Tool 19: Semantic Log Search
@mcp.tool()
async def semantic_log_search(
    query: str,
    time_range: str = "1h",
    namespaces: Optional[List[str]] = None,
    severity_levels: Optional[List[str]] = None,
    max_results: int = 100,
    context_lines: int = 3,
    group_similar: bool = True,
) -> Dict[str, Any]:
    """
    Search logs using natural language queries with semantic understanding beyond keyword matching.

    Uses NLP for query interpretation, Kubernetes/Tekton entity recognition, and relevance ranking.

    Args:
        query: Natural language query describing what to search for.
        time_range: Time range - "1h", "6h", "24h", "7d" (default: "1h").
        namespaces: Specific namespaces to search (default: auto-detect relevant namespaces).
        severity_levels: Log severity levels to include.
        max_results: Maximum results to return (default: 100).
        context_lines: Surrounding lines per match (default: 3).
        group_similar: Group similar log entries (default: True).

    Returns:
        Dict: Keys: query_interpretation, search_results, result_summary, suggestions.
    """
    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}
    logger.info(
        f"Starting semantic log search for query: '{query}' with time_range: {time_range}"
    )

    try:
        # === Query Understanding and Interpretation ===
        query_interpretation = interpret_semantic_query(query, time_range)
        logger.info(
            f"Query interpreted as: {query_interpretation['interpreted_intent']}"
        )

        # === Determine Search Strategy ===
        search_strategy = determine_search_strategy(query_interpretation)
        logger.info(f"Using search strategy: {search_strategy['strategy']}")

        # === Entity Recognition and Context Building ===
        identified_components = extract_k8s_entities(query)
        logger.info(f"Identified components: {identified_components}")

        # === Build Search Parameters ===
        search_params = {
            "namespaces": await _get_target_namespaces(
                namespaces,
                identified_components,
                list_namespaces,
                detect_tekton_namespaces,
            ),
            "time_range": time_range,
            "severity_levels": severity_levels or ["error", "warn", "info", "debug"],
            "max_results": max_results,
            "context_lines": context_lines,
        }

        # === Execute Semantic Search ===
        search_results = []
        sources_searched = 0

        # Search across identified namespaces with fixed function calls
        for namespace in search_params["namespaces"]:
            logger.info(f"Searching namespace: {namespace}")
            try:
                namespace_results = []

                # Get pods in namespace
                pods_info = await list_pods_in_namespace(namespace)

                # Search pod logs with correct arguments
                for pod_info in pods_info[
                    :5
                ]:  # Limit to 5 pods per namespace for performance
                    if isinstance(pod_info, dict) and "error" not in pod_info:
                        try:
                            pod_logs_result = await _search_pod_logs_semantically(
                                pod_info,
                                namespace,
                                query_interpretation,
                                search_params,
                                get_pod_logs,
                                _build_log_params,
                                find_semantic_matches,
                            )
                            if pod_logs_result:
                                namespace_results.extend(pod_logs_result)
                        except Exception as e:
                            logger.debug(
                                f"Error searching pod logs in {namespace}: {e}"
                            )
                            continue

                # Search events with correct arguments
                try:
                    events_result = await _search_events_semantically(
                        namespace,
                        query_interpretation,
                        search_params,
                        smart_get_namespace_events,
                        calculate_semantic_relevance,
                        identify_match_reasons,
                        extract_log_metadata,
                    )
                    if events_result:
                        namespace_results.extend(events_result)
                except Exception as e:
                    logger.debug(f"Error searching events in {namespace}: {e}")

                # Search Tekton resources if relevant
                if any(
                    comp in ["pipelinerun", "taskrun", "pipeline"]
                    for comp in query_interpretation.get("semantic_keywords", [])
                ):
                    try:
                        tekton_results = await _search_tekton_resources_semantically(
                            namespace,
                            query_interpretation,
                            search_params,
                            list_pipelineruns,
                            calculate_semantic_relevance,
                        )
                        if tekton_results:
                            namespace_results.extend(tekton_results)
                    except Exception as e:
                        logger.debug(
                            f"Error searching Tekton resources in {namespace}: {e}"
                        )

                search_results.extend(namespace_results)

            except Exception as e:
                logger.warning(f"Error searching namespace {namespace}: {e}")
                continue

            sources_searched += 1

            # Respect max_results limit
            if len(search_results) >= max_results:
                search_results = search_results[:max_results]
                break

        # === Semantic Ranking and Relevance Scoring ===
        ranked_results = rank_results_by_semantic_relevance(
            search_results, query_interpretation, group_similar
        )

        # === Pattern Analysis ===
        common_patterns = identify_common_patterns(ranked_results)
        severity_distribution = analyze_severity_distribution(ranked_results)

        # === Generate Suggestions ===
        suggestions = generate_semantic_suggestions(
            query_interpretation, ranked_results
        )

        # === Build Final Response ===
        return {
            "query_interpretation": {
                "original_query": query,
                "interpreted_intent": query_interpretation["interpreted_intent"],
                "search_strategy": search_strategy["strategy"],
                "identified_components": identified_components,
                "time_scope": time_range,
            },
            "search_results": ranked_results,
            "result_summary": {
                "total_matches": len(ranked_results),
                "sources_searched": sources_searched,
                "common_patterns": common_patterns,
                "severity_distribution": severity_distribution,
            },
            "suggestions": suggestions,
        }

    except Exception as e:
        logger.error(f"Error in semantic log search: {str(e)}", exc_info=True)
        return {
            "query_interpretation": {
                "original_query": query,
                "interpreted_intent": "Error processing query",
                "search_strategy": "error",
                "identified_components": [],
                "time_scope": time_range,
            },
            "search_results": [],
            "result_summary": {
                "total_matches": 0,
                "sources_searched": 0,
                "common_patterns": [],
                "severity_distribution": {},
            },
            "suggestions": {
                "related_queries": [],
                "broader_search": "Try simplifying your query",
                "narrower_search": "Add more specific terms",
            },
            "error": str(e),
        }


# NEW TOOL: SIMULATION SCENARIOS
@mcp.tool()
async def what_if_scenario_simulator(
    scenario_type: str,
    changes: Dict[str, Any],
    scope: Optional[Dict[str, Any]] = None,
    simulation_duration: str = "24h",
    load_profile: str = "current",
    risk_tolerance: str = "moderate",
) -> Dict[str, Any]:
    """
    Simulate impact of configuration changes before applying to live system with risk assessment.

    Uses Monte Carlo simulation and load modeling based on historical data.

    Args:
        scenario_type: Type - "resource_limits", "scaling", "configuration", "deployment".
        changes: Changes to simulate with before/after values.
        scope: Simulation scope - clusters, namespaces, components.
        simulation_duration: Duration - "1h", "24h", "7d" (default: "24h").
        load_profile: Expected load - "current", "peak", "custom" (default: "current").
        risk_tolerance: Risk level - "conservative", "moderate", "aggressive" (default: "moderate").

    Returns:
        Dict: Keys: simulation_id, impact_analysis, risk_assessment, affected_components, recommendations.
    """
    return await what_if_scenario_simulator_impl(
        scenario_type=scenario_type,
        changes=changes,
        scope=scope,
        simulation_duration=simulation_duration,
        load_profile=load_profile,
        risk_tolerance=risk_tolerance,
        k8s_core_api=k8s_core_api,
        k8s_apps_api=k8s_apps_api,
        list_namespaces_fn=list_namespaces,
        list_pods_fn=list_pods,
        prometheus_query_fn=_execute_prometheus_query_internal,
    )


@mcp.tool()
async def query_kubearchive(
    resource_type: str,
    namespace: str,
    name: Optional[str] = None,
    label_selector: Optional[str] = None,
    field_selector: Optional[str] = None,
    since_time: Optional[str] = None,
    until_time: Optional[str] = None,
    include_logs: bool = False,
    container: Optional[str] = None,
    limit: int = 100,
    output_format: str = "summary",
) -> Dict[str, Any]:
    """
    Query archived Kubernetes resources from KubeArchive (historical data no longer on the cluster).

    Single entry point for archived resources and their logs: set include_logs=True to attach logs
    for pipelinerun, taskrun, and pod results (use an exact name when you only need one resource).
    Optional container selects a container for multi-container pods.

    Args:
        resource_type: One of pipelinerun, taskrun, pod, release, snapshot (case-insensitive).
        namespace: Kubernetes namespace to search.
        name: Optional resource name; wildcards supported (e.g. my-pipeline-*).
        label_selector: Kubernetes label selector string.
        field_selector: Kubernetes field selector string (KubeArchive support may vary).
        since_time: Lower bound for creation time (RFC3339 or ISO date).
        until_time: Upper bound for creation time (RFC3339 or ISO date).
        include_logs: If True, fetch logs for each matching pod, taskrun, or pipelinerun.
        container: Optional container name (pods; passed to KubeArchive when include_logs=True).
        limit: Max resources to return (1-1000; out-of-range values are clamped).
        output_format: summary, detailed, or yaml.

    Returns:
        Dict with kubearchive_status, kubearchive_endpoint, resources, total_count, time_range,
        filters_applied, message, and error when applicable.
    """
    if not kubearchive_endpoint_discovery:
        return {"error": "Kubernetes client not available."}
    ka_logger = logging.getLogger("lumino-mcp.query_kubearchive")
    valid_types = ["pipelinerun", "taskrun", "pod", "release", "snapshot"]
    valid_formats = ["summary", "detailed", "yaml"]
    filters_applied = {
        "resource_type": resource_type,
        "namespace": namespace,
        "name": name,
        "label_selector": label_selector,
        "field_selector": field_selector,
        "container": container,
    }

    def _base_response(
        status: str,
        resources: Optional[List[Any]] = None,
        total: int = 0,
        endpoint: Optional[str] = None,
        error: Optional[str] = None,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "kubearchive_status": status,
            "resources": resources if resources is not None else [],
            "total_count": total,
            "time_range": {"since": since_time, "until": until_time},
            "filters_applied": filters_applied,
        }
        if endpoint is not None:
            out["kubearchive_endpoint"] = endpoint
        if message:
            out["message"] = message
        if error:
            out["error"] = error
        return out

    ka_logger.info(
        "query_kubearchive resource_type=%s namespace=%s include_logs=%s container=%s limit=%s output_format=%s",
        resource_type,
        namespace,
        include_logs,
        container,
        limit,
        output_format,
    )

    try:
        if resource_type.lower() not in valid_types:
            return _base_response(
                "error",
                error=f"Invalid resource_type '{resource_type}'. Must be one of: {', '.join(valid_types)}",
                message="Validation failed",
            )

        if output_format not in valid_formats:
            return _base_response(
                "error",
                error=f"Invalid output_format '{output_format}'. Must be one of: {', '.join(valid_formats)}",
                message="Validation failed",
            )

        orig_limit = limit
        if limit < 1 or limit > 1000:
            limit = min(max(1, limit), 1000)
            ka_logger.warning(
                "limit adjusted from %s to %s (valid range 1-1000)", orig_limit, limit
            )

        if since_time:
            try:
                since_time = normalize_to_rfc3339(since_time.strip())
            except ValueError as e:
                return _base_response(
                    "error",
                    error=f"Invalid since_time: {e}. Use RFC3339 or ISO date, e.g. 2024-01-15T10:30:00Z or 2024-01-15",
                    message="Validation failed",
                )

        if until_time:
            try:
                until_time = normalize_to_rfc3339(until_time.strip())
            except ValueError as e:
                return _base_response(
                    "error",
                    error=f"Invalid until_time: {e}. Use RFC3339 or ISO date, e.g. 2024-01-15T10:30:00Z or 2024-01-15",
                    message="Validation failed",
                )

        if kubearchive_endpoint_discovery is None:
            return _base_response(
                "error",
                error="Kubernetes API clients are not initialized. Load kubeconfig or run in-cluster.",
                message="KubeArchive discovery requires CoreV1Api and CustomObjectsApi",
            )

        availability = await check_kubearchive_availability(
            kubearchive_endpoint_discovery
        )
        ka_endpoint = availability.get("endpoint")
        if not availability.get("available"):
            msg = availability.get("message", "KubeArchive not available")
            ka_logger.warning("KubeArchive availability check failed: %s", msg)
            sug = [
                "Deploy KubeArchive (https://github.com/kubearchive/kubearchive)",
                "Set KUBEARCHIVE_HOST to your KubeArchive API base URL",
                "Example: export KUBEARCHIVE_HOST='https://kubearchive-api-server.kubearchive.svc.cluster.local:8081'",
            ]
            out = _base_response(
                "error",
                error=msg,
                message="KubeArchive unavailable",
            )
            if ka_endpoint:
                out["kubearchive_endpoint"] = ka_endpoint
            out["suggestions"] = sug
            return out

        ka_client = await setup_kubearchive_client(
            endpoint_discovery=kubearchive_endpoint_discovery,
            k8s_core_api=k8s_core_api,
        )

        result = await query_kubearchive_resources(
            kubearchive_client=ka_client,
            resource_type=resource_type,
            namespace=namespace,
            name=name,
            label_selector=label_selector,
            field_selector=field_selector,
            since_time=since_time,
            until_time=until_time,
            include_logs=include_logs,
            container=container,
            limit=limit,
            output_format=output_format,
        )

        result["filters_applied"] = filters_applied
        result["kubearchive_endpoint"] = ka_endpoint

        if result.get("kubearchive_status") == "success":
            n = result.get("total_count", 0)
            result["message"] = (
                f"Found {n} archived resource(s)"
                if n
                else "No archived resources found matching criteria"
            )
        else:
            if "message" not in result:
                result["message"] = result.get("error", "KubeArchive query failed")

        ka_logger.info(
            "query_kubearchive completed status=%s count=%s",
            result.get("kubearchive_status"),
            result.get("total_count"),
        )
        return result

    except Exception as e:
        ka_logger.error("query_kubearchive failed: %s", e, exc_info=True)
        out = _base_response("error", error=str(e), message="Unexpected error")
        if kubearchive_endpoint_discovery is not None:
            try:
                ep = await kubearchive_endpoint_discovery.discover_endpoint()
                if ep:
                    out["kubearchive_endpoint"] = ep
            except Exception:
                pass
        return out
