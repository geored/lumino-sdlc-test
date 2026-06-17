"""Tool modules for LUMINO MCP Server.

Extracted from server-mcp.py as part of issue #58 (sub-task of #30).
"""

from .kubernetes_tools import (
    list_namespaces_impl,
    list_pods_in_namespace_impl,
    get_kubernetes_resource_impl,
    check_resource_constraints_impl,
    _all_supported_types,
    _CORE_RESOURCES,
    _APPS_RESOURCES,
    _BATCH_RESOURCES,
    _NETWORKING_RESOURCES,
    _STORAGE_RESOURCES,
    _AUTOSCALING_RESOURCES,
    _TEKTON_RESOURCES,
    _TEKTON_TRIGGERS_RESOURCES,
    _MONITORING_RESOURCES,
    _ADMISSION_RESOURCES,
    _KONFLUX_RESOURCES,
)

from .tekton_tools import (
    get_pipelinerun_logs_impl,
)

from .log_tools import (
    smart_summarize_pod_logs_impl,
    stream_analyze_pod_logs_impl,
    analyze_pod_logs_hybrid_impl,
)

from .prometheus_helpers import (
    discover_prometheus_endpoint,
    _discover_prometheus_via_routes,
    _discover_prometheus_via_services,
    _discover_prometheus_via_operator_crd,
    _discover_thanos_via_services,
)

from .prometheus_query import (
    _get_k8s_bearer_token,
    _execute_prometheus_query_internal,
    _process_prometheus_results,
    prometheus_query_impl,
)

__all__ = [
    "list_namespaces_impl",
    "list_pods_in_namespace_impl",
    "get_kubernetes_resource_impl",
    "check_resource_constraints_impl",
    "_all_supported_types",
    "_CORE_RESOURCES",
    "_APPS_RESOURCES",
    "_BATCH_RESOURCES",
    "_NETWORKING_RESOURCES",
    "_STORAGE_RESOURCES",
    "_AUTOSCALING_RESOURCES",
    "_TEKTON_RESOURCES",
    "_TEKTON_TRIGGERS_RESOURCES",
    "_MONITORING_RESOURCES",
    "_ADMISSION_RESOURCES",
    "_KONFLUX_RESOURCES",
    "get_pipelinerun_logs_impl",
    "smart_summarize_pod_logs_impl",
    "stream_analyze_pod_logs_impl",
    "analyze_pod_logs_hybrid_impl",
    "discover_prometheus_endpoint",
    "_discover_prometheus_via_routes",
    "_discover_prometheus_via_services",
    "_discover_prometheus_via_operator_crd",
    "_discover_thanos_via_services",
    "_get_k8s_bearer_token",
    "_execute_prometheus_query_internal",
    "_process_prometheus_results",
    "prometheus_query_impl",
]
