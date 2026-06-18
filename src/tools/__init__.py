"""Tool modules for LUMINO MCP Server.

Extracted from server-mcp.py as part of issue #58 (sub-task of #30).
"""

from .kubernetes_tools import (
    _ADMISSION_RESOURCES,
    _APPS_RESOURCES,
    _AUTOSCALING_RESOURCES,
    _BATCH_RESOURCES,
    _CORE_RESOURCES,
    _KONFLUX_RESOURCES,
    _MONITORING_RESOURCES,
    _NETWORKING_RESOURCES,
    _STORAGE_RESOURCES,
    _TEKTON_RESOURCES,
    _TEKTON_TRIGGERS_RESOURCES,
    _all_supported_types,
    check_resource_constraints_impl,
    get_kubernetes_resource_impl,
    list_namespaces_impl,
    list_pods_in_namespace_impl,
)
from .log_tools import (
    _quick_volume_estimate_impl,
    analyze_logs_impl,
    analyze_pod_logs_hybrid_impl,
    detect_log_anomalies_impl,
    get_etcd_logs_impl,
    get_pod_logs_impl,
    smart_summarize_pod_logs_impl,
    stream_analyze_pod_logs_impl,
)
from .prometheus_helpers import (
    _discover_prometheus_via_operator_crd,
    _discover_prometheus_via_routes,
    _discover_prometheus_via_services,
    _discover_thanos_via_services,
    discover_prometheus_endpoint,
)
from .prometheus_query import (
    _execute_prometheus_query_internal,
    _get_k8s_bearer_token,
    _process_prometheus_results,
    prometheus_query_impl,
)
from .prometheus_tools import (
    ci_cd_performance_baselining_tool_impl,
    resource_bottleneck_forecaster_impl,
    what_if_scenario_simulator_impl,
)
from .tekton_tools import get_pipelinerun_logs_impl

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
    "get_pod_logs_impl",
    "analyze_logs_impl",
    "detect_log_anomalies_impl",
    "_quick_volume_estimate_impl",
    "get_etcd_logs_impl",
    "discover_prometheus_endpoint",
    "_discover_prometheus_via_routes",
    "_discover_prometheus_via_services",
    "_discover_prometheus_via_operator_crd",
    "_discover_thanos_via_services",
    "_get_k8s_bearer_token",
    "_execute_prometheus_query_internal",
    "_process_prometheus_results",
    "prometheus_query_impl",
    "ci_cd_performance_baselining_tool_impl",
    "resource_bottleneck_forecaster_impl",
    "what_if_scenario_simulator_impl",
]

from .event_rca_tools import (
    _get_namespace_events_as_dicts as _get_namespace_events_as_dicts_impl,
)
from .event_rca_tools import (
    _get_namespace_events_internal as _get_namespace_events_internal_impl,
)
from .event_rca_tools import (
    advanced_event_analytics_impl,
    automated_triage_rca_report_generator_impl,
    progressive_event_analysis_impl,
    smart_get_namespace_events_impl,
)

__all__ += [
    "_get_namespace_events_internal_impl",
    "_get_namespace_events_as_dicts_impl",
    "smart_get_namespace_events_impl",
    "progressive_event_analysis_impl",
    "advanced_event_analytics_impl",
    "automated_triage_rca_report_generator_impl",
]
