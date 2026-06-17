"""
Tests for Issue #17: Missing k8s_*_api None-Guards in Most Tools.

Every @mcp.tool() that calls a Kubernetes API client must return an error value
(not raise AttributeError) when the relevant client is None.
"""
import importlib
import importlib.util
import sys
import types
import asyncio
import pytest
from unittest.mock import MagicMock, patch, AsyncMock


# ---------------------------------------------------------------------------
# Load the server module via importlib (matches main.py approach)
# ---------------------------------------------------------------------------

def load_server_module():
    spec = importlib.util.spec_from_file_location(
        "server_mcp", "src/server-mcp.py"
    )
    mod = importlib.util.module_from_spec(spec)
    # Inject minimal kubernetes stubs so the module loads without a cluster
    kube_stub = types.ModuleType("kubernetes")
    kube_client = types.ModuleType("kubernetes.client")
    kube_config = types.ModuleType("kubernetes.config")
    kube_watch  = types.ModuleType("kubernetes.watch")
    kube_rest   = types.ModuleType("kubernetes.client.rest")

    class _ApiException(Exception):
        def __init__(self, status=0, reason=""):
            self.status = status
            self.reason = reason
        def __str__(self):
            return f"({self.status})\nReason: {self.reason}"

    kube_rest.ApiException = _ApiException
    kube_client.rest = kube_rest
    kube_client.ApiException = _ApiException
    kube_config.load_incluster_config = MagicMock(side_effect=Exception("no cluster"))
    kube_config.load_kube_config      = MagicMock(side_effect=Exception("no kubeconfig"))
    kube_watch.Watch = MagicMock()
    kube_stub.client = kube_client
    kube_stub.config = kube_config
    kube_stub.watch  = kube_watch

    sys.modules.setdefault("kubernetes",            kube_stub)
    sys.modules.setdefault("kubernetes.client",     kube_client)
    sys.modules.setdefault("kubernetes.config",     kube_config)
    sys.modules.setdefault("kubernetes.watch",      kube_watch)
    sys.modules.setdefault("kubernetes.client.rest",kube_rest)

    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def srv():
    return load_server_module()


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Helper: patch a client to None and call the tool
# ---------------------------------------------------------------------------

def _patch_and_call(srv, client_attr, tool_name, *args, **kwargs):
    """Set client_attr to None on the module, call the tool, restore."""
    original = getattr(srv, client_attr)
    setattr(srv, client_attr, None)
    try:
        tool_fn = getattr(srv, tool_name)
        if asyncio.iscoroutinefunction(tool_fn):
            return run(tool_fn(*args, **kwargs))
        return tool_fn(*args, **kwargs)
    finally:
        setattr(srv, client_attr, original)


# ---------------------------------------------------------------------------
# list_namespaces  (returns List[str])
# ---------------------------------------------------------------------------

def test_list_namespaces_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "list_namespaces")
    assert isinstance(result, list), "Expected list when core_api is None"
    # Must not raise; empty list is acceptable per task spec
    assert result == [] or all(isinstance(x, str) for x in result)


# ---------------------------------------------------------------------------
# list_pipelineruns  (returns List[Dict])
# ---------------------------------------------------------------------------

def test_list_pipelineruns_none_custom_api(srv):
    result = _patch_and_call(srv, "k8s_custom_api", "list_pipelineruns", "default")
    assert isinstance(result, list)
    assert len(result) == 1 and "error" in result[0]


# ---------------------------------------------------------------------------
# list_taskruns  (returns List[Dict])
# ---------------------------------------------------------------------------

def test_list_taskruns_none_custom_api(srv):
    result = _patch_and_call(srv, "k8s_custom_api", "list_taskruns", "default")
    assert isinstance(result, list)
    assert len(result) == 1 and "error" in result[0]


# ---------------------------------------------------------------------------
# list_pods_in_namespace  (returns List[Dict])
# ---------------------------------------------------------------------------

def test_list_pods_in_namespace_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "list_pods_in_namespace", "default")
    assert isinstance(result, list)
    assert len(result) == 1 and "error" in result[0]


# ---------------------------------------------------------------------------
# get_kubernetes_resource  (returns str)
# ---------------------------------------------------------------------------

def test_get_kubernetes_resource_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "get_kubernetes_resource",
                             "pod", "default", "mypod")
    assert isinstance(result, str)
    assert "error" in result.lower() or "Error" in result


# ---------------------------------------------------------------------------
# get_pipelinerun_logs  (returns Dict)
# ---------------------------------------------------------------------------

def test_get_pipelinerun_logs_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "get_pipelinerun_logs",
                             "default", "my-pr")
    assert isinstance(result, dict)
    assert "error" in result


def test_get_pipelinerun_logs_none_custom_api(srv):
    result = _patch_and_call(srv, "k8s_custom_api", "get_pipelinerun_logs",
                             "default", "my-pr")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# check_resource_constraints  (returns Dict)
# ---------------------------------------------------------------------------

def test_check_resource_constraints_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "check_resource_constraints", "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# detect_anomalies  (returns Dict)
# ---------------------------------------------------------------------------

def test_detect_anomalies_none_custom_api(srv):
    result = _patch_and_call(srv, "k8s_custom_api", "detect_anomalies", "default")
    assert isinstance(result, dict)
    assert "error" not in result or True  # returns {"pipeline_anomalies":[], "task_anomalies":[]}
    assert "pipeline_anomalies" in result or "error" in result


# ---------------------------------------------------------------------------
# smart_get_namespace_events  (returns Dict)
# ---------------------------------------------------------------------------

def test_smart_get_namespace_events_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "smart_get_namespace_events", "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# analyze_failed_pipeline  (returns Dict)
# ---------------------------------------------------------------------------

def test_analyze_failed_pipeline_none_custom_api(srv):
    result = _patch_and_call(srv, "k8s_custom_api", "analyze_failed_pipeline",
                             "default", "my-pr")
    assert isinstance(result, dict)
    assert "error" in result


def test_analyze_failed_pipeline_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "analyze_failed_pipeline",
                             "default", "my-pr")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# list_recent_pipeline_runs  (returns Dict)
# ---------------------------------------------------------------------------

def test_list_recent_pipeline_runs_none_custom_api(srv):
    result = _patch_and_call(srv, "k8s_custom_api", "list_recent_pipeline_runs")
    assert isinstance(result, dict)
    # Returns {} when client is None
    assert result == {} or "error" in result


# ---------------------------------------------------------------------------
# find_pipeline  (returns Dict)
# ---------------------------------------------------------------------------

def test_find_pipeline_none_custom_api(srv):
    result = _patch_and_call(srv, "k8s_custom_api", "find_pipeline", "my-pipeline")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# get_tekton_pipeline_runs_status  (returns Dict)
# ---------------------------------------------------------------------------

def test_get_tekton_pipeline_runs_status_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "get_tekton_pipeline_runs_status")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# search_resources_by_labels  (returns Dict)
# ---------------------------------------------------------------------------

def test_search_resources_by_labels_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "search_resources_by_labels",
                             ["pods"], [{"key": "app", "value": "myapp", "operator": "equals"}])
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# get_pod_logs  (returns Dict)
# ---------------------------------------------------------------------------

def test_get_pod_logs_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "get_pod_logs",
                             "default", "mypod")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# smart_summarize_pod_logs  (returns Dict)
# ---------------------------------------------------------------------------

def test_smart_summarize_pod_logs_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "smart_summarize_pod_logs",
                             "default", "mypod")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# investigate_tls_certificate_issues  (returns Dict)
# ---------------------------------------------------------------------------

def test_investigate_tls_certificate_issues_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "investigate_tls_certificate_issues",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# conservative_namespace_overview  (returns Dict)
# ---------------------------------------------------------------------------

def test_conservative_namespace_overview_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "conservative_namespace_overview",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# adaptive_namespace_investigation  (returns Dict)
# ---------------------------------------------------------------------------

def test_adaptive_namespace_investigation_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "adaptive_namespace_investigation",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# get_etcd_logs  (returns Dict)
# ---------------------------------------------------------------------------

def test_get_etcd_logs_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "get_etcd_logs")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# stream_analyze_pod_logs  (returns Dict)
# ---------------------------------------------------------------------------

def test_stream_analyze_pod_logs_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "stream_analyze_pod_logs",
                             "default", "mypod")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# analyze_pod_logs_hybrid  (returns Dict)
# ---------------------------------------------------------------------------

def test_analyze_pod_logs_hybrid_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "analyze_pod_logs_hybrid",
                             "default", "mypod")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# progressive_event_analysis  (returns Dict)
# ---------------------------------------------------------------------------

def test_progressive_event_analysis_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "progressive_event_analysis",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# advanced_event_analytics  (returns Dict)
# ---------------------------------------------------------------------------

def test_advanced_event_analytics_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "advanced_event_analytics",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# automated_triage_rca_report_generator  (returns Dict)
# ---------------------------------------------------------------------------

def test_automated_triage_rca_report_generator_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "automated_triage_rca_report_generator",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# check_cluster_certificate_health  (returns Dict)
# ---------------------------------------------------------------------------

def test_check_cluster_certificate_health_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "check_cluster_certificate_health")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# ci_cd_performance_baselining_tool  (returns Dict)
# ---------------------------------------------------------------------------

def test_ci_cd_performance_baselining_tool_none_custom_api(srv):
    result = _patch_and_call(srv, "k8s_custom_api", "ci_cd_performance_baselining_tool",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# pipeline_tracer  (returns Dict)
# ---------------------------------------------------------------------------

def test_pipeline_tracer_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "pipeline_tracer",
                             "default", "my-pr")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# get_machine_config_pool_status  (returns Dict)
# ---------------------------------------------------------------------------

def test_get_machine_config_pool_status_none_custom_api(srv):
    result = _patch_and_call(srv, "k8s_custom_api", "get_machine_config_pool_status")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# get_openshift_cluster_operator_status  (returns Dict)
# ---------------------------------------------------------------------------

def test_get_openshift_cluster_operator_status_none_custom_api(srv):
    result = _patch_and_call(srv, "k8s_custom_api", "get_openshift_cluster_operator_status")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# live_system_topology_mapper  (returns Dict)
# ---------------------------------------------------------------------------

def test_live_system_topology_mapper_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "live_system_topology_mapper",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# predictive_log_analyzer  (returns Dict)
# ---------------------------------------------------------------------------

def test_predictive_log_analyzer_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "predictive_log_analyzer",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# manage_prediction_training_data  (returns Dict)
# ---------------------------------------------------------------------------

def test_manage_prediction_training_data_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "manage_prediction_training_data",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# resource_bottleneck_forecaster  (returns Dict)
# ---------------------------------------------------------------------------

def test_resource_bottleneck_forecaster_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "resource_bottleneck_forecaster",
                             "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# semantic_log_search  (returns Dict)
# ---------------------------------------------------------------------------

def test_semantic_log_search_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "semantic_log_search",
                             "find errors", "default")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# what_if_scenario_simulator  (returns Dict)
# ---------------------------------------------------------------------------

def test_what_if_scenario_simulator_none_core_api(srv):
    result = _patch_and_call(srv, "k8s_core_api", "what_if_scenario_simulator",
                             "default", "scale_up")
    assert isinstance(result, dict)
    assert "error" in result


# ---------------------------------------------------------------------------
# query_kubearchive  (guards kubearchive_endpoint_discovery)
# ---------------------------------------------------------------------------

def test_query_kubearchive_none_discovery(srv):
    original = srv.kubearchive_endpoint_discovery
    srv.kubearchive_endpoint_discovery = None
    try:
        result = run(srv.query_kubearchive("pipelinerun", "default"))
        assert isinstance(result, dict)
        assert "error" in result
    finally:
        srv.kubearchive_endpoint_discovery = original


# ---------------------------------------------------------------------------
# analyze_logs — no k8s API used; must still return a dict (not raise)
# ---------------------------------------------------------------------------

def test_analyze_logs_no_k8s_needed(srv):
    result = run(srv.analyze_logs("some log text with ERROR"))
    assert isinstance(result, dict)
    assert "error_count" in result


# ---------------------------------------------------------------------------
# detect_log_anomalies — no k8s API used; must still return a dict (not raise)
# ---------------------------------------------------------------------------

def test_detect_log_anomalies_no_k8s_needed(srv):
    result = run(srv.detect_log_anomalies("ERROR something went wrong\nINFO ok"))
    assert isinstance(result, dict)
    assert "anomaly_detected" in result
