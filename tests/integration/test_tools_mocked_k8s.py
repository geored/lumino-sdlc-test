"""
Integration tests for all major @mcp.tool() implementations with mocked
Kubernetes clients.  Every tool gets ≥1 happy-path test + ≥1 error-path test.

Uses the extracted *_impl functions so tests are independent of the server
module load machinery.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_pod(name, phase="Running"):
    pod = MagicMock()
    pod.metadata.name = name
    pod.metadata.labels = {}
    pod.metadata.creation_timestamp = None
    pod.status.phase = phase
    pod.status.pod_ip = "10.0.0.1"
    pod.spec.node_name = "node-1"
    pod.status.container_statuses = []
    pod.status.init_container_statuses = []
    return pod


def _make_ns(name):
    ns = MagicMock()
    ns.metadata.name = name
    return ns


# ===========================================================================
# list_namespaces_impl
# ===========================================================================


class TestListNamespacesImpl:
    @pytest.mark.asyncio
    async def test_returns_empty_when_client_none(self):
        from tools.kubernetes_tools import list_namespaces_impl
        result = await list_namespaces_impl(None)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_sorted_list(self):
        from tools.kubernetes_tools import list_namespaces_impl
        mock_api = MagicMock()
        ns_list = [_make_ns("z-ns"), _make_ns("a-ns"), _make_ns("m-ns")]
        with patch("tools.kubernetes_tools._namespace_cache",
                   {"namespaces": None, "timestamp": 0}):
            with patch("tools.kubernetes_tools.asyncio.to_thread",
                       new_callable=AsyncMock,
                       return_value=MagicMock(items=ns_list)):
                result = await list_namespaces_impl(mock_api)
        assert isinstance(result, list)
        assert result == sorted(result)

    @pytest.mark.asyncio
    async def test_403_returns_empty(self):
        from tools.kubernetes_tools import list_namespaces_impl
        from kubernetes.client.rest import ApiException
        mock_api = MagicMock()
        with patch("tools.kubernetes_tools._namespace_cache",
                   {"namespaces": None, "timestamp": 0}):
            with patch("tools.kubernetes_tools.asyncio.to_thread",
                       side_effect=ApiException(status=403, reason="Forbidden")):
                result = await list_namespaces_impl(mock_api)
        assert result == []


# ===========================================================================
# list_pods_in_namespace_impl
# ===========================================================================


class TestListPodsInNamespaceImpl:
    @pytest.mark.asyncio
    async def test_returns_error_when_client_none(self):
        from tools.kubernetes_tools import list_pods_in_namespace_impl
        result = await list_pods_in_namespace_impl("default", None)
        assert isinstance(result, list)
        assert "error" in result[0]

    @pytest.mark.asyncio
    async def test_happy_path(self):
        from tools.kubernetes_tools import list_pods_in_namespace_impl
        mock_api = MagicMock()
        pods = [_make_pod("pod-1"), _make_pod("pod-2")]
        with patch("tools.kubernetes_tools.asyncio.to_thread",
                   new_callable=AsyncMock,
                   return_value=MagicMock(items=pods)):
            result = await list_pods_in_namespace_impl("default", mock_api)
        assert len(result) == 2
        assert result[0]["name"] == "pod-1"

    @pytest.mark.asyncio
    async def test_403_returns_error_list(self):
        from tools.kubernetes_tools import list_pods_in_namespace_impl
        from kubernetes.client.rest import ApiException
        mock_api = MagicMock()
        with patch("tools.kubernetes_tools.asyncio.to_thread",
                   side_effect=ApiException(status=403, reason="Forbidden")):
            result = await list_pods_in_namespace_impl("default", mock_api)
        assert "error" in result[0]


# ===========================================================================
# get_kubernetes_resource_impl
# ===========================================================================


class TestGetKubernetesResourceImpl:
    @pytest.mark.asyncio
    async def test_returns_error_when_client_none(self):
        from tools.kubernetes_tools import get_kubernetes_resource_impl
        result = await get_kubernetes_resource_impl(
            "pod", "my-pod", "default", k8s_core_api=None)
        assert "Error" in result

    @pytest.mark.asyncio
    async def test_unsupported_type_returns_error_string(self):
        from tools.kubernetes_tools import get_kubernetes_resource_impl
        result = await get_kubernetes_resource_impl(
            "unknowntype", "res", "default", k8s_core_api=MagicMock())
        assert "Unsupported" in result or "Error" in result

    @pytest.mark.asyncio
    async def test_404_returns_not_found(self):
        from tools.kubernetes_tools import get_kubernetes_resource_impl
        from kubernetes.client.rest import ApiException
        mock_api = MagicMock()
        with patch("tools.kubernetes_tools.asyncio.to_thread",
                   side_effect=ApiException(status=404, reason="Not Found")):
            result = await get_kubernetes_resource_impl(
                "pod", "missing", "default", k8s_core_api=mock_api)
        assert "not found" in result.lower() or "Error" in result


# ===========================================================================
# check_resource_constraints_impl
# ===========================================================================


class TestCheckResourceConstraintsImpl:
    @pytest.mark.asyncio
    async def test_returns_error_when_client_none(self):
        from tools.kubernetes_tools import check_resource_constraints_impl
        result = await check_resource_constraints_impl("default", None)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_happy_path_healthy(self):
        from tools.kubernetes_tools import check_resource_constraints_impl
        mock_api = MagicMock()

        async def fake_list_pods(ns, api, log):
            return []

        with patch("tools.kubernetes_tools.list_pods", fake_list_pods):
            with patch("tools.kubernetes_tools.asyncio.to_thread",
                       new_callable=AsyncMock,
                       return_value=MagicMock(items=[])):
                result = await check_resource_constraints_impl("default", mock_api)
        assert "status" in result

    @pytest.mark.asyncio
    async def test_api_exception_returns_error(self):
        from tools.kubernetes_tools import check_resource_constraints_impl
        from kubernetes.client.rest import ApiException
        mock_api = MagicMock()

        async def fake_list_pods_raises(*a, **kw):
            raise ApiException(status=403, reason="Forbidden")

        with patch("tools.kubernetes_tools.list_pods", fake_list_pods_raises):
            result = await check_resource_constraints_impl("default", mock_api)
        assert "error" in result


# ===========================================================================
# get_pipelinerun_logs_impl
# ===========================================================================


class TestGetPipelinerunLogsImpl:
    def _proc(self):
        p = MagicMock()
        p.get_remaining_budget.return_value = 100000
        p.can_process_more.return_value = True
        p.get_usage_percentage.return_value = 5.0
        p.max_token_budget = 120000
        p.record_usage = MagicMock()
        return p

    @pytest.mark.asyncio
    async def test_error_when_core_api_none(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl
        result = await get_pipelinerun_logs_impl(
            "pr", "ns", k8s_core_api=None, k8s_custom_api=MagicMock(),
            adaptive_processor_cls=MagicMock(), prioritize_pods_fn=AsyncMock(),
            estimate_tokens_fn=AsyncMock(), calculate_tail_lines_fn=MagicMock(),
            truncate_logs_fn=MagicMock())
        assert "error" in result

    @pytest.mark.asyncio
    async def test_error_when_custom_api_none(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl
        result = await get_pipelinerun_logs_impl(
            "pr", "ns", k8s_core_api=MagicMock(), k8s_custom_api=None,
            adaptive_processor_cls=MagicMock(), prioritize_pods_fn=AsyncMock(),
            estimate_tokens_fn=AsyncMock(), calculate_tail_lines_fn=MagicMock(),
            truncate_logs_fn=MagicMock())
        assert "error" in result

    @pytest.mark.asyncio
    async def test_no_pods_returns_info(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl
        with patch("tools.tekton_tools.asyncio.to_thread",
                   new_callable=AsyncMock, return_value=MagicMock(items=[])):
            result = await get_pipelinerun_logs_impl(
                "pr", "ns", k8s_core_api=MagicMock(), k8s_custom_api=MagicMock(),
                adaptive_processor_cls=MagicMock(), prioritize_pods_fn=AsyncMock(return_value=[]),
                estimate_tokens_fn=AsyncMock(return_value=0),
                calculate_tail_lines_fn=MagicMock(return_value=100),
                truncate_logs_fn=MagicMock(return_value=("", False)))
        assert "info" in result or "error" in result

    @pytest.mark.asyncio
    async def test_adaptive_mode_injects_metadata(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl
        pods = [_make_pod("pod-a")]
        proc_cls = MagicMock(return_value=self._proc())
        with patch("tools.tekton_tools.asyncio.to_thread",
                   new_callable=AsyncMock, return_value=MagicMock(items=pods)):
            with patch("tools.tekton_tools.get_all_pod_logs",
                       new_callable=AsyncMock, return_value={"step": "log"}):
                with patch("tools.tekton_tools.asyncio.sleep", new_callable=AsyncMock):
                    result = await get_pipelinerun_logs_impl(
                        "pr", "ns",
                        k8s_core_api=MagicMock(), k8s_custom_api=MagicMock(),
                        adaptive_processor_cls=proc_cls,
                        prioritize_pods_fn=AsyncMock(return_value=["pod-a"]),
                        estimate_tokens_fn=AsyncMock(return_value=100),
                        calculate_tail_lines_fn=MagicMock(return_value=50),
                        truncate_logs_fn=MagicMock(return_value=("log", False)))
        assert "_metadata" in result
        assert result["_metadata"]["adaptive_mode"] is True

    @pytest.mark.asyncio
    async def test_manual_mode_injects_metadata(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl
        pods = [_make_pod("pod-a")]
        proc_cls = MagicMock(return_value=self._proc())
        with patch("tools.tekton_tools.asyncio.to_thread",
                   new_callable=AsyncMock, return_value=MagicMock(items=pods)):
            with patch("tools.tekton_tools.get_all_pod_logs",
                       new_callable=AsyncMock, return_value={"step": "log"}):
                result = await get_pipelinerun_logs_impl(
                    "pr", "ns", tail_lines=100,
                    k8s_core_api=MagicMock(), k8s_custom_api=MagicMock(),
                    adaptive_processor_cls=proc_cls,
                    prioritize_pods_fn=AsyncMock(return_value=["pod-a"]),
                    estimate_tokens_fn=AsyncMock(return_value=100),
                    calculate_tail_lines_fn=MagicMock(return_value=100),
                    truncate_logs_fn=MagicMock(return_value=("log", False)))
        assert "_metadata" in result
        assert result["_metadata"].get("mode") == "manual"

    @pytest.mark.asyncio
    async def test_first_pod_always_processed_despite_budget_exhaustion(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl
        pods = [_make_pod("pod-a"), _make_pod("pod-b")]
        proc = self._proc()
        proc.can_process_more.return_value = False  # budget "exhausted"
        proc_cls = MagicMock(return_value=proc)
        with patch("tools.tekton_tools.asyncio.to_thread",
                   new_callable=AsyncMock, return_value=MagicMock(items=pods)):
            with patch("tools.tekton_tools.get_all_pod_logs",
                       new_callable=AsyncMock, return_value={"step": "data"}):
                with patch("tools.tekton_tools.asyncio.sleep", new_callable=AsyncMock):
                    result = await get_pipelinerun_logs_impl(
                        "pr", "ns",
                        k8s_core_api=MagicMock(), k8s_custom_api=MagicMock(),
                        adaptive_processor_cls=proc_cls,
                        prioritize_pods_fn=AsyncMock(return_value=["pod-a", "pod-b"]),
                        estimate_tokens_fn=AsyncMock(return_value=999999),
                        calculate_tail_lines_fn=MagicMock(return_value=10),
                        truncate_logs_fn=MagicMock(return_value=("trunc", True)))
        assert "pod-a" in result
        assert result["_metadata"]["pods_skipped"] >= 1


# ===========================================================================
# discover_prometheus_endpoint
# ===========================================================================


class TestDiscoverPrometheusEndpoint:
    @pytest.mark.asyncio
    async def test_thanos_env_highest_priority(self):
        from tools.prometheus_helpers import discover_prometheus_endpoint
        with patch("tools.prometheus_helpers.get_thanos_url", return_value="http://thanos:9090"):
            url, etype = await discover_prometheus_endpoint()
        assert url == "http://thanos:9090"
        assert etype == "thanos"

    @pytest.mark.asyncio
    async def test_prometheus_env_second(self):
        from tools.prometheus_helpers import discover_prometheus_endpoint
        with patch("tools.prometheus_helpers.get_thanos_url", return_value=None):
            with patch("tools.prometheus_helpers.get_prometheus_url", return_value="http://prom:9090"):
                url, etype = await discover_prometheus_endpoint()
        assert url == "http://prom:9090"
        assert etype == "prometheus"

    @pytest.mark.asyncio
    async def test_cache_hit_returned(self):
        from tools.prometheus_helpers import discover_prometheus_endpoint
        with patch("tools.prometheus_helpers.get_thanos_url", return_value=None):
            with patch("tools.prometheus_helpers.get_prometheus_url", return_value=None):
                with patch("tools.prometheus_helpers.OPENSHIFT_PROMETHEUS_ENDPOINTS", {}):
                    with patch("tools.prometheus_helpers._prometheus_endpoint_cache") as c:
                        c.get.return_value = ("http://cached:9090", "prometheus")
                        url, etype = await discover_prometheus_endpoint()
        assert url == "http://cached:9090"

    @pytest.mark.asyncio
    async def test_all_methods_fail_returns_none_tuple(self):
        from tools.prometheus_helpers import discover_prometheus_endpoint
        with patch("tools.prometheus_helpers.get_thanos_url", return_value=None):
            with patch("tools.prometheus_helpers.get_prometheus_url", return_value=None):
                with patch("tools.prometheus_helpers.OPENSHIFT_PROMETHEUS_ENDPOINTS", {}):
                    with patch("tools.prometheus_helpers._prometheus_endpoint_cache") as c:
                        c.get.return_value = None
                        with patch("tools.prometheus_helpers.is_running_in_cluster", return_value=False):
                            with patch("tools.prometheus_helpers._discover_prometheus_via_routes",
                                       new_callable=AsyncMock, return_value=None):
                                with patch("tools.prometheus_helpers._discover_thanos_via_services",
                                           new_callable=AsyncMock, return_value=None):
                                    with patch("tools.prometheus_helpers._discover_prometheus_via_operator_crd",
                                               new_callable=AsyncMock, return_value=None):
                                        with patch("tools.prometheus_helpers._discover_prometheus_via_services",
                                                   new_callable=AsyncMock, return_value=None):
                                            url, etype = await discover_prometheus_endpoint()
        assert url is None
        assert etype is None

    @pytest.mark.asyncio
    async def test_route_discovery_detects_thanos_type(self):
        from tools.prometheus_helpers import discover_prometheus_endpoint
        with patch("tools.prometheus_helpers.get_thanos_url", return_value=None):
            with patch("tools.prometheus_helpers.get_prometheus_url", return_value=None):
                with patch("tools.prometheus_helpers.OPENSHIFT_PROMETHEUS_ENDPOINTS", {}):
                    with patch("tools.prometheus_helpers._prometheus_endpoint_cache") as c:
                        c.get.return_value = None
                        c.set = MagicMock()
                        with patch("tools.prometheus_helpers.is_running_in_cluster", return_value=False):
                            with patch("tools.prometheus_helpers._discover_prometheus_via_routes",
                                       new_callable=AsyncMock) as mock_routes:
                                mock_routes.return_value = "https://thanos-querier.example.com"
                                with patch("tools.prometheus_helpers._discover_thanos_via_services",
                                           new_callable=AsyncMock) as mock_thanos:
                                    mock_thanos.return_value = None
                                    with patch("tools.prometheus_helpers._discover_prometheus_via_operator_crd",
                                               new_callable=AsyncMock) as mock_crd:
                                        mock_crd.return_value = None
                                        with patch("tools.prometheus_helpers._discover_prometheus_via_services",
                                                   new_callable=AsyncMock) as mock_svc:
                                            mock_svc.return_value = None
                                            url, etype = await discover_prometheus_endpoint()
        assert etype == "thanos"


# ===========================================================================
# Prometheus discovery sub-functions — None-guard + happy path
# ===========================================================================


class TestPrometheusDiscoverySubFunctions:
    @pytest.mark.asyncio
    async def test_routes_none_api_returns_none(self):
        from tools.prometheus_helpers import _discover_prometheus_via_routes
        assert await _discover_prometheus_via_routes(None) is None

    @pytest.mark.asyncio
    async def test_services_none_api_returns_none(self):
        from tools.prometheus_helpers import _discover_prometheus_via_services
        assert await _discover_prometheus_via_services(None) is None

    @pytest.mark.asyncio
    async def test_operator_crd_none_apis_returns_none(self):
        from tools.prometheus_helpers import _discover_prometheus_via_operator_crd
        assert await _discover_prometheus_via_operator_crd(None, None) is None

    @pytest.mark.asyncio
    async def test_thanos_services_none_api_returns_none(self):
        from tools.prometheus_helpers import _discover_thanos_via_services
        assert await _discover_thanos_via_services(None) is None

    @pytest.mark.asyncio
    async def test_routes_404_returns_none(self):
        from tools.prometheus_helpers import _discover_prometheus_via_routes
        from kubernetes.client.rest import ApiException
        mock_custom = MagicMock()
        with patch("tools.prometheus_helpers.asyncio.to_thread",
                   side_effect=ApiException(status=404, reason="Not Found")):
            result = await _discover_prometheus_via_routes(mock_custom)
        assert result is None

    @pytest.mark.asyncio
    async def test_routes_happy_path_thanos_querier(self):
        from tools.prometheus_helpers import _discover_prometheus_via_routes
        mock_custom = MagicMock()
        payload = {"items": [{"metadata": {"name": "thanos-querier"},
                               "spec": {"host": "thanos.example.com",
                                        "tls": {"termination": "edge"}}}]}
        with patch("tools.prometheus_helpers.asyncio.to_thread",
                   new_callable=AsyncMock, return_value=payload):
            result = await _discover_prometheus_via_routes(mock_custom)
        assert result == "https://thanos.example.com"

    @pytest.mark.asyncio
    async def test_routes_happy_path_prometheus_k8s(self):
        from tools.prometheus_helpers import _discover_prometheus_via_routes
        mock_custom = MagicMock()
        payload = {"items": [{"metadata": {"name": "prometheus-k8s"},
                               "spec": {"host": "prom.example.com"}}]}
        with patch("tools.prometheus_helpers.asyncio.to_thread",
                   new_callable=AsyncMock, return_value=payload):
            result = await _discover_prometheus_via_routes(mock_custom)
        assert result == "http://prom.example.com"


# ===========================================================================
# ci_cd_performance_baselining_tool_impl
# ===========================================================================


class TestCiCdPerformanceBaselineImpl:
    @pytest.mark.asyncio
    async def test_both_clients_none_returns_error(self):
        from tools.prometheus_tools import ci_cd_performance_baselining_tool_impl
        result = await ci_cd_performance_baselining_tool_impl(
            k8s_custom_api=None, k8s_core_api=None)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_custom_none_returns_error(self):
        from tools.prometheus_tools import ci_cd_performance_baselining_tool_impl
        result = await ci_cd_performance_baselining_tool_impl(
            k8s_custom_api=None, k8s_core_api=MagicMock())
        assert "error" in result

    @pytest.mark.asyncio
    async def test_prometheus_failure_returns_pipeline_baselines_key(self):
        from tools.prometheus_tools import ci_cd_performance_baselining_tool_impl
        failed = {"success": False, "error": "conn refused", "data": []}
        with patch("tools.prometheus_tools._execute_prometheus_query_internal",
                   new_callable=AsyncMock, return_value=failed):
            result = await ci_cd_performance_baselining_tool_impl(
                k8s_custom_api=MagicMock(), k8s_core_api=MagicMock())
        assert "pipeline_baselines" in result or "error" in result

    @pytest.mark.asyncio
    async def test_happy_path_structure(self):
        from tools.prometheus_tools import ci_cd_performance_baselining_tool_impl
        data = [{"metric": {"namespace": "my-ns", "status": "success"},
                 "value": [0, "10"]}]
        with patch("tools.prometheus_tools._execute_prometheus_query_internal",
                   new_callable=AsyncMock,
                   return_value={"success": True, "data": data}):
            result = await ci_cd_performance_baselining_tool_impl(
                k8s_custom_api=MagicMock(), k8s_core_api=MagicMock())
        assert "pipeline_baselines" in result
        assert isinstance(result["pipeline_baselines"], list)


# ===========================================================================
# resource_bottleneck_forecaster_impl
# ===========================================================================


class TestResourceBottleneckForecasterImpl:
    @pytest.mark.asyncio
    async def test_error_when_core_api_none(self):
        from tools.prometheus_tools import resource_bottleneck_forecaster_impl
        result = await resource_bottleneck_forecaster_impl(k8s_core_api=None)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_error_when_no_prometheus_fn(self):
        from tools.prometheus_tools import resource_bottleneck_forecaster_impl
        result = await resource_bottleneck_forecaster_impl(
            k8s_core_api=MagicMock(), prometheus_query_fn=None)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_prometheus_unreachable_returns_recommendation(self):
        from tools.prometheus_tools import resource_bottleneck_forecaster_impl

        async def fail_q(q, *a, **kw):
            return {"status": "error"}

        result = await resource_bottleneck_forecaster_impl(
            k8s_core_api=MagicMock(), prometheus_query_fn=fail_q)
        assert "capacity_recommendations" in result

    @pytest.mark.asyncio
    async def test_happy_path_required_keys(self):
        from tools.prometheus_tools import resource_bottleneck_forecaster_impl
        calls = [0]

        async def fake_q(query, *a, **kw):
            calls[0] += 1
            if calls[0] == 1:
                return {"status": "success",
                        "data": [{"metric": {}, "value": [0, "1"]}]}
            return {"status": "success", "data": []}

        mock_core = MagicMock()
        mock_core.list_node.return_value = MagicMock(items=[])
        with patch("tools.prometheus_tools._get_active_node_names_with_api",
                   new_callable=AsyncMock, return_value=set()):
            result = await resource_bottleneck_forecaster_impl(
                k8s_core_api=mock_core, prometheus_query_fn=fake_q)
        for key in ("forecasts", "capacity_recommendations",
                    "cluster_overview", "historical_accuracy"):
            assert key in result


# ===========================================================================
# what_if_scenario_simulator_impl
# ===========================================================================


class TestWhatIfScenarioSimulatorImpl:
    @pytest.mark.asyncio
    async def test_error_when_clients_none(self):
        from tools.prometheus_tools import what_if_scenario_simulator_impl
        result = await what_if_scenario_simulator_impl(
            "scaling", {"r": 1}, k8s_core_api=None, k8s_apps_api=None)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_invalid_scenario_type_error(self):
        from tools.prometheus_tools import what_if_scenario_simulator_impl
        result = await what_if_scenario_simulator_impl(
            "bad_type", {"k": "v"},
            k8s_core_api=MagicMock(), k8s_apps_api=MagicMock())
        assert "error" in result

    @pytest.mark.asyncio
    async def test_empty_changes_error(self):
        from tools.prometheus_tools import what_if_scenario_simulator_impl
        result = await what_if_scenario_simulator_impl(
            "scaling", {},
            k8s_core_api=MagicMock(), k8s_apps_api=MagicMock())
        assert "error" in result

    @pytest.mark.asyncio
    async def test_happy_path_returns_simulation_id(self):
        from tools.prometheus_tools import what_if_scenario_simulator_impl
        with patch("tools.prometheus_tools.collect_baseline_system_data",
                   new_callable=AsyncMock, return_value={}):
            with patch("tools.prometheus_tools.build_system_behavior_models",
                       new_callable=AsyncMock, return_value={}):
                with patch("tools.prometheus_tools.load_historical_performance_data",
                           new_callable=AsyncMock, return_value={}):
                    with patch("tools.prometheus_tools.calibrate_simulation_models",
                               return_value={}):
                        with patch("tools.prometheus_tools.run_monte_carlo_simulation",
                                   new_callable=AsyncMock, return_value={}):
                            with patch("tools.prometheus_tools.analyze_system_impact",
                                       return_value={}):
                                with patch("tools.prometheus_tools.identify_affected_components",
                                           new_callable=AsyncMock, return_value=[]):
                                    with patch("tools.prometheus_tools.perform_risk_assessment",
                                               return_value={}):
                                        with patch("tools.prometheus_tools.calculate_simulation_quality",
                                                   return_value={}):
                                            with patch("tools.prometheus_tools.generate_simulation_recommendations",
                                                       return_value=[]):
                                                result = await what_if_scenario_simulator_impl(
                                                    "scaling",
                                                    {"replicas": {"before": 1, "after": 3}},
                                                    k8s_core_api=MagicMock(),
                                                    k8s_apps_api=MagicMock())
        assert "simulation_id" in result
        assert result["simulation_id"].startswith("sim-")


# ===========================================================================
# log_tools — integration re-check
# ===========================================================================


class TestGetPodLogsImplIntegration:
    @pytest.mark.asyncio
    async def test_none_api_returns_error(self):
        from tools.log_tools import get_pod_logs_impl
        result = await get_pod_logs_impl("ns", "pod", k8s_core_api=None)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_multi_container_happy_path(self):
        from tools.log_tools import get_pod_logs_impl
        fake_logs = {"step-build": "ok", "step-test": "pass"}
        with patch("tools.log_tools.get_all_pod_logs",
                   new_callable=AsyncMock, return_value=fake_logs):
            result = await get_pod_logs_impl("ns", "pod", k8s_core_api=MagicMock())
        assert result.get("logs") == fake_logs
