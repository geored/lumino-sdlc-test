"""
Integration tests for ApiException handling across tool implementations.

Verifies that 401/403/404/410 ApiException responses from Kubernetes API
calls are caught and returned as error dicts/strings, never propagated.

Fixes #154
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _api_exc(status, reason=""):
    """Create an ApiException matching common K8s error codes."""
    reasons = {401: "Unauthorized", 403: "Forbidden", 404: "Not Found", 410: "Gone"}
    return ApiException(status=status, reason=reason or reasons.get(status, "Error"))


# ===========================================================================
# list_namespaces_impl — 401, 403, 404, 410
# ===========================================================================


class TestListNamespacesApiExceptions:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [401, 403, 404, 410])
    async def test_returns_empty_list_on_api_error(self, status):
        from tools.kubernetes_tools import list_namespaces_impl

        mock_api = MagicMock()
        with patch(
            "tools.kubernetes_tools._namespace_cache",
            {"namespaces": None, "timestamp": 0},
        ):
            with patch(
                "tools.kubernetes_tools.asyncio.to_thread",
                side_effect=_api_exc(status),
            ):
                result = await list_namespaces_impl(mock_api)
        assert isinstance(result, list)
        assert result == []


# ===========================================================================
# list_pods_in_namespace_impl — 401, 403, 404, 410
# ===========================================================================


class TestListPodsApiExceptions:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [401, 403, 404, 410])
    async def test_returns_error_dict_on_api_error(self, status):
        from tools.kubernetes_tools import list_pods_in_namespace_impl

        mock_api = MagicMock()
        with patch(
            "tools.kubernetes_tools.asyncio.to_thread",
            side_effect=_api_exc(status),
        ):
            result = await list_pods_in_namespace_impl("default", mock_api)
        assert isinstance(result, list)
        assert len(result) >= 1
        assert "error" in result[0]


# ===========================================================================
# get_kubernetes_resource_impl — 401, 403, 404, 410
# ===========================================================================


class TestGetKubernetesResourceApiExceptions:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [401, 403, 404, 410])
    async def test_returns_error_string_on_api_error(self, status):
        from tools.kubernetes_tools import get_kubernetes_resource_impl

        mock_api = MagicMock()
        with patch(
            "tools.kubernetes_tools.asyncio.to_thread",
            side_effect=_api_exc(status),
        ):
            result = await get_kubernetes_resource_impl(
                "pod", "my-pod", "default", k8s_core_api=mock_api
            )
        assert isinstance(result, str)
        # Should contain error info, not raise
        assert "error" in result.lower() or "Error" in result or "not found" in result.lower()


# ===========================================================================
# check_resource_constraints_impl — 403
# ===========================================================================


class TestCheckResourceConstraintsApiExceptions:
    @pytest.mark.asyncio
    async def test_403_returns_error(self):
        from tools.kubernetes_tools import check_resource_constraints_impl

        mock_api = MagicMock()

        async def raise_403(*a, **kw):
            raise _api_exc(403)

        with patch("tools.kubernetes_tools.list_pods", raise_403):
            result = await check_resource_constraints_impl("default", mock_api)
        assert isinstance(result, dict)
        assert "error" in result


# ===========================================================================
# get_pipelinerun_logs_impl — 403, 404, 410
# ===========================================================================


class TestGetPipelinerunLogsApiExceptions:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [403, 404, 410])
    async def test_returns_error_dict_on_api_error(self, status):
        from tools.tekton_tools import get_pipelinerun_logs_impl

        with patch(
            "tools.tekton_tools.asyncio.to_thread",
            side_effect=_api_exc(status),
        ):
            result = await get_pipelinerun_logs_impl(
                "my-pr", "ns",
                k8s_core_api=MagicMock(),
                k8s_custom_api=MagicMock(),
                adaptive_processor_cls=MagicMock(),
                prioritize_pods_fn=AsyncMock(),
                estimate_tokens_fn=AsyncMock(),
                calculate_tail_lines_fn=MagicMock(),
                truncate_logs_fn=MagicMock(),
            )
        assert isinstance(result, dict)
        assert "error" in result


# ===========================================================================
# get_pod_logs_impl — 403, 404
# ===========================================================================


class TestGetPodLogsApiExceptions:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [403, 404])
    async def test_returns_error_dict_on_api_error(self, status):
        from tools.log_tools import get_pod_logs_impl

        with patch(
            "tools.log_tools.get_all_pod_logs",
            new_callable=AsyncMock,
            side_effect=_api_exc(status),
        ):
            result = await get_pod_logs_impl("ns", "pod", k8s_core_api=MagicMock())
        assert isinstance(result, dict)
        assert "error" in result


# ===========================================================================
# smart_get_namespace_events_impl — 403, 404
# ===========================================================================


class TestSmartGetNamespaceEventsApiExceptions:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [403, 404])
    async def test_returns_error_dict_on_api_error(self, status):
        from tools.event_rca_tools import smart_get_namespace_events_impl

        mock_api = MagicMock()
        with patch(
            "tools.event_rca_tools._get_namespace_events_internal",
            new_callable=AsyncMock,
            side_effect=_api_exc(status),
        ):
            result = await smart_get_namespace_events_impl(
                "default", k8s_core_api=mock_api
            )
        assert isinstance(result, dict)
        assert "error" in result


# ===========================================================================
# progressive_event_analysis_impl — 403
# ===========================================================================


class TestProgressiveEventAnalysisApiExceptions:
    @pytest.mark.asyncio
    async def test_403_returns_error_dict(self):
        from tools.event_rca_tools import progressive_event_analysis_impl

        mock_api = MagicMock()
        with patch(
            "tools.event_rca_tools._get_namespace_events_as_dicts",
            new_callable=AsyncMock,
            side_effect=_api_exc(403),
        ):
            result = await progressive_event_analysis_impl(
                "default", k8s_core_api=mock_api
            )
        assert isinstance(result, dict)
        assert "error" in result


# ===========================================================================
# Prometheus discovery sub-functions — 404, 403
# ===========================================================================


class TestPrometheusDiscoveryApiExceptions:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [403, 404])
    async def test_routes_returns_none_on_api_error(self, status):
        from tools.prometheus_helpers import _discover_prometheus_via_routes

        with patch(
            "tools.prometheus_helpers.asyncio.to_thread",
            side_effect=_api_exc(status),
        ):
            result = await _discover_prometheus_via_routes(MagicMock())
        assert result is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [403, 404])
    async def test_services_returns_none_on_api_error(self, status):
        from tools.prometheus_helpers import _discover_prometheus_via_services

        with patch(
            "tools.prometheus_helpers.asyncio.to_thread",
            side_effect=_api_exc(status),
        ):
            result = await _discover_prometheus_via_services(MagicMock())
        assert result is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [403, 404])
    async def test_thanos_services_returns_none_on_api_error(self, status):
        from tools.prometheus_helpers import _discover_thanos_via_services

        with patch(
            "tools.prometheus_helpers.asyncio.to_thread",
            side_effect=_api_exc(status),
        ):
            result = await _discover_thanos_via_services(MagicMock())
        assert result is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [403, 404])
    async def test_operator_crd_returns_none_on_api_error(self, status):
        from tools.prometheus_helpers import _discover_prometheus_via_operator_crd

        with patch(
            "tools.prometheus_helpers.asyncio.to_thread",
            side_effect=_api_exc(status),
        ):
            result = await _discover_prometheus_via_operator_crd(
                MagicMock(), MagicMock()
            )
        assert result is None


# ===========================================================================
# None-client guards (complementary to test_none_k8s_clients.py)
# ===========================================================================


class TestNoneClientGuards:
    @pytest.mark.asyncio
    async def test_list_namespaces_none_returns_empty(self):
        from tools.kubernetes_tools import list_namespaces_impl

        result = await list_namespaces_impl(None)
        assert result == []

    @pytest.mark.asyncio
    async def test_list_pods_none_returns_error(self):
        from tools.kubernetes_tools import list_pods_in_namespace_impl

        result = await list_pods_in_namespace_impl("default", None)
        assert isinstance(result, list)
        assert "error" in result[0]

    @pytest.mark.asyncio
    async def test_get_resource_none_returns_error_string(self):
        from tools.kubernetes_tools import get_kubernetes_resource_impl

        result = await get_kubernetes_resource_impl(
            "pod", "my-pod", "default", k8s_core_api=None
        )
        assert "Error" in result

    @pytest.mark.asyncio
    async def test_pipelinerun_logs_none_core_api(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl

        result = await get_pipelinerun_logs_impl(
            "pr", "ns",
            k8s_core_api=None,
            k8s_custom_api=MagicMock(),
            adaptive_processor_cls=MagicMock(),
            prioritize_pods_fn=AsyncMock(),
            estimate_tokens_fn=AsyncMock(),
            calculate_tail_lines_fn=MagicMock(),
            truncate_logs_fn=MagicMock(),
        )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_pipelinerun_logs_none_custom_api(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl

        result = await get_pipelinerun_logs_impl(
            "pr", "ns",
            k8s_core_api=MagicMock(),
            k8s_custom_api=None,
            adaptive_processor_cls=MagicMock(),
            prioritize_pods_fn=AsyncMock(),
            estimate_tokens_fn=AsyncMock(),
            calculate_tail_lines_fn=MagicMock(),
            truncate_logs_fn=MagicMock(),
        )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_get_pod_logs_none_api(self):
        from tools.log_tools import get_pod_logs_impl

        result = await get_pod_logs_impl("ns", "pod", k8s_core_api=None)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_check_constraints_none_api(self):
        from tools.kubernetes_tools import check_resource_constraints_impl

        result = await check_resource_constraints_impl("default", None)
        assert "error" in result
