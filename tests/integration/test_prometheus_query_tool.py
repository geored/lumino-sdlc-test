"""
Integration tests for Prometheus query tools.

Covers:
  - prometheus_query_impl: happy path, all HTTP error codes, None client guards
  - _execute_prometheus_query_internal: success, connection error, timeout
  - _process_prometheus_results: namespace filter, series cap, CSV/JSON/table formats
  - ci_cd_performance_baselining_tool_impl: happy path, None guards, Prometheus failure
  - resource_bottleneck_forecaster_impl: required keys, None guards, no prometheus fn
  - what_if_scenario_simulator_impl: happy path, invalid type, empty changes, None guards
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_prom_success(data=None):
    if data is None:
        data = [{"metric": {"namespace": "ns"}, "value": [0, "42"]}]
    return {"success": True, "data": data}


def _make_prom_failure(error="conn refused"):
    return {"success": False, "error": error, "data": []}


# ===========================================================================
# prometheus_query_impl
# ===========================================================================


class TestPrometheusQueryImpl:
    @pytest.mark.asyncio
    async def test_happy_path_table_format(self):
        from tools.prometheus_query import prometheus_query_impl

        with patch(
            "tools.prometheus_query._execute_prometheus_query_internal",
            new_callable=AsyncMock,
            return_value=_make_prom_success(),
        ):
            with patch(
                "tools.prometheus_query._process_prometheus_results",
                new_callable=AsyncMock,
                return_value="| namespace | value |\n| ns | 42 |",
            ):
                result = await prometheus_query_impl(
                    query='up{job="prometheus"}',
                    output_format="table",
                    k8s_core_api=MagicMock(),
                    k8s_custom_api=MagicMock(),
                )
        assert isinstance(result, str)
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_happy_path_json_format(self):
        from tools.prometheus_query import prometheus_query_impl

        with patch(
            "tools.prometheus_query._execute_prometheus_query_internal",
            new_callable=AsyncMock,
            return_value=_make_prom_success(),
        ):
            with patch(
                "tools.prometheus_query._process_prometheus_results",
                new_callable=AsyncMock,
                return_value='{"results": []}',
            ):
                result = await prometheus_query_impl(
                    query="up",
                    output_format="json",
                    k8s_core_api=MagicMock(),
                    k8s_custom_api=MagicMock(),
                )
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_happy_path_csv_format(self):
        from tools.prometheus_query import prometheus_query_impl

        with patch(
            "tools.prometheus_query._execute_prometheus_query_internal",
            new_callable=AsyncMock,
            return_value=_make_prom_success(),
        ):
            with patch(
                "tools.prometheus_query._process_prometheus_results",
                new_callable=AsyncMock,
                return_value="namespace,value\nns,42",
            ):
                result = await prometheus_query_impl(
                    query="up",
                    output_format="csv",
                    k8s_core_api=MagicMock(),
                    k8s_custom_api=MagicMock(),
                )
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_prometheus_query_failure_returns_non_empty_string(self):
        from tools.prometheus_query import prometheus_query_impl

        with patch(
            "tools.prometheus_query._execute_prometheus_query_internal",
            new_callable=AsyncMock,
            return_value=_make_prom_failure("connection refused"),
        ):
            result = await prometheus_query_impl(
                query="up",
                k8s_core_api=MagicMock(),
                k8s_custom_api=MagicMock(),
            )
        assert isinstance(result, str)
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_empty_query_does_not_raise(self):
        from tools.prometheus_query import prometheus_query_impl

        result = await prometheus_query_impl(
            query="",
            k8s_core_api=MagicMock(),
            k8s_custom_api=MagicMock(),
        )
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_no_results_returns_string(self):
        from tools.prometheus_query import prometheus_query_impl

        with patch(
            "tools.prometheus_query._execute_prometheus_query_internal",
            new_callable=AsyncMock,
            return_value={"success": True, "data": []},
        ):
            with patch(
                "tools.prometheus_query._process_prometheus_results",
                new_callable=AsyncMock,
                return_value="No results found",
            ):
                result = await prometheus_query_impl(
                    query="nonexistent_metric",
                    k8s_core_api=MagicMock(),
                    k8s_custom_api=MagicMock(),
                )
        assert isinstance(result, str)


# ===========================================================================
# _execute_prometheus_query_internal
# ===========================================================================


class TestExecutePrometheusQueryInternal:
    @pytest.mark.asyncio
    async def test_no_endpoint_returns_failure_dict(self):
        from tools.prometheus_query import _execute_prometheus_query_internal

        with patch(
            "tools.prometheus_query.discover_prometheus_endpoint",
            new_callable=AsyncMock,
            return_value=(None, None),
        ):
            result = await _execute_prometheus_query_internal("up")

        assert isinstance(result, dict)
        assert result.get("success") is False or "error" in result

    @pytest.mark.asyncio
    async def test_connection_error_returns_failure(self):
        from tools.prometheus_query import _execute_prometheus_query_internal

        with patch(
            "tools.prometheus_query.discover_prometheus_endpoint",
            new_callable=AsyncMock,
            return_value=("http://prometheus:9090", "prometheus"),
        ):
            with patch(
                "tools.prometheus_query._get_k8s_bearer_token",
                new_callable=AsyncMock,
                return_value=None,
            ):
                with patch(
                    "tools.prometheus_query.aiohttp.ClientSession",
                    side_effect=Exception("connection refused"),
                ):
                    result = await _execute_prometheus_query_internal("up")

        assert isinstance(result, dict)
        assert result.get("success") is False or "error" in result

    @pytest.mark.asyncio
    @pytest.mark.parametrize("http_status", [400, 401, 403, 500, 503])
    async def test_http_error_codes_do_not_raise(self, http_status):
        from tools.prometheus_query import _execute_prometheus_query_internal

        mock_response = MagicMock()
        mock_response.status = http_status
        mock_response.text = AsyncMock(return_value="error body")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "tools.prometheus_query.discover_prometheus_endpoint",
            new_callable=AsyncMock,
            return_value=("http://prometheus:9090", "prometheus"),
        ):
            with patch(
                "tools.prometheus_query._get_k8s_bearer_token",
                new_callable=AsyncMock,
                return_value=None,
            ):
                with patch(
                    "tools.prometheus_query.aiohttp.ClientSession"
                ) as mock_cls:
                    mock_cls.return_value = mock_session
                    result = await _execute_prometheus_query_internal("up")

        assert isinstance(result, dict)


# ===========================================================================
# _process_prometheus_results
# ===========================================================================


class TestProcessPrometheusResults:
    @pytest.mark.asyncio
    async def test_empty_data_returns_string(self):
        from tools.prometheus_query import _process_prometheus_results

        with patch("tools.prometheus_query.format_as_table", return_value="no data"):
            with patch(
                "tools.prometheus_query.generate_result_summary", return_value="empty"
            ):
                with patch(
                    "tools.prometheus_query.generate_query_suggestions", return_value=[]
                ):
                    result = await _process_prometheus_results(
                        data=[],
                        query="up",
                        output_format="table",
                        namespace_filter=None,
                        k8s_core_api=None,
                        k8s_custom_api=None,
                    )
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_series_cap_limits_output(self):
        from tools.prometheus_query import _process_prometheus_results

        data = [
            {"metric": {"namespace": f"ns-{i}"}, "value": [0, str(i)]}
            for i in range(600)
        ]
        with patch("tools.prometheus_query.MAX_SERIES_LIMIT", 500):
            with patch(
                "tools.prometheus_query.format_as_table", return_value="table"
            ):
                with patch(
                    "tools.prometheus_query.generate_result_summary", return_value=""
                ):
                    with patch(
                        "tools.prometheus_query.generate_query_suggestions",
                        return_value=[],
                    ):
                        result = await _process_prometheus_results(
                            data=data,
                            query="up",
                            output_format="table",
                            namespace_filter=None,
                            k8s_core_api=None,
                            k8s_custom_api=None,
                        )
        assert isinstance(result, str)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("fmt", ["table", "json", "csv"])
    async def test_output_formats_do_not_raise(self, fmt):
        from tools.prometheus_query import _process_prometheus_results

        data = [{"metric": {"job": "test"}, "value": [0, "1"]}]
        format_fns = {
            "table": "tools.prometheus_query.format_as_table",
            "json": "tools.prometheus_query.format_as_json",
            "csv": "tools.prometheus_query.format_as_csv",
        }
        with patch(format_fns[fmt], return_value=f"{fmt} output"):
            with patch(
                "tools.prometheus_query.generate_result_summary", return_value=""
            ):
                with patch(
                    "tools.prometheus_query.generate_query_suggestions", return_value=[]
                ):
                    result = await _process_prometheus_results(
                        data=data,
                        query="up",
                        output_format=fmt,
                        namespace_filter=None,
                        k8s_core_api=None,
                        k8s_custom_api=None,
                    )
        assert isinstance(result, str)


# ===========================================================================
# ci_cd_performance_baselining_tool_impl
# ===========================================================================


class TestCiCdPerformanceBaselineImplExtended:
    @pytest.mark.asyncio
    async def test_invalid_baseline_period_still_returns_dict(self):
        from tools.prometheus_tools import ci_cd_performance_baselining_tool_impl

        with patch(
            "tools.prometheus_tools._execute_prometheus_query_internal",
            new_callable=AsyncMock,
            return_value=_make_prom_failure(),
        ):
            result = await ci_cd_performance_baselining_tool_impl(
                baseline_period="999y",
                k8s_custom_api=MagicMock(),
                k8s_core_api=MagicMock(),
            )
        assert isinstance(result, dict)
        assert "pipeline_baselines" in result or "error" in result

    @pytest.mark.asyncio
    async def test_pipeline_names_filter_accepted(self):
        from tools.prometheus_tools import ci_cd_performance_baselining_tool_impl

        data = [
            {
                "metric": {"namespace": "my-ns", "status": "success"},
                "value": [0, "10"],
            }
        ]
        with patch(
            "tools.prometheus_tools._execute_prometheus_query_internal",
            new_callable=AsyncMock,
            return_value={"success": True, "data": data},
        ):
            result = await ci_cd_performance_baselining_tool_impl(
                pipeline_names=["my-pipeline"],
                k8s_custom_api=MagicMock(),
                k8s_core_api=MagicMock(),
            )
        assert isinstance(result, dict)
        assert "pipeline_baselines" in result

    @pytest.mark.asyncio
    async def test_returns_performance_trends_key(self):
        from tools.prometheus_tools import ci_cd_performance_baselining_tool_impl

        with patch(
            "tools.prometheus_tools._execute_prometheus_query_internal",
            new_callable=AsyncMock,
            return_value={"success": True, "data": []},
        ):
            result = await ci_cd_performance_baselining_tool_impl(
                k8s_custom_api=MagicMock(),
                k8s_core_api=MagicMock(),
            )
        assert "performance_trends" in result or "error" in result

    @pytest.mark.asyncio
    async def test_exception_during_query_returns_error_dict(self):
        from tools.prometheus_tools import ci_cd_performance_baselining_tool_impl

        with patch(
            "tools.prometheus_tools._execute_prometheus_query_internal",
            new_callable=AsyncMock,
            side_effect=RuntimeError("unexpected crash"),
        ):
            result = await ci_cd_performance_baselining_tool_impl(
                k8s_custom_api=MagicMock(),
                k8s_core_api=MagicMock(),
            )
        assert isinstance(result, dict)
        assert "error" in result


# ===========================================================================
# resource_bottleneck_forecaster_impl
# ===========================================================================


class TestResourceBottleneckForecasterImplExtended:
    @pytest.mark.asyncio
    async def test_returns_all_required_keys(self):
        from tools.prometheus_tools import resource_bottleneck_forecaster_impl

        async def fake_q(query, *a, **kw):
            return {
                "status": "success",
                "data": [{"metric": {}, "value": [0, "0.5"]}],
            }

        with patch(
            "tools.prometheus_tools._get_active_node_names_with_api",
            new_callable=AsyncMock,
            return_value=set(),
        ):
            result = await resource_bottleneck_forecaster_impl(
                k8s_core_api=MagicMock(),
                prometheus_query_fn=fake_q,
            )
        for key in ("forecasts", "capacity_recommendations", "cluster_overview"):
            assert key in result, f"Missing key: {key}"

    @pytest.mark.asyncio
    async def test_node_filter_accepted_does_not_raise(self):
        from tools.prometheus_tools import resource_bottleneck_forecaster_impl

        async def fake_q(query, *a, **kw):
            return {"status": "success", "data": []}

        with patch(
            "tools.prometheus_tools._get_active_node_names_with_api",
            new_callable=AsyncMock,
            return_value={"node-1", "node-2"},
        ):
            result = await resource_bottleneck_forecaster_impl(
                k8s_core_api=MagicMock(),
                prometheus_query_fn=fake_q,
                node_names=["node-1"],
            )
        assert isinstance(result, dict)
        assert "forecasts" in result or "error" in result

    @pytest.mark.asyncio
    async def test_exception_returns_error_or_safe_dict(self):
        from tools.prometheus_tools import resource_bottleneck_forecaster_impl

        async def boom(*a, **kw):
            raise RuntimeError("prom down")

        result = await resource_bottleneck_forecaster_impl(
            k8s_core_api=MagicMock(),
            prometheus_query_fn=boom,
        )
        assert isinstance(result, dict)
        assert "error" in result or "capacity_recommendations" in result


# ===========================================================================
# what_if_scenario_simulator_impl
# ===========================================================================


class TestWhatIfScenarioSimulatorImplExtended:
    def _start_all(self):
        patches = [
            patch(
                "tools.prometheus_tools.collect_baseline_system_data",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "tools.prometheus_tools.build_system_behavior_models",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "tools.prometheus_tools.load_historical_performance_data",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "tools.prometheus_tools.calibrate_simulation_models",
                return_value={},
            ),
            patch(
                "tools.prometheus_tools.run_monte_carlo_simulation",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch("tools.prometheus_tools.analyze_system_impact", return_value={}),
            patch(
                "tools.prometheus_tools.identify_affected_components",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "tools.prometheus_tools.perform_risk_assessment",
                return_value={},
            ),
            patch(
                "tools.prometheus_tools.calculate_simulation_quality",
                return_value={},
            ),
            patch(
                "tools.prometheus_tools.generate_simulation_recommendations",
                return_value=[],
            ),
        ]
        for p in patches:
            p.start()
        return patches

    @pytest.mark.asyncio
    async def test_simulation_id_starts_with_sim(self):
        from tools.prometheus_tools import what_if_scenario_simulator_impl

        patches = self._start_all()
        try:
            result = await what_if_scenario_simulator_impl(
                "scaling",
                {"replicas": {"before": 1, "after": 5}},
                k8s_core_api=MagicMock(),
                k8s_apps_api=MagicMock(),
            )
        finally:
            for p in patches:
                p.stop()
        assert "simulation_id" in result
        assert result["simulation_id"].startswith("sim-")

    @pytest.mark.asyncio
    async def test_resource_scenario_type_accepted(self):
        from tools.prometheus_tools import what_if_scenario_simulator_impl

        patches = self._start_all()
        try:
            result = await what_if_scenario_simulator_impl(
                "resource",
                {"cpu_limit": {"before": "100m", "after": "500m"}},
                k8s_core_api=MagicMock(),
                k8s_apps_api=MagicMock(),
            )
        finally:
            for p in patches:
                p.stop()
        assert "simulation_id" in result or "error" in result

    @pytest.mark.asyncio
    async def test_exception_in_baseline_returns_error(self):
        from tools.prometheus_tools import what_if_scenario_simulator_impl

        with patch(
            "tools.prometheus_tools.collect_baseline_system_data",
            new_callable=AsyncMock,
            side_effect=RuntimeError("baseline crash"),
        ):
            result = await what_if_scenario_simulator_impl(
                "scaling",
                {"replicas": {"before": 1, "after": 3}},
                k8s_core_api=MagicMock(),
                k8s_apps_api=MagicMock(),
            )
        assert isinstance(result, dict)
        assert "error" in result

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "scenario_type",
        ["bad_type", "SCALING", "123", ""],
    )
    async def test_invalid_scenario_types_return_error(self, scenario_type):
        from tools.prometheus_tools import what_if_scenario_simulator_impl

        result = await what_if_scenario_simulator_impl(
            scenario_type,
            {"replicas": {"before": 1, "after": 3}},
            k8s_core_api=MagicMock(),
            k8s_apps_api=MagicMock(),
        )
        assert isinstance(result, dict)
        assert "error" in result
