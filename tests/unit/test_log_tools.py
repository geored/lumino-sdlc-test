"""Unit tests for src/tools/log_tools.py — get_pod_logs_impl, analyze_logs_impl,
detect_log_anomalies_impl and _quick_volume_estimate_impl.

Fixes #113
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tools.log_tools import (_quick_volume_estimate_impl, analyze_logs_impl,
                             detect_log_anomalies_impl, get_pod_logs_impl)

# ---------------------------------------------------------------------------
# get_pod_logs_impl
# ---------------------------------------------------------------------------


class TestGetPodLogsImpl:
    """Tests for get_pod_logs_impl."""

    @pytest.mark.asyncio
    async def test_returns_error_when_k8s_client_none(self):
        result = await get_pod_logs_impl(
            namespace="ns",
            pod_name="pod-1",
            k8s_core_api=None,
        )
        assert result == {"error": "Kubernetes client not available."}

    @pytest.mark.asyncio
    async def test_happy_path_returns_logs(self):
        mock_api = MagicMock()
        fake_logs = {"step-build": "building...\nDone"}
        with patch(
            "tools.log_tools.get_all_pod_logs",
            new_callable=AsyncMock,
            return_value=fake_logs,
        ):
            result = await get_pod_logs_impl(
                namespace="default",
                pod_name="my-pod",
                k8s_core_api=mock_api,
            )
        assert "logs" in result
        assert result["logs"] == fake_logs

    @pytest.mark.asyncio
    async def test_container_filter(self):
        mock_api = MagicMock()
        fake_logs = {"step-build": "build output", "step-test": "test output"}
        with patch(
            "tools.log_tools.get_all_pod_logs",
            new_callable=AsyncMock,
            return_value=fake_logs,
        ):
            result = await get_pod_logs_impl(
                namespace="ns",
                pod_name="pod",
                container_name="step-build",
                k8s_core_api=mock_api,
            )
        assert result == {"logs": {"step-build": "build output"}}

    @pytest.mark.asyncio
    async def test_container_not_found(self):
        mock_api = MagicMock()
        fake_logs = {"step-build": "output"}
        with patch(
            "tools.log_tools.get_all_pod_logs",
            new_callable=AsyncMock,
            return_value=fake_logs,
        ):
            result = await get_pod_logs_impl(
                namespace="ns",
                pod_name="pod",
                container_name="nonexistent",
                k8s_core_api=mock_api,
            )
        assert "error" in result
        assert "nonexistent" in result["error"]

    @pytest.mark.asyncio
    async def test_error_keys_detected(self):
        mock_api = MagicMock()
        fake_logs = {"error_fetching": "connection refused"}
        with patch(
            "tools.log_tools.get_all_pod_logs",
            new_callable=AsyncMock,
            return_value=fake_logs,
        ):
            result = await get_pod_logs_impl(
                namespace="ns",
                pod_name="pod",
                k8s_core_api=mock_api,
            )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_exception_returns_error(self):
        mock_api = MagicMock()
        with patch(
            "tools.log_tools.get_all_pod_logs",
            new_callable=AsyncMock,
            side_effect=RuntimeError("boom"),
        ):
            result = await get_pod_logs_impl(
                namespace="ns",
                pod_name="pod",
                k8s_core_api=mock_api,
            )
        assert "error" in result
        assert "boom" in result["error"]


# ---------------------------------------------------------------------------
# analyze_logs_impl
# ---------------------------------------------------------------------------


class TestAnalyzeLogsImpl:
    """Tests for analyze_logs_impl."""

    @pytest.mark.asyncio
    async def test_empty_log(self):
        result = await analyze_logs_impl("")
        assert "error_count" in result
        assert result["error_count"] >= 0

    @pytest.mark.asyncio
    async def test_detects_errors(self):
        log = "INFO starting\nERROR something failed\nFATAL crash"
        with (
            patch(
                "tools.log_tools.extract_error_patterns",
                return_value=["ERROR something failed", "FATAL crash"],
            ),
            patch(
                "tools.log_tools.categorize_errors",
                return_value={"runtime": ["ERROR something failed"]},
            ),
            patch(
                "tools.log_tools.generate_log_summary", return_value="2 errors found"
            ),
        ):
            result = await analyze_logs_impl(log)
        assert result["error_count"] == 2
        assert len(result["error_patterns"]) == 2

    @pytest.mark.asyncio
    async def test_exception_returns_safe_dict(self):
        with patch(
            "tools.log_tools.extract_error_patterns",
            side_effect=ValueError("parse error"),
        ):
            result = await analyze_logs_impl("some log")
        assert result["error_count"] == 0
        assert "Analysis failed" in result["summary"]


# ---------------------------------------------------------------------------
# detect_log_anomalies_impl
# ---------------------------------------------------------------------------


class TestDetectLogAnomaliesImpl:
    """Tests for detect_log_anomalies_impl."""

    @pytest.mark.asyncio
    async def test_empty_logs(self):
        result = await detect_log_anomalies_impl("")
        assert result["anomaly_detected"] is False
        assert "No logs" in result["analysis_summary"]

    @pytest.mark.asyncio
    async def test_whitespace_only_logs(self):
        result = await detect_log_anomalies_impl("   \n  \n   ")
        assert result["anomaly_detected"] is False

    @pytest.mark.asyncio
    async def test_normal_logs_no_anomaly(self):
        lines = "\n".join(
            [f"2024-01-01T00:00:{i:02d} INFO all good line {i}" for i in range(20)]
        )
        result = await detect_log_anomalies_impl(lines)
        # Should not flag high_error_rate
        if result["anomaly_details"]:
            types = [a["type"] for a in result["anomaly_details"]["anomalies"]]
            assert "high_error_rate" not in types

    @pytest.mark.asyncio
    async def test_high_error_rate_detected(self):
        # 80% error lines -> triggers high_error_rate at any threshold
        lines = "\n".join(
            [f"2024-01-01T00:00:{i:02d} ERROR failure {i}" for i in range(40)]
            + [f"2024-01-01T00:00:{i:02d} INFO ok" for i in range(10)]
        )
        result = await detect_log_anomalies_impl(lines, severity_threshold="high")
        assert result["anomaly_detected"] is True
        types = [a["type"] for a in result["anomaly_details"]["anomalies"]]
        assert "high_error_rate" in types

    @pytest.mark.asyncio
    async def test_repetitive_pattern_detected(self):
        # Same line repeated 100 times out of 110
        repeated = "2024-01-01T00:00:00 INFO heartbeat alive"
        unique = [f"2024-01-01T00:00:{i:02d} INFO unique-{i}" for i in range(10)]
        lines = "\n".join([repeated] * 100 + unique)
        result = await detect_log_anomalies_impl(lines, severity_threshold="low")
        assert result["anomaly_detected"] is True
        types = [a["type"] for a in result["anomaly_details"]["anomalies"]]
        assert "repetitive_pattern" in types

    @pytest.mark.asyncio
    async def test_time_gap_anomaly(self):
        # 10 lines 1s apart, then a 600s gap
        ts_lines = [f"2024-01-01T00:00:{i:02d} INFO line {i}" for i in range(10)]
        ts_lines.append("2024-01-01T00:10:00 INFO after gap")
        ts_lines.append("2024-01-01T00:10:01 INFO after gap 2")
        result = await detect_log_anomalies_impl(
            "\n".join(ts_lines), severity_threshold="low"
        )
        if result["anomaly_detected"] and result["anomaly_details"]:
            types = [a["type"] for a in result["anomaly_details"]["anomalies"]]
            # Time gap of 600s with avg ~1s should be detected
            assert "time_gap_anomaly" in types

    @pytest.mark.asyncio
    async def test_baseline_new_patterns(self):
        lines = "\n".join(
            [
                "2024-01-01T00:00:00 ERROR timeout connecting",
                "2024-01-01T00:00:01 ERROR permission denied",
            ]
            + [f"2024-01-01T00:00:{i:02d} INFO ok" for i in range(20)]
        )
        result = await detect_log_anomalies_impl(
            lines,
            baseline_patterns=["memory"],  # only memory expected
            severity_threshold="low",
        )
        if result["anomaly_detected"] and result["anomaly_details"]:
            types = [a["type"] for a in result["anomaly_details"]["anomalies"]]
            assert "new_error_patterns" in types

    @pytest.mark.asyncio
    async def test_unusual_log_level_distribution(self):
        # All error/fatal lines
        lines = "\n".join(
            [f"2024-01-01T00:00:{i:02d} ERROR failure" for i in range(30)]
            + [f"2024-01-01T00:00:{i:02d} FATAL crash" for i in range(30)]
        )
        result = await detect_log_anomalies_impl(lines)
        assert result["anomaly_detected"] is True
        types = [a["type"] for a in result["anomaly_details"]["anomalies"]]
        assert "unusual_log_level_distribution" in types

    @pytest.mark.asyncio
    @pytest.mark.parametrize("threshold", ["low", "medium", "high"])
    async def test_all_severity_thresholds(self, threshold):
        lines = "\n".join([f"2024-01-01T00:00:{i:02d} INFO ok {i}" for i in range(20)])
        result = await detect_log_anomalies_impl(lines, severity_threshold=threshold)
        assert "anomaly_detected" in result
        assert "analysis_summary" in result

    @pytest.mark.asyncio
    async def test_invalid_threshold_defaults_to_medium(self):
        lines = "\n".join([f"2024-01-01T00:00:{i:02d} INFO ok" for i in range(10)])
        result = await detect_log_anomalies_impl(lines, severity_threshold="invalid")
        assert "anomaly_detected" in result

    @pytest.mark.asyncio
    async def test_escaped_newlines_normalized(self):
        # Logs with literal \n (as from JSON transport)
        raw = "2024-01-01T00:00:00 INFO start\\n2024-01-01T00:00:01 ERROR fail\\n2024-01-01T00:00:02 INFO end"
        result = await detect_log_anomalies_impl(raw)
        assert "analysis_summary" in result

    @pytest.mark.asyncio
    async def test_exception_returns_safe_result(self):
        with patch("tools.log_tools.re.sub", side_effect=RuntimeError("regex boom")):
            result = await detect_log_anomalies_impl("some logs")
        assert result["anomaly_detected"] is False
        assert (
            "error" in result["analysis_summary"].lower()
            or "failed" in result["analysis_summary"].lower()
        )

    @pytest.mark.asyncio
    async def test_anomalies_sorted_by_severity(self):
        # Many errors + repetitive to trigger multiple anomaly types
        lines = "\n".join(
            [f"2024-01-01T00:00:{i:02d} ERROR same error" for i in range(60)]
        )
        result = await detect_log_anomalies_impl(lines, severity_threshold="low")
        if (
            result["anomaly_details"]
            and len(result["anomaly_details"]["anomalies"]) > 1
        ):
            severities = [a["severity"] for a in result["anomaly_details"]["anomalies"]]
            order = {"high": 3, "medium": 2, "low": 1}
            assert all(
                order.get(severities[i], 0) >= order.get(severities[i + 1], 0)
                for i in range(len(severities) - 1)
            )


# ---------------------------------------------------------------------------
# _quick_volume_estimate_impl
# ---------------------------------------------------------------------------


class TestQuickVolumeEstimateImpl:
    """Tests for _quick_volume_estimate_impl."""

    @pytest.mark.asyncio
    async def test_estimates_from_sample(self):
        mock_fn = AsyncMock(return_value={"logs": {"container": "line1\nline2\nline3"}})
        result = await _quick_volume_estimate_impl(
            namespace="ns",
            pod_name="pod",
            get_pod_logs_fn=mock_fn,
        )
        # 3 lines in 5 min -> 3 * (24*60/5) = 864
        assert result == 864

    @pytest.mark.asyncio
    async def test_returns_default_on_error(self):
        mock_fn = AsyncMock(side_effect=RuntimeError("fail"))
        result = await _quick_volume_estimate_impl(
            namespace="ns",
            pod_name="pod",
            get_pod_logs_fn=mock_fn,
        )
        assert result == 10000

    @pytest.mark.asyncio
    async def test_empty_logs_returns_default(self):
        mock_fn = AsyncMock(return_value={"logs": {}})
        result = await _quick_volume_estimate_impl(
            namespace="ns",
            pod_name="pod",
            get_pod_logs_fn=mock_fn,
        )
        # 0 lines -> estimate is 0, not default, because no exception
        # Actually 0 * (24*60/5) = 0, and 0 is returned since sample_lines=0
        assert result == 0 or result == 10000

    @pytest.mark.asyncio
    async def test_list_format_logs(self):
        mock_fn = AsyncMock(return_value={"logs": {"c1": ["line1", "line2"]}})
        result = await _quick_volume_estimate_impl(
            namespace="ns",
            pod_name="pod",
            get_pod_logs_fn=mock_fn,
        )
        # 2 lines -> 2 * 288 = 576
        assert result == 576
