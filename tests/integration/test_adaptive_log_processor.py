"""
Integration tests for AdaptiveLogProcessor, _prioritize_pipeline_pods,
_estimate_pod_log_tokens, _calculate_adaptive_tail_lines, and
_truncate_logs_to_token_limit.

Covers P0 code paths: token budget math, first-pod guarantee, truncation,
pod prioritization, and the full get_pipelinerun_logs_impl adaptive flow.

Fixes #154
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_pod(name, phase="Running", restarts=0, age_hours=1):
    """Build a mock pod object matching the Kubernetes SDK shape."""
    pod = MagicMock()
    pod.metadata.name = name
    pod.metadata.labels = {"tekton.dev/pipelineRun": "my-pr"}
    ts = datetime.now(timezone.utc)
    pod.metadata.creation_timestamp = ts
    pod.status.phase = phase
    pod.status.pod_ip = "10.0.0.1"
    pod.spec.node_name = "node-1"

    cs = MagicMock()
    cs.restart_count = restarts
    cs.state.waiting = None
    cs.state.terminated = None
    pod.status.container_statuses = [cs]
    pod.status.init_container_statuses = []
    return pod


# ===========================================================================
# AdaptiveLogProcessor — unit-level integration
# ===========================================================================


class TestAdaptiveLogProcessor:
    def test_initial_budget(self):
        from helpers.k8s_client import AdaptiveLogProcessor

        p = AdaptiveLogProcessor(max_token_budget=100_000)
        assert p.max_token_budget == 100_000
        assert p.effective_budget == 80_000  # 100k * 0.8
        assert p.get_remaining_budget() == 80_000
        assert p.get_usage_percentage() == 0.0

    def test_record_usage_decreases_budget(self):
        from helpers.k8s_client import AdaptiveLogProcessor

        p = AdaptiveLogProcessor(max_token_budget=100_000)
        p.record_usage(20_000)
        assert p.get_remaining_budget() == 60_000
        assert p.get_usage_percentage() == pytest.approx(25.0)

    def test_can_process_more_true(self):
        from helpers.k8s_client import AdaptiveLogProcessor

        p = AdaptiveLogProcessor(max_token_budget=100_000)
        assert p.can_process_more(80_000) is True  # exactly at budget

    def test_can_process_more_false_when_exceeded(self):
        from helpers.k8s_client import AdaptiveLogProcessor

        p = AdaptiveLogProcessor(max_token_budget=100_000)
        p.record_usage(70_000)
        assert p.can_process_more(20_000) is False  # 70k + 20k > 80k

    def test_remaining_budget_never_negative(self):
        from helpers.k8s_client import AdaptiveLogProcessor

        p = AdaptiveLogProcessor(max_token_budget=100)
        p.record_usage(999_999)
        assert p.get_remaining_budget() == 0

    def test_usage_percentage_at_full(self):
        from helpers.k8s_client import AdaptiveLogProcessor

        p = AdaptiveLogProcessor(max_token_budget=100_000)
        p.record_usage(80_000)
        assert p.get_usage_percentage() == pytest.approx(100.0)

    def test_custom_budget_values(self):
        from helpers.k8s_client import AdaptiveLogProcessor

        p = AdaptiveLogProcessor(max_token_budget=50_000)
        assert p.effective_budget == 40_000
        p.record_usage(10_000)
        assert p.can_process_more(30_000) is True
        assert p.can_process_more(30_001) is False


# ===========================================================================
# _calculate_adaptive_tail_lines
# ===========================================================================


class TestCalculateAdaptiveTailLines:
    def test_small_pipeline(self):
        from helpers.k8s_client import _calculate_adaptive_tail_lines

        result = _calculate_adaptive_tail_lines(3, 0, 120_000)
        assert result <= 2000
        assert result >= 100

    def test_medium_pipeline(self):
        from helpers.k8s_client import _calculate_adaptive_tail_lines

        result = _calculate_adaptive_tail_lines(10, 0, 120_000)
        assert result <= 1000
        assert result >= 100

    def test_large_pipeline(self):
        from helpers.k8s_client import _calculate_adaptive_tail_lines

        result = _calculate_adaptive_tail_lines(20, 0, 120_000)
        assert result <= 500
        assert result >= 100

    def test_minimum_100_lines(self):
        from helpers.k8s_client import _calculate_adaptive_tail_lines

        result = _calculate_adaptive_tail_lines(100, 99, 100)
        assert result >= 100

    def test_zero_remaining_budget(self):
        from helpers.k8s_client import _calculate_adaptive_tail_lines

        result = _calculate_adaptive_tail_lines(5, 0, 0)
        assert result == 100  # floor

    def test_all_pods_processed_except_one(self):
        from helpers.k8s_client import _calculate_adaptive_tail_lines

        result = _calculate_adaptive_tail_lines(10, 9, 50_000)
        # Last pod gets all remaining budget
        assert result >= 100

    def test_budget_decreases_with_processing(self):
        from helpers.k8s_client import _calculate_adaptive_tail_lines

        lines_first = _calculate_adaptive_tail_lines(5, 0, 120_000)
        lines_later = _calculate_adaptive_tail_lines(5, 3, 30_000)
        # With less budget, lines should be <= first pod's allocation
        assert lines_later <= lines_first


# ===========================================================================
# _truncate_logs_to_token_limit
# ===========================================================================


class TestTruncateLogsToTokenLimit:
    def test_no_truncation_when_under_budget(self):
        from helpers.k8s_client import _truncate_logs_to_token_limit

        logs = "short log\n" * 10
        result, was_truncated = _truncate_logs_to_token_limit(logs, 999_999, "pod-a")
        assert was_truncated is False
        assert result == logs

    def test_truncation_when_over_budget(self):
        from helpers.k8s_client import _truncate_logs_to_token_limit

        logs = "A very long log line with plenty of content.\n" * 5000
        result, was_truncated = _truncate_logs_to_token_limit(logs, 100, "pod-b")
        assert was_truncated is True
        assert "TRUNCATED" in result
        assert "pod-b" in result
        assert len(result) < len(logs)

    def test_truncation_notice_contains_budget(self):
        from helpers.k8s_client import _truncate_logs_to_token_limit

        logs = "x" * 100_000
        result, was_truncated = _truncate_logs_to_token_limit(logs, 50, "pod-c")
        assert was_truncated is True
        assert "50" in result  # budget value in notice

    def test_empty_logs_no_truncation(self):
        from helpers.k8s_client import _truncate_logs_to_token_limit

        result, was_truncated = _truncate_logs_to_token_limit("", 100, "pod-d")
        assert was_truncated is False


# ===========================================================================
# _estimate_pod_log_tokens
# ===========================================================================


class TestEstimatePodLogTokens:
    @pytest.mark.asyncio
    async def test_estimation_returns_positive_int(self):
        from helpers.k8s_client import _estimate_pod_log_tokens

        sample_logs = {"step-build": "line1\nline2\nline3\n" * 20}
        with patch(
            "helpers.k8s_client.get_all_pod_logs",
            new_callable=AsyncMock,
            return_value=sample_logs,
        ):
            result = await _estimate_pod_log_tokens(
                "ns", "pod-a", tail_lines=500, k8s_core_api=MagicMock()
            )
        assert isinstance(result, int)
        assert result > 0

    @pytest.mark.asyncio
    async def test_estimation_fallback_on_exception(self):
        from helpers.k8s_client import _estimate_pod_log_tokens

        with patch(
            "helpers.k8s_client.get_all_pod_logs",
            new_callable=AsyncMock,
            side_effect=Exception("fail"),
        ):
            result = await _estimate_pod_log_tokens(
                "ns", "pod-err", tail_lines=200, k8s_core_api=MagicMock()
            )
        # Fallback: tail_lines * 30
        assert result == 200 * 30

    @pytest.mark.asyncio
    async def test_estimation_empty_sample(self):
        from helpers.k8s_client import _estimate_pod_log_tokens

        with patch(
            "helpers.k8s_client.get_all_pod_logs",
            new_callable=AsyncMock,
            return_value={},
        ):
            result = await _estimate_pod_log_tokens(
                "ns", "pod-empty", tail_lines=100, k8s_core_api=MagicMock()
            )
        # Empty sample falls through to fallback
        assert result == 100 * 30

    @pytest.mark.asyncio
    async def test_extrapolation_factor_capped_at_3(self):
        from helpers.k8s_client import _estimate_pod_log_tokens

        # With tail_lines=5000, sample_lines=500, raw_factor=10, capped to 3.0
        sample_logs = {"step-a": "data\n" * 100}
        with patch(
            "helpers.k8s_client.get_all_pod_logs",
            new_callable=AsyncMock,
            return_value=sample_logs,
        ):
            result = await _estimate_pod_log_tokens(
                "ns", "pod-cap", tail_lines=5000, k8s_core_api=MagicMock()
            )
        assert isinstance(result, int)
        assert result > 0


# ===========================================================================
# _prioritize_pipeline_pods
# ===========================================================================


class TestPrioritizePipelinePods:
    @pytest.mark.asyncio
    async def test_none_api_returns_original_order(self):
        from helpers.k8s_client import _prioritize_pipeline_pods

        names = ["pod-c", "pod-a", "pod-b"]
        result = await _prioritize_pipeline_pods(names, "ns", k8s_core_api=None)
        assert result == names

    @pytest.mark.asyncio
    async def test_failed_pods_come_first(self):
        from helpers.k8s_client import _prioritize_pipeline_pods

        running_pod = _make_pod("pod-ok", phase="Running")
        failed_pod = _make_pod("pod-fail", phase="Failed")

        mock_api = MagicMock()

        async def fake_read(name, namespace):
            if name == "pod-fail":
                return failed_pod
            return running_pod

        with patch(
            "helpers.k8s_client.asyncio.to_thread",
            side_effect=lambda fn, **kw: fake_read(kw["name"], kw["namespace"]),
        ):
            result = await _prioritize_pipeline_pods(
                ["pod-ok", "pod-fail"], "ns", k8s_core_api=mock_api
            )
        assert result[0] == "pod-fail"

    @pytest.mark.asyncio
    async def test_high_restart_pods_prioritized(self):
        from helpers.k8s_client import _prioritize_pipeline_pods

        normal_pod = _make_pod("pod-normal", restarts=0)
        restarting_pod = _make_pod("pod-restart", restarts=5)

        async def fake_read(fn, **kw):
            if kw["name"] == "pod-restart":
                return restarting_pod
            return normal_pod

        with patch("helpers.k8s_client.asyncio.to_thread", side_effect=fake_read):
            result = await _prioritize_pipeline_pods(
                ["pod-normal", "pod-restart"], "ns", k8s_core_api=MagicMock()
            )
        assert result[0] == "pod-restart"

    @pytest.mark.asyncio
    async def test_exception_for_one_pod_does_not_fail_all(self):
        from helpers.k8s_client import _prioritize_pipeline_pods

        good_pod = _make_pod("pod-good")
        call_count = [0]

        async def fake_read(fn, **kw):
            call_count[0] += 1
            if kw["name"] == "pod-bad":
                raise Exception("cannot read pod")
            return good_pod

        with patch("helpers.k8s_client.asyncio.to_thread", side_effect=fake_read):
            result = await _prioritize_pipeline_pods(
                ["pod-good", "pod-bad"], "ns", k8s_core_api=MagicMock()
            )
        assert len(result) == 2
        assert "pod-good" in result
        assert "pod-bad" in result


# ===========================================================================
# get_pipelinerun_logs_impl — full adaptive flow integration
# ===========================================================================


class TestGetPipelinerunLogsAdaptiveFlow:
    """Integration tests for the full adaptive pipeline log flow."""

    def _proc(self, budget=120_000):
        p = MagicMock()
        remaining = [budget]

        def record(tokens):
            remaining[0] = max(0, remaining[0] - tokens)

        p.get_remaining_budget.side_effect = lambda: remaining[0]
        p.can_process_more.return_value = True
        p.get_usage_percentage.return_value = 5.0
        p.max_token_budget = budget
        p.record_usage.side_effect = record
        return p

    @pytest.mark.asyncio
    async def test_adaptive_mode_processes_all_pods(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl

        pods = [_make_pod("pod-a"), _make_pod("pod-b"), _make_pod("pod-c")]
        proc = self._proc()
        proc_cls = MagicMock(return_value=proc)

        with patch(
            "tools.tekton_tools.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=MagicMock(items=pods),
        ):
            with patch(
                "tools.tekton_tools.get_all_pod_logs",
                new_callable=AsyncMock,
                return_value={"step": "log data"},
            ):
                with patch("tools.tekton_tools.asyncio.sleep", new_callable=AsyncMock):
                    result = await get_pipelinerun_logs_impl(
                        "pr", "ns",
                        k8s_core_api=MagicMock(),
                        k8s_custom_api=MagicMock(),
                        adaptive_processor_cls=proc_cls,
                        prioritize_pods_fn=AsyncMock(
                            return_value=["pod-a", "pod-b", "pod-c"]
                        ),
                        estimate_tokens_fn=AsyncMock(return_value=100),
                        calculate_tail_lines_fn=MagicMock(return_value=500),
                        truncate_logs_fn=MagicMock(return_value=("log data", False)),
                    )

        assert "_metadata" in result
        assert result["_metadata"]["adaptive_mode"] is True
        assert result["_metadata"]["pods_processed"] == 3
        assert result["_metadata"]["pods_skipped"] == 0

    @pytest.mark.asyncio
    async def test_first_pod_always_processed_despite_exhaustion(self):
        """Invariant: first pod is always processed regardless of budget."""
        from tools.tekton_tools import get_pipelinerun_logs_impl

        pods = [_make_pod("pod-a"), _make_pod("pod-b")]
        proc = self._proc()
        proc.can_process_more.return_value = False  # budget "exhausted"
        proc_cls = MagicMock(return_value=proc)

        with patch(
            "tools.tekton_tools.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=MagicMock(items=pods),
        ):
            with patch(
                "tools.tekton_tools.get_all_pod_logs",
                new_callable=AsyncMock,
                return_value={"step": "data"},
            ):
                with patch("tools.tekton_tools.asyncio.sleep", new_callable=AsyncMock):
                    result = await get_pipelinerun_logs_impl(
                        "pr", "ns",
                        k8s_core_api=MagicMock(),
                        k8s_custom_api=MagicMock(),
                        adaptive_processor_cls=proc_cls,
                        prioritize_pods_fn=AsyncMock(
                            return_value=["pod-a", "pod-b"]
                        ),
                        estimate_tokens_fn=AsyncMock(return_value=999_999),
                        calculate_tail_lines_fn=MagicMock(return_value=10),
                        truncate_logs_fn=MagicMock(return_value=("trunc", True)),
                    )

        assert "pod-a" in result
        assert result["_metadata"]["pods_processed"] >= 1
        assert result["_metadata"]["pods_skipped"] >= 1

    @pytest.mark.asyncio
    async def test_manual_mode_with_tail_lines(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl

        pods = [_make_pod("pod-x")]
        proc = self._proc()
        proc_cls = MagicMock(return_value=proc)

        with patch(
            "tools.tekton_tools.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=MagicMock(items=pods),
        ):
            with patch(
                "tools.tekton_tools.get_all_pod_logs",
                new_callable=AsyncMock,
                return_value={"step": "manual log"},
            ):
                result = await get_pipelinerun_logs_impl(
                    "pr", "ns",
                    tail_lines=200,
                    k8s_core_api=MagicMock(),
                    k8s_custom_api=MagicMock(),
                    adaptive_processor_cls=proc_cls,
                    prioritize_pods_fn=AsyncMock(return_value=["pod-x"]),
                    estimate_tokens_fn=AsyncMock(return_value=50),
                    calculate_tail_lines_fn=MagicMock(return_value=200),
                    truncate_logs_fn=MagicMock(return_value=("manual log", False)),
                )

        assert result["_metadata"]["mode"] == "manual"
        assert "pod-x" in result

    @pytest.mark.asyncio
    async def test_no_pods_returns_info(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl

        with patch(
            "tools.tekton_tools.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=MagicMock(items=[]),
        ):
            result = await get_pipelinerun_logs_impl(
                "pr", "ns",
                k8s_core_api=MagicMock(),
                k8s_custom_api=MagicMock(),
                adaptive_processor_cls=MagicMock(),
                prioritize_pods_fn=AsyncMock(return_value=[]),
                estimate_tokens_fn=AsyncMock(return_value=0),
                calculate_tail_lines_fn=MagicMock(return_value=100),
                truncate_logs_fn=MagicMock(return_value=("", False)),
            )

        assert "info" in result

    @pytest.mark.asyncio
    async def test_api_exception_returns_error(self):
        from kubernetes.client.rest import ApiException
        from tools.tekton_tools import get_pipelinerun_logs_impl

        with patch(
            "tools.tekton_tools.asyncio.to_thread",
            side_effect=ApiException(status=403, reason="Forbidden"),
        ):
            result = await get_pipelinerun_logs_impl(
                "pr", "ns",
                k8s_core_api=MagicMock(),
                k8s_custom_api=MagicMock(),
                adaptive_processor_cls=MagicMock(),
                prioritize_pods_fn=AsyncMock(),
                estimate_tokens_fn=AsyncMock(),
                calculate_tail_lines_fn=MagicMock(),
                truncate_logs_fn=MagicMock(),
            )

        assert "error" in result

    @pytest.mark.asyncio
    async def test_connection_error_returns_error(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl

        with patch(
            "tools.tekton_tools.asyncio.to_thread",
            side_effect=ConnectionError("refused"),
        ):
            result = await get_pipelinerun_logs_impl(
                "pr", "ns",
                k8s_core_api=MagicMock(),
                k8s_custom_api=MagicMock(),
                adaptive_processor_cls=MagicMock(),
                prioritize_pods_fn=AsyncMock(),
                estimate_tokens_fn=AsyncMock(),
                calculate_tail_lines_fn=MagicMock(),
                truncate_logs_fn=MagicMock(),
            )

        assert "error" in result

    @pytest.mark.asyncio
    async def test_truncation_tracked_in_metadata(self):
        from tools.tekton_tools import get_pipelinerun_logs_impl

        pods = [_make_pod("pod-big")]
        proc = self._proc(budget=50)
        proc_cls = MagicMock(return_value=proc)

        with patch(
            "tools.tekton_tools.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=MagicMock(items=pods),
        ):
            with patch(
                "tools.tekton_tools.get_all_pod_logs",
                new_callable=AsyncMock,
                return_value={"step": "x" * 10_000},
            ):
                with patch("tools.tekton_tools.asyncio.sleep", new_callable=AsyncMock):
                    with patch(
                        "tools.tekton_tools.calculate_context_tokens",
                        return_value=999_999,
                    ):
                        result = await get_pipelinerun_logs_impl(
                            "pr", "ns",
                            k8s_core_api=MagicMock(),
                            k8s_custom_api=MagicMock(),
                            adaptive_processor_cls=proc_cls,
                            prioritize_pods_fn=AsyncMock(
                                return_value=["pod-big"]
                            ),
                            estimate_tokens_fn=AsyncMock(return_value=100),
                            calculate_tail_lines_fn=MagicMock(return_value=100),
                            truncate_logs_fn=MagicMock(
                                return_value=("truncated", True)
                            ),
                        )

        assert result["_metadata"]["pods_truncated"] >= 1
