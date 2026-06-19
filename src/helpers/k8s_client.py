"""Adaptive log processing helpers for Kubernetes pod log management.

Extracted from server-mcp.py as part of issue #30 refactoring.
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Tuple

from helpers.utils import calculate_context_tokens, get_all_pod_logs

logger = logging.getLogger("lumino-mcp-server")


class AdaptiveLogProcessor:
    """Helper class for adaptive log processing with token management."""

    def __init__(self, max_token_budget: int = 150000):
        if max_token_budget <= 0:
            raise ValueError(
                f"max_token_budget must be a positive integer, got {max_token_budget}"
            )
        self.max_token_budget = max_token_budget
        self.safety_buffer = 0.8
        self.effective_budget = int(max_token_budget * self.safety_buffer)
        self.used_tokens = 0

    def can_process_more(self, estimated_tokens: int) -> bool:
        """Check if we can process more data within token budget."""
        return (self.used_tokens + estimated_tokens) <= self.effective_budget

    def record_usage(self, actual_tokens: int) -> None:
        """Record actual token usage."""
        self.used_tokens += actual_tokens

    def get_remaining_budget(self) -> int:
        """Get remaining token budget."""
        return max(0, self.effective_budget - self.used_tokens)

    def get_usage_percentage(self) -> float:
        """Get current token usage as percentage."""
        if self.effective_budget == 0:
            return 0.0
        return (self.used_tokens / self.effective_budget) * 100


async def _estimate_pod_log_tokens(
    namespace: str,
    pod_name: str,
    tail_lines: int = 500,
    sample_ratio: float = 0.1,
    *,
    k8s_core_api=None,
) -> int:
    """Estimate token usage for a pod's logs using representative sampling."""
    try:
        sample_lines = max(50, int(tail_lines * sample_ratio))

        sample = await get_all_pod_logs(
            pod_name=pod_name,
            namespace=namespace,
            k8s_core_api=k8s_core_api,
            tail_lines=sample_lines,
        )

        if sample:
            sample_text = ""
            for container_logs in sample.values():
                if isinstance(container_logs, str):
                    sample_text += container_logs

            sample_tokens = calculate_context_tokens(sample_text)
            raw_factor = tail_lines / sample_lines
            extrapolation_factor = min(raw_factor * 1.1, 3.0)
            estimated_tokens = int(sample_tokens * extrapolation_factor)

            logger.debug(
                f"Token estimate for {pod_name}: ~{estimated_tokens} tokens "
                f"(sampled {sample_lines} lines, factor {extrapolation_factor:.2f}x)"
            )
            return estimated_tokens

    except Exception as e:
        logger.debug(f"Token estimation failed for {pod_name}: {e}")

    return tail_lines * 30


async def _prioritize_pipeline_pods(
    pod_names: List[str],
    namespace: str,
    *,
    k8s_core_api=None,
) -> List[str]:
    """Prioritize pods for processing — failed pods first, recent pods next."""
    if k8s_core_api is None:
        return pod_names

    try:
        pod_priorities: List[Tuple[str, float]] = []

        for pod_name in pod_names:
            try:
                pod = await asyncio.to_thread(
                    k8s_core_api.read_namespaced_pod,
                    name=pod_name,
                    namespace=namespace,
                )

                priority_score: float = 0.0

                if pod.status and pod.status.phase in ("Failed", "Error"):
                    priority_score += 1000

                if pod.metadata and pod.metadata.creation_timestamp:
                    age_hours = (
                        datetime.now(pod.metadata.creation_timestamp.tzinfo)
                        - pod.metadata.creation_timestamp
                    ).total_seconds() / 3600
                    priority_score += max(0, 100 - age_hours)

                if pod.status and pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        if cs.restart_count and cs.restart_count > 0:
                            priority_score += 50 + cs.restart_count * 10

                pod_priorities.append((pod_name, priority_score))

            except Exception as e:
                logger.debug(f"Could not get details for pod {pod_name}: {e}")
                pod_priorities.append((pod_name, 1.0))

        pod_priorities.sort(key=lambda x: x[1], reverse=True)
        prioritized_names = [name for name, _ in pod_priorities]
        logger.info(f"Pod prioritization: {prioritized_names[:3]}... (showing top 3)")
        return prioritized_names

    except Exception as e:
        logger.warning(f"Pod prioritization failed: {e}")
        return pod_names


def _calculate_adaptive_tail_lines(
    total_pods: int,
    processed_pods: int,
    remaining_budget: int,
) -> int:
    """Calculate adaptive tail_lines based on pipeline size and remaining token budget."""
    remaining_pods = total_pods - processed_pods
    tokens_per_pod = remaining_budget // max(remaining_pods, 1)
    estimated_lines = tokens_per_pod // 25

    if total_pods <= 5:
        base_lines = min(2000, estimated_lines)
    elif total_pods <= 15:
        base_lines = min(1000, estimated_lines)
    else:
        base_lines = min(500, estimated_lines)

    adaptive_lines = max(100, base_lines)
    logger.debug(
        f"Adaptive tail_lines: {adaptive_lines} "
        f"(budget: {remaining_budget}, pods left: {remaining_pods})"
    )
    return adaptive_lines


def _truncate_logs_to_token_limit(
    logs: str,
    max_tokens: int,
    pod_name: str,
) -> Tuple[str, bool]:
    """Truncate logs if they exceed the token limit."""
    current_tokens = calculate_context_tokens(logs)
    if current_tokens <= max_tokens:
        return logs, False

    chars_per_token = len(logs) / current_tokens if current_tokens > 0 else 4
    target_chars = int(max_tokens * chars_per_token * 0.9)

    truncated = logs[:target_chars]
    last_newline = truncated.rfind("\n")
    if last_newline > target_chars * 0.8:
        truncated = truncated[:last_newline]

    truncation_notice = (
        f"\n\n[... TRUNCATED: {current_tokens:,} tokens exceeded budget of "
        f"{max_tokens:,} tokens for pod {pod_name} ...]"
    )
    truncated += truncation_notice

    logger.warning(
        f"Truncated logs for {pod_name}: {current_tokens:,} -> ~{max_tokens:,} tokens"
    )
    return truncated, True
