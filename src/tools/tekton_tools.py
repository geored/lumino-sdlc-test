"""Tekton pipeline log retrieval tools — extracted from server-mcp.py.

Each function accepts injected Kubernetes API clients and helpers rather than
relying on module-level globals.  The thin ``@mcp.tool()`` wrappers that remain
in ``server-mcp.py`` simply forward to these implementations.

Fixes #67
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from kubernetes.client.rest import ApiException

from helpers.utils import (
    calculate_context_tokens,
    clean_pipeline_logs,
    get_all_pod_logs,
)

logger = logging.getLogger("lumino-mcp-server")


async def get_pipelinerun_logs_impl(
    pipelinerun_name: str,
    namespace: str,
    clean_logs: bool = True,
    tail_lines: Optional[int] = None,
    since_seconds: Optional[int] = None,
    since_time: Optional[str] = None,
    timestamps: bool = True,
    previous: bool = False,
    max_token_budget: int = 120000,
    *,
    k8s_core_api: Any,
    k8s_custom_api: Any,
    adaptive_processor_cls: Any,
    prioritize_pods_fn: Any,
    estimate_tokens_fn: Any,
    calculate_tail_lines_fn: Any,
    truncate_logs_fn: Any,
    clean_pipeline_logs_fn: Any = None,
) -> Dict[str, Any]:
    """Fetch logs from all pods in a Tekton PipelineRun with adaptive volume management.

    Prioritizes failed pods and manages token budgets automatically when no
    time/line filters are specified.

    Parameters
    ----------
    pipelinerun_name:
        PipelineRun name.
    namespace:
        Kubernetes namespace.
    clean_logs:
        Clean and format logs (default: True).
    tail_lines:
        Lines from end (optional).
    since_seconds:
        Logs newer than N seconds (optional).
    since_time:
        Logs newer than RFC3339 timestamp (optional).
    timestamps:
        Include timestamps (default: True).
    previous:
        Get logs from previous container instance (default: False).
    max_token_budget:
        Maximum tokens for output (default: 120000). Applies to both adaptive
        and manual modes.
    k8s_core_api:
        Kubernetes CoreV1Api client.
    k8s_custom_api:
        Kubernetes CustomObjectsApi client.
    adaptive_processor_cls:
        ``AdaptiveLogProcessor`` class.
    prioritize_pods_fn:
        ``_prioritize_pipeline_pods`` async callable.
    estimate_tokens_fn:
        ``_estimate_pod_log_tokens`` async callable.
    calculate_tail_lines_fn:
        ``_calculate_adaptive_tail_lines`` callable.
    truncate_logs_fn:
        ``_truncate_logs_to_token_limit`` callable.
    clean_pipeline_logs_fn:
        ``clean_pipeline_logs`` callable (falls back to
        ``helpers.utils.clean_pipeline_logs``).

    Returns
    -------
    Dict[str, Any]
        Pod names as keys, logs as values.  Includes ``"_metadata"`` with
        processing info.  Returns ``{"info": "No pods found..."}`` if pods are
        garbage-collected -- use ``query_kubearchive`` tool.
    """
    if clean_pipeline_logs_fn is None:
        clean_pipeline_logs_fn = clean_pipeline_logs

    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}

    # Build log filtering info for logging
    filter_info: List[str] = []
    if since_time:
        filter_info.append(f"since_time={since_time}")
    elif since_seconds:
        filter_info.append(f"since_seconds={since_seconds}")
    elif tail_lines:
        filter_info.append(f"tail_lines={tail_lines}")

    filter_str = f" with filters: {', '.join(filter_info)}" if filter_info else ""
    logger.info(
        f"Fetching logs for PipelineRun '{pipelinerun_name}' in ns '{namespace}'{filter_str}..."
    )
    all_logs: Dict[str, Any] = {}

    try:
        # Find pods associated with the PipelineRun using Tekton labels
        label_selector = f"tekton.dev/pipelineRun={pipelinerun_name}"

        pod_list = await asyncio.to_thread(
            k8s_core_api.list_namespaced_pod,
            namespace=namespace,
            label_selector=label_selector,
        )

        if not pod_list.items:
            # Fallback: Try alternative label format used by some Tekton versions
            label_selector_alt = f"tekton.dev/pipeline={pipelinerun_name}"
            pod_list = await asyncio.to_thread(
                k8s_core_api.list_namespaced_pod,
                namespace=namespace,
                label_selector=label_selector_alt,
            )

        if not pod_list.items:
            return {
                "info": (
                    f"No pods found for PipelineRun '{pipelinerun_name}'. "
                    "Check if the PipelineRun exists and has completed pods."
                )
            }

        # Get all pod names
        pod_names = [pod.metadata.name for pod in pod_list.items]
        logger.info(f"Found {len(pod_names)} pods for PipelineRun '{pipelinerun_name}'")

        # Check if adaptive mode should be used
        use_adaptive_processing = (
            tail_lines is None and since_seconds is None and since_time is None
        )

        if use_adaptive_processing:
            logger.info(
                f"ADAPTIVE MODE activated for PipelineRun '{pipelinerun_name}' "
                f"- {len(pod_names)} pods detected"
            )

            # Initialize adaptive processor with configurable budget
            processor = adaptive_processor_cls(max_token_budget=max_token_budget)

            # Prioritize pods (failed pods first, recent pods next)
            prioritized_pods = await prioritize_pods_fn(
                pod_names, namespace, k8s_core_api=k8s_core_api
            )

            # Process pods progressively with token management
            processed_pods = 0
            truncated_pods = 0
            for pod_name in prioritized_pods:
                # STEP 1: Calculate adaptive tail_lines
                adaptive_tail_lines = calculate_tail_lines_fn(
                    len(pod_names),
                    processed_pods,
                    processor.get_remaining_budget(),
                )

                # STEP 2: Estimate tokens
                estimated_tokens = await estimate_tokens_fn(
                    namespace,
                    pod_name,
                    tail_lines=adaptive_tail_lines,
                    k8s_core_api=k8s_core_api,
                )

                # STEP 3: Check budget (GUARANTEE: always process first pod)
                is_first_pod = processed_pods == 0
                if not is_first_pod and not processor.can_process_more(
                    estimated_tokens
                ):
                    logger.info(
                        f"Token budget reached ({processor.get_usage_percentage():.1f}% used) "
                        f"- processed {processed_pods}/{len(pod_names)} pods"
                    )
                    break

                try:
                    # STEP 4: Fetch logs
                    pod_logs = await get_all_pod_logs(
                        pod_name=pod_name,
                        namespace=namespace,
                        k8s_core_api=k8s_core_api,
                        tail_lines=adaptive_tail_lines,
                        timestamps=timestamps,
                        previous=previous,
                    )

                    # Format and clean logs
                    if len(pod_logs) == 1:
                        container_name, logs = next(iter(pod_logs.items()))
                        if clean_logs:
                            logs = clean_pipeline_logs_fn(logs)
                        all_logs[pod_name] = logs
                    else:
                        formatted_logs: List[str] = []
                        for container_name, logs in pod_logs.items():
                            if clean_logs:
                                logs = clean_pipeline_logs_fn(logs)
                            formatted_logs.append(
                                f"--- Container: {container_name} ---"
                            )
                            formatted_logs.append(logs)
                            formatted_logs.append(
                                f"--- End Container: {container_name} ---"
                            )
                        all_logs[pod_name] = "\n".join(formatted_logs)

                    # HARD LIMIT: Truncate if actual tokens exceed remaining budget
                    remaining_budget = processor.get_remaining_budget()
                    actual_tokens = calculate_context_tokens(str(all_logs[pod_name]))

                    if actual_tokens > remaining_budget:
                        all_logs[pod_name], was_truncated = truncate_logs_fn(
                            all_logs[pod_name], remaining_budget, pod_name
                        )
                        if was_truncated:
                            truncated_pods += 1
                        actual_tokens = calculate_context_tokens(
                            str(all_logs[pod_name])
                        )

                    processor.record_usage(actual_tokens)
                    processed_pods += 1

                    logger.info(
                        f"Processed pod {processed_pods}/{len(pod_names)}: "
                        f"{pod_name} ({actual_tokens:,} tokens, "
                        f"{processor.get_usage_percentage():.1f}% budget used)"
                    )

                    # Brief pause for rate limiting
                    await asyncio.sleep(0.2)

                except Exception as e:
                    logger.error(f"Error fetching logs for pod {pod_name}: {e}")
                    all_logs[pod_name] = (
                        f"Error fetching logs for pod {pod_name}: {str(e)}"
                    )

            # Add adaptive processing metadata
            all_logs["_metadata"] = {
                "adaptive_mode": True,
                "pods_processed": processed_pods,
                "pods_truncated": truncated_pods,
                "pods_skipped": len(pod_names) - processed_pods,
                "total_pods_found": len(pod_names),
                "token_budget_used": f"{processor.get_usage_percentage():.1f}%",
                "token_budget_max": processor.max_token_budget,
                "processing_strategy": (
                    f"Pipeline size: {len(pod_names)} pods -> adaptive batching"
                ),
            }

        else:
            # MANUAL MODE: Use specified parameters with token budget enforcement
            logger.info(
                f"MANUAL MODE for PipelineRun '{pipelinerun_name}' "
                "- using specified constraints"
            )

            # Initialize processor for token tracking in manual mode
            processor = adaptive_processor_cls(max_token_budget=max_token_budget)
            truncated_pods = 0

            async def fetch_pod_logs(pod_name: str):
                try:
                    pod_logs = await get_all_pod_logs(
                        pod_name=pod_name,
                        namespace=namespace,
                        k8s_core_api=k8s_core_api,
                        tail_lines=tail_lines,
                        since_seconds=since_seconds,
                        since_time=since_time,
                        timestamps=timestamps,
                        previous=previous,
                    )
                    if len(pod_logs) == 1:
                        container_name, logs = next(iter(pod_logs.items()))
                        if clean_logs:
                            logs = clean_pipeline_logs_fn(logs)
                        return pod_name, logs
                    else:
                        formatted_logs: List[str] = []
                        for container_name, logs in pod_logs.items():
                            if clean_logs:
                                logs = clean_pipeline_logs_fn(logs)
                            formatted_logs.append(
                                f"--- Container: {container_name} ---"
                            )
                            formatted_logs.append(logs)
                            formatted_logs.append(
                                f"--- End Container: {container_name} ---"
                            )
                        return pod_name, "\n".join(formatted_logs)
                except Exception as e:
                    logger.error(f"Error fetching logs for pod {pod_name}: {e}")
                    return pod_name, (
                        f"Error fetching logs for pod {pod_name}: {str(e)}"
                    )

            # Fetch logs concurrently for all pods
            log_tasks = [fetch_pod_logs(pod_name) for pod_name in pod_names]
            results = await asyncio.gather(*log_tasks)

            # Apply token budget limiting to collected logs
            for pod_name, logs in results:
                remaining_budget = processor.get_remaining_budget()
                actual_tokens = calculate_context_tokens(str(logs))

                if actual_tokens > remaining_budget and remaining_budget > 0:
                    logs, was_truncated = truncate_logs_fn(
                        logs, remaining_budget, pod_name
                    )
                    if was_truncated:
                        truncated_pods += 1
                    actual_tokens = calculate_context_tokens(str(logs))
                elif remaining_budget <= 0:
                    logs = "[Skipped - token budget exhausted]"
                    actual_tokens = calculate_context_tokens(logs)

                all_logs[pod_name] = logs
                processor.record_usage(actual_tokens)

            # Add metadata for manual mode
            all_logs["_metadata"] = {
                "mode": "manual",
                "pods_processed": len(pod_names),
                "pods_truncated": truncated_pods,
                "token_budget_used": f"{processor.get_usage_percentage():.1f}%",
                "token_budget_max": max_token_budget,
                "filters_applied": filter_info if filter_info else ["none"],
            }

        return all_logs

    except ConnectionError as e:
        logger.error(f"Connection error: {e}")
        return {"error": str(e)}
    except ApiException as e:
        logger.error(
            f"K8s API error getting PipelineRun pods: "
            f"{e.status} - {e.reason} - {e.body}"
        )
        return {"error": f"Failed to find pods for PipelineRun: {e.reason}"}
    except Exception as e:
        logger.error(f"Unexpected error getting PipelineRun logs: {e}", exc_info=True)
        return {"error": f"An unexpected error occurred: {str(e)}"}
