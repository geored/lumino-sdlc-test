"""Log analysis tools — extracted from server-mcp.py.

Each function accepts injected Kubernetes API clients and helpers rather than
relying on module-level globals.  The thin ``@mcp.tool()`` wrappers that remain
in ``server-mcp.py`` simply forward to these implementations.

Fixes #75
"""

import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from helpers.log_analysis import (
    LogAnalysisContext,
    LogAnalysisStrategy,
    LogStreamProcessor,
    StrategySelector,
    analyze_trending_patterns,
    combine_analysis_results,
    extract_log_patterns,
    generate_focused_summary,
    generate_hybrid_recommendations,
    generate_streaming_recommendations,
    generate_streaming_summary,
    generate_supplementary_insights,
    get_strategy_selection_reason,
    sample_logs_by_time,
    truncate_to_token_limit,
)
from helpers.utils import (
    calculate_context_tokens,
    categorize_errors,
    extract_error_patterns,
    generate_log_summary,
    get_all_pod_logs,
    parse_time_parameters,
)

logger = logging.getLogger("lumino-mcp-server")


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


async def _quick_volume_estimate_impl(
    namespace: str,
    pod_name: str,
    *,
    get_pod_logs_fn: Any,
) -> int:
    """Quick estimate of log volume using a 5-minute sample."""
    try:
        sample = await get_pod_logs_fn(
            namespace=namespace,
            pod_name=pod_name,
            since_seconds=300,
        )
        if "logs" in sample and sample["logs"]:
            sample_lines = 0
            for container_logs in sample["logs"].values():
                if isinstance(container_logs, str):
                    sample_lines += len(container_logs.split("\n"))
                elif isinstance(container_logs, list):
                    sample_lines += len(container_logs)
            estimated_total = sample_lines * (24 * 60 / 5)
            logger.info(
                f"Volume estimate for {pod_name}: {sample_lines} lines in 5min "
                f"-> ~{int(estimated_total)} total estimated"
            )
            return int(estimated_total)
    except Exception as e:
        logger.debug(f"Volume estimation failed for {pod_name}: {e}")
    return 10000


# ---------------------------------------------------------------------------
# get_pod_logs
# ---------------------------------------------------------------------------


async def get_pod_logs_impl(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    tail_lines: Optional[int] = None,
    since_seconds: Optional[int] = None,
    since_time: Optional[str] = None,
    timestamps: bool = True,
    previous: bool = False,
    *,
    k8s_core_api: Any,
) -> Dict[str, Any]:
    """Fetch logs from a pod -- thin wrapper around get_all_pod_logs."""
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}

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

        if isinstance(pod_logs, dict):
            error_keys = [k for k in pod_logs.keys() if k.startswith(("error_", "pod_error", "no_"))]
            if error_keys:
                return {"error": pod_logs.get(error_keys[0], "Unknown error retrieving logs")}

            if container_name:
                if container_name in pod_logs:
                    return {"logs": {container_name: pod_logs[container_name]}}
                return {"error": f"Container '{container_name}' not found in pod '{pod_name}'"}

            return {"logs": pod_logs}

        return {"error": f"Unexpected response format from get_all_pod_logs: {type(pod_logs)}"}

    except Exception as e:
        logger.error(f"Error in get_pod_logs for pod {pod_name} in {namespace}: {e}")
        return {"error": f"Failed to retrieve logs: {str(e)}"}


# ---------------------------------------------------------------------------
# analyze_logs
# ---------------------------------------------------------------------------


async def analyze_logs_impl(log_text: str) -> Dict[str, Any]:
    """Analyse raw log text and return error patterns and insights."""
    try:
        error_patterns = extract_error_patterns(log_text)
        error_categories = categorize_errors(log_text, error_patterns)
        return {
            "error_count": len(error_patterns),
            "error_patterns": error_patterns,
            "categorized_errors": error_categories,
            "summary": generate_log_summary(log_text, error_patterns, error_categories),
        }
    except Exception as e:
        logger.error(f"Error in analyze_logs: {e}", exc_info=True)
        return {
            "error_count": 0,
            "error_patterns": [],
            "categorized_errors": {},
            "summary": f"Analysis failed: {str(e)}",
        }


# ---------------------------------------------------------------------------
# detect_log_anomalies
# ---------------------------------------------------------------------------


async def detect_log_anomalies_impl(
    logs: str,
    baseline_patterns: Optional[List[str]] = None,
    severity_threshold: str = "medium",
) -> Dict[str, Any]:
    """Detect anomalies in log data using error frequency, pattern repetition, and timestamps."""
    logger.info(f"Starting log anomaly detection with severity threshold: {severity_threshold}")

    if not logs or logs.strip() == "":
        return {
            "anomaly_detected": False,
            "anomaly_details": None,
            "analysis_summary": "No logs provided for analysis",
        }

    try:
        normalized_logs = re.sub(
            r"\\n(?=\d{4}-\d{2}-\d{2}|level=|\"level\"|time=|\"ts\"|msg=|http:)",
            "\n",
            logs,
        )
        if "\n" not in normalized_logs and "\\n" in normalized_logs:
            normalized_logs = normalized_logs.replace("\\n", "\n")

        log_lines = [line.strip() for line in normalized_logs.split("\n") if line.strip()]
        total_lines = len(log_lines)

        if total_lines == 0:
            return {
                "anomaly_detected": False,
                "anomaly_details": None,
                "analysis_summary": "No valid log lines found",
            }

        logger.info(f"Analyzing {total_lines} log lines for anomalies")
        anomalies: List[Dict[str, Any]] = []

        thresholds = {
            "low": {"error_rate": 0.05, "repetition_rate": 0.3, "time_gap": 300},
            "medium": {"error_rate": 0.10, "repetition_rate": 0.5, "time_gap": 180},
            "high": {"error_rate": 0.20, "repetition_rate": 0.7, "time_gap": 60},
        }
        threshold_config = thresholds.get(severity_threshold, thresholds["medium"])

        # 1. Error frequency
        error_pattern_map = {
            r"(?i)(error|exception|failed|fatal|panic|critical)": "error",
            r"(?i)(timeout|connection\s+refused|connection\s+reset)": "timeout",
            r"(?i)(out\s+of\s+memory|memory\s+limit|oom)": "memory",
            r"(?i)(permission\s+denied|access\s+denied|unauthorized)": "permission",
            r"(?i)(not\s+found|missing|invalid|corrupt)": "not_found",
        }
        error_counts: Dict[str, int] = {}
        error_lines = []

        for i, line in enumerate(log_lines):
            for pattern, label in error_pattern_map.items():
                if re.search(pattern, line):
                    error_lines.append((i, line))
                    error_counts[label] = error_counts.get(label, 0) + 1

        unique_error_line_indices = set(x[0] for x in error_lines)
        error_rate = len(unique_error_line_indices) / total_lines
        if error_rate > threshold_config["error_rate"]:
            anomalies.append(
                {
                    "type": "high_error_rate",
                    "severity": "high" if error_rate > 0.3 else "medium",
                    "description": (
                        f"High error rate detected: {error_rate:.2%} "
                        f"({len(unique_error_line_indices)}/{total_lines} lines)"
                    ),
                    "details": {
                        "error_rate": error_rate,
                        "error_patterns": error_counts,
                        "sample_errors": [x[1][:200] for x in error_lines[:5]],
                    },
                }
            )

        # 2. Repetitive patterns
        line_frequency: Dict[str, int] = {}
        for line in log_lines:
            norm = re.sub(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", "TIMESTAMP", line)
            norm = re.sub(r"\b\d+\b", "NUMBER", norm)
            norm = re.sub(r"\b[a-f0-9]{8,}\b", "HASH", norm)
            line_frequency[norm] = line_frequency.get(norm, 0) + 1

        for pattern, count in line_frequency.items():
            repetition_rate = count / total_lines
            if repetition_rate > threshold_config["repetition_rate"] and count > 10:
                anomalies.append(
                    {
                        "type": "repetitive_pattern",
                        "severity": "high" if repetition_rate > 0.8 else "medium",
                        "description": (
                            f"Highly repetitive log pattern detected: {repetition_rate:.2%} "
                            f"of logs ({count} occurrences)"
                        ),
                        "details": {
                            "pattern": pattern[:200],
                            "occurrence_count": count,
                            "repetition_rate": repetition_rate,
                        },
                    }
                )

        # 3. Timestamp gap / burst analysis
        timestamps = []
        for line in log_lines:
            m = re.search(r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})", line)
            if m:
                try:
                    ts = datetime.fromisoformat(m.group(1).replace("T", " "))
                    timestamps.append(ts)
                except Exception:
                    continue

        if len(timestamps) > 2:
            time_gaps = [(timestamps[i] - timestamps[i - 1]).total_seconds() for i in range(1, len(timestamps))]
            if time_gaps:
                avg_gap = sum(time_gaps) / len(time_gaps)
                max_gap = max(time_gaps)
                if max_gap > threshold_config["time_gap"] and max_gap > avg_gap * 10:
                    anomalies.append(
                        {
                            "type": "time_gap_anomaly",
                            "severity": "medium",
                            "description": f"Unusual time gap detected: {max_gap:.0f} seconds (avg: {avg_gap:.1f}s)",
                            "details": {
                                "max_gap_seconds": max_gap,
                                "average_gap_seconds": avg_gap,
                                "total_timestamps": len(timestamps),
                            },
                        }
                    )

                burst_threshold = 50
                one_minute_windows: Dict[Any, int] = {}
                for ts in timestamps:
                    minute_key = ts.replace(second=0, microsecond=0)
                    one_minute_windows[minute_key] = one_minute_windows.get(minute_key, 0) + 1
                max_burst = max(one_minute_windows.values()) if one_minute_windows else 0
                if max_burst > burst_threshold:
                    anomalies.append(
                        {
                            "type": "log_burst",
                            "severity": "medium",
                            "description": f"Log burst detected: {max_burst} logs in one minute",
                            "details": {
                                "max_logs_per_minute": max_burst,
                                "burst_threshold": burst_threshold,
                            },
                        }
                    )

        # 4. Baseline comparison
        if baseline_patterns:
            baseline_set = set(baseline_patterns)
            current_patterns = set(error_counts.keys())
            new_patterns = current_patterns - baseline_set
            missing_patterns = baseline_set - current_patterns
            if new_patterns:
                anomalies.append(
                    {
                        "type": "new_error_patterns",
                        "severity": "medium",
                        "description": f"New error patterns not seen in baseline: {', '.join(list(new_patterns)[:5])}",
                        "details": {
                            "new_patterns": list(new_patterns),
                            "baseline_patterns": baseline_patterns,
                        },
                    }
                )
            if missing_patterns and len(missing_patterns) > len(baseline_patterns) * 0.5:
                anomalies.append(
                    {
                        "type": "missing_expected_patterns",
                        "severity": "low",
                        "description": f"Expected patterns missing from logs: {', '.join(list(missing_patterns)[:5])}",
                        "details": {"missing_patterns": list(missing_patterns)},
                    }
                )

        # 5. Log level distribution
        log_levels = {"debug": 0, "info": 0, "warn": 0, "error": 0, "fatal": 0}
        for line in log_lines:
            ll = line.lower()
            if re.search(r"\b(debug|trace)\b", ll):
                log_levels["debug"] += 1
            elif re.search(r"\binfo\b", ll):
                log_levels["info"] += 1
            elif re.search(r"\b(warn|warning)\b", ll):
                log_levels["warn"] += 1
            elif re.search(r"\b(error|err)\b", ll):
                log_levels["error"] += 1
            elif re.search(r"\b(fatal|critical|panic)\b", ll):
                log_levels["fatal"] += 1

        total_leveled = sum(log_levels.values())
        if total_leveled > 0:
            severe_ratio = (log_levels["error"] + log_levels["fatal"]) / total_leveled
            if severe_ratio > 0.5:
                anomalies.append(
                    {
                        "type": "unusual_log_level_distribution",
                        "severity": "high",
                        "description": f"High proportion of severe logs: {severe_ratio:.2%}",
                        "details": {
                            "log_level_distribution": log_levels,
                            "severe_log_ratio": severe_ratio,
                        },
                    }
                )

        anomaly_detected = bool(anomalies)
        if anomaly_detected:
            severity_order = {"high": 3, "medium": 2, "low": 1}
            anomalies.sort(key=lambda x: severity_order.get(x["severity"], 0), reverse=True)
            anomaly_details: Optional[Dict[str, Any]] = {
                "total_anomalies": len(anomalies),
                "anomalies": anomalies,
                "log_statistics": {
                    "total_lines": total_lines,
                    "error_rate": error_rate,
                    "unique_patterns": len(line_frequency),
                    "timestamp_coverage": (len(timestamps) / total_lines if total_lines > 0 else 0),
                },
            }
            analysis_summary = (
                f"Detected {len(anomalies)} anomalies in {total_lines} log lines. "
                f"Highest severity: {anomalies[0]['severity']}. "
                f"Primary issues: {', '.join(a['type'] for a in anomalies[:3])}"
            )
        else:
            anomaly_details = None
            analysis_summary = f"No anomalies detected in {total_lines} log lines. Log patterns appear normal."

        logger.info(f"Anomaly detection completed. Found {len(anomalies)} anomalies")
        return {
            "anomaly_detected": anomaly_detected,
            "anomaly_details": anomaly_details,
            "analysis_summary": analysis_summary,
        }

    except Exception as e:
        logger.error(f"Error during log anomaly detection: {str(e)}", exc_info=True)
        return {
            "anomaly_detected": False,
            "anomaly_details": None,
            "analysis_summary": f"Analysis failed due to error: {str(e)}",
        }


# ---------------------------------------------------------------------------
# smart_summarize_pod_logs
# ---------------------------------------------------------------------------


async def smart_summarize_pod_logs_impl(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    summary_level: str = "detailed",
    focus_areas: Optional[List[str]] = None,
    time_segments: int = 5,
    max_context_tokens: int = 10000,
    since_seconds: Optional[int] = None,
    tail_lines: Optional[int] = None,
    time_period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    *,
    k8s_core_api: Any,
    get_pod_logs_fn: Any,
) -> Dict[str, Any]:
    """Adaptive pod log analysis with automatic volume management."""
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}

    if focus_areas is None:
        focus_areas = ["errors", "warnings", "performance"]

    start_timestamp = time.time()
    tool_name = "smart_summarize_pod_logs"

    if not namespace or not isinstance(namespace, str):
        return {"error": f"Invalid namespace parameter: {namespace}. Must be a non-empty string."}
    if not pod_name or not isinstance(pod_name, str):
        return {"error": f"Invalid pod_name parameter: {pod_name}. Must be a non-empty string."}
    if summary_level not in ["brief", "detailed", "comprehensive"]:
        summary_level = "detailed"
    if time_segments <= 0:
        time_segments = 10
    if max_context_tokens < 500:
        max_context_tokens = 500

    try:
        user_specified_constraints = any(
            v is not None for v in [since_seconds, tail_lines, time_period, start_time, end_time]
        )

        if not user_specified_constraints:
            volume_estimate = await _quick_volume_estimate_impl(namespace, pod_name, get_pod_logs_fn=get_pod_logs_fn)
            if volume_estimate > 50000:
                log_params: Dict[str, Any] = {"tail_lines": 500}
                if "errors" not in focus_areas:
                    focus_areas = ["errors"] + list(focus_areas)
            elif volume_estimate > 10000:
                log_params = {"tail_lines": 2000}
            else:
                log_params = {"since_seconds": 7200}
            time_info: Dict[str, Any] = {
                "method": "adaptive",
                "strategy": "volume_based",
                "volume_estimate": volume_estimate,
            }
        else:
            time_config = parse_time_parameters(
                since_seconds=since_seconds,
                time_period=time_period,
                start_time=start_time,
                end_time=end_time,
            )
            log_params = time_config["log_params"].copy()
            time_info = time_config["time_info"]
            if tail_lines is not None:
                log_params["tail_lines"] = tail_lines

        logger.info(f"[{tool_name}] Time configuration: {time_info}")

        if not log_params and max_context_tokens < 20000:
            log_params["tail_lines"] = min(1000, max_context_tokens // 10)

        raw_logs = await get_pod_logs_fn(namespace=namespace, pod_name=pod_name, **log_params)

        if "error" in raw_logs:
            return {"error": f"Failed to retrieve logs: {raw_logs['error']}"}
        if "logs" not in raw_logs or not raw_logs["logs"]:
            return {
                "error": "No logs found for the specified pod",
                "metadata": {"pod_name": pod_name, "namespace": namespace},
            }

        all_log_lines: List[str] = []
        container_info: Dict[str, int] = {}
        for container, logs in raw_logs["logs"].items():
            if container_name and container != container_name:
                continue
            lines = logs if isinstance(logs, list) else str(logs).split("\n")
            container_info[container] = len(lines)
            all_log_lines.extend(lines)

        if not all_log_lines:
            return {
                "error": (
                    f"No logs found for container '{container_name}'" if container_name else "No log content found"
                ),
                "available_containers": list(raw_logs["logs"].keys()),
            }

        all_log_lines = [ln for ln in all_log_lines if ln.strip()]
        total_log_lines = len(all_log_lines)

        patterns = extract_log_patterns(all_log_lines, focus_areas)
        time_samples = sample_logs_by_time(all_log_lines, time_segments)
        summary = generate_focused_summary(patterns, focus_areas, summary_level)

        summary_tokens = calculate_context_tokens(str(summary))
        available_tokens = min(max_context_tokens - summary_tokens - 10000, 15000)
        representative_samples: Dict[str, Any] = {}
        current_tokens = 0

        for area in focus_areas:
            if area in patterns and patterns[area] and current_tokens < available_tokens:
                samples = []
                for item in patterns[area][:3]:
                    content = item["content"]
                    truncated = content[:200] + "..." if len(content) > 200 else content
                    sample_tokens = calculate_context_tokens(truncated)
                    if current_tokens + sample_tokens < available_tokens:
                        samples.append(
                            {
                                "line_number": item["line_number"],
                                "content": truncated,
                                "timestamp": item.get("timestamp"),
                            }
                        )
                        current_tokens += sample_tokens
                    else:
                        break
                if samples:
                    representative_samples[area] = samples

        processing_time = time.time() - start_timestamp
        results = {
            "summary": summary,
            "patterns": {k: v for k, v in patterns.items() if v},
            "time_segments": {
                "segment_count": len(time_samples),
                "lines_per_segment": {k: len(v) for k, v in time_samples.items()},
            },
            "representative_samples": representative_samples,
            "metadata": {
                "pod_name": pod_name,
                "namespace": namespace,
                "container_info": container_info,
                "analysis_parameters": {
                    "summary_level": summary_level,
                    "focus_areas": focus_areas,
                    "time_segments": time_segments,
                    "max_context_tokens": max_context_tokens,
                },
                "processing_metrics": {
                    "total_log_lines": total_log_lines,
                    "processing_time_seconds": round(processing_time, 2),
                    "estimated_tokens_used": current_tokens + summary_tokens,
                    "patterns_extracted": sum(len(v) for v in patterns.values()),
                },
            },
        }

        results = truncate_to_token_limit(results, max_context_tokens)
        if results.get("_truncated"):
            logger.info(f"[{tool_name}] Output truncated to fit within {max_context_tokens} token limit")
        return results

    except Exception as e:
        logger.error(f"[{tool_name}] Unexpected error: {e}", exc_info=True)
        return {
            "error": f"Unexpected error during log analysis: {str(e)}",
            "metadata": {
                "pod_name": pod_name,
                "namespace": namespace,
                "processing_time": time.time() - start_timestamp,
            },
        }


# ---------------------------------------------------------------------------
# stream_analyze_pod_logs
# ---------------------------------------------------------------------------


async def stream_analyze_pod_logs_impl(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    chunk_size: int = 5000,
    analysis_mode: str = "errors_and_warnings",
    time_window: Optional[str] = None,
    follow: bool = False,
    max_chunks: int = 50,
    since_seconds: Optional[int] = None,
    tail_lines: Optional[int] = None,
    time_period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    max_context_tokens: int = 50000,
    *,
    k8s_core_api: Any,
    get_pod_logs_fn: Any,
) -> Dict[str, Any]:
    """Stream and analyse pod logs in chunks with progressive pattern detection."""
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}

    start_timestamp = time.time()
    tool_name = "stream_analyze_pod_logs"

    if not namespace or not isinstance(namespace, str):
        return {"error": f"Invalid namespace parameter: {namespace}. Must be a non-empty string."}
    if not pod_name or not isinstance(pod_name, str):
        return {"error": f"Invalid pod_name parameter: {pod_name}. Must be a non-empty string."}
    if chunk_size < 1000 or chunk_size > 10000:
        chunk_size = 5000
    if analysis_mode not in [
        "errors_only",
        "errors_and_warnings",
        "full_analysis",
        "custom_patterns",
    ]:
        analysis_mode = "errors_and_warnings"

    try:
        processor = LogStreamProcessor(chunk_size=chunk_size, analysis_mode=analysis_mode)

        if time_period or start_time or end_time or since_seconds:
            time_config = parse_time_parameters(
                since_seconds=since_seconds,
                time_period=time_period,
                start_time=start_time,
                end_time=end_time,
            )
            log_params: Dict[str, Any] = time_config["log_params"].copy()
        else:
            log_params = {}
            if time_window:
                time_mapping = {"1h": 3600, "6h": 21600, "24h": 86400, "1d": 86400}
                if time_window in time_mapping:
                    log_params["since_seconds"] = time_mapping[time_window]

        if tail_lines is not None:
            log_params["tail_lines"] = tail_lines
        elif "since_seconds" not in log_params:
            log_params["tail_lines"] = 2000

        raw_logs = await get_pod_logs_fn(namespace=namespace, pod_name=pod_name, **log_params)

        if "error" in raw_logs:
            return {"error": f"Failed to retrieve logs: {raw_logs['error']}"}
        if "logs" not in raw_logs or not raw_logs["logs"]:
            return {
                "error": "No logs found for the specified pod",
                "metadata": {"pod_name": pod_name, "namespace": namespace},
            }

        all_log_lines: List[str] = []
        container_info: Dict[str, int] = {}
        for container, logs in raw_logs["logs"].items():
            if container_name and container != container_name:
                continue
            lines = logs if isinstance(logs, list) else str(logs).split("\n")
            container_info[container] = len(lines)
            all_log_lines.extend(lines)

        if not all_log_lines:
            return {
                "error": (
                    f"No logs found for container '{container_name}'" if container_name else "No log content found"
                ),
                "available_containers": list(raw_logs["logs"].keys()),
            }

        all_log_lines = [ln for ln in all_log_lines if ln.strip()]
        total_log_lines = len(all_log_lines)

        chunk_results = []
        lines_processed = 0
        chunks_processed = 0

        for line in all_log_lines:
            if chunks_processed >= max_chunks:
                break
            chunk_result = processor.add_line(line)
            lines_processed += 1
            if chunk_result:
                chunk_results.append(chunk_result)
                chunks_processed += 1

        final_summary = processor.finalize()
        if final_summary.get("last_chunk"):
            chunk_results.append(final_summary["last_chunk"])
            chunks_processed += 1

        overall_summary = generate_streaming_summary(chunk_results)
        trending_patterns = analyze_trending_patterns(chunk_results)
        recommendations = generate_streaming_recommendations(overall_summary, trending_patterns)
        processing_time = time.time() - start_timestamp

        results = {
            "chunks": chunk_results,
            "overall_summary": overall_summary,
            "trending_patterns": trending_patterns,
            "recommendations": recommendations,
            "metadata": {
                "pod_name": pod_name,
                "namespace": namespace,
                "container_info": container_info,
                "analysis_parameters": {
                    "chunk_size": chunk_size,
                    "analysis_mode": analysis_mode,
                    "follow": follow,
                    "max_chunks": max_chunks,
                },
                "processing_metrics": {
                    "total_log_lines": total_log_lines,
                    "lines_processed": lines_processed,
                    "chunks_processed": chunks_processed,
                    "processing_time_seconds": round(processing_time, 2),
                    "average_chunk_processing_time": round(processing_time / max(chunks_processed, 1), 3),
                },
            },
        }

        results = truncate_to_token_limit(results, max_context_tokens)
        return results

    except Exception as e:
        logger.error(f"[{tool_name}] Unexpected error: {e}", exc_info=True)
        return {
            "error": f"Unexpected error during streaming log analysis: {str(e)}",
            "metadata": {
                "pod_name": pod_name,
                "namespace": namespace,
                "processing_time": time.time() - start_timestamp,
            },
        }


# ---------------------------------------------------------------------------
# analyze_pod_logs_hybrid
# ---------------------------------------------------------------------------


async def analyze_pod_logs_hybrid_impl(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    strategy: str = "auto",
    request_type: str = "investigation",
    urgency: str = "medium",
    use_cache: bool = True,
    custom_params: Optional[Dict[str, Any]] = None,
    *,
    k8s_core_api: Any,
    analysis_cache: Any,
    smart_summarize_fn: Any,
    stream_analyze_fn: Any,
) -> Dict[str, Any]:
    """Hybrid log analyser with intelligent strategy selection and caching."""
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}

    start_timestamp = time.time()
    tool_name = "analyze_pod_logs_hybrid"

    if not namespace or not isinstance(namespace, str):
        return {"error": f"Invalid namespace parameter: {namespace}. Must be a non-empty string."}
    if not pod_name or not isinstance(pod_name, str):
        return {"error": f"Invalid pod_name parameter: {pod_name}. Must be a non-empty string."}
    if strategy not in ["auto", "smart_summary", "streaming", "hybrid"]:
        strategy = "auto"
    if request_type not in ["investigation", "troubleshooting", "monitoring"]:
        request_type = "investigation"
    if urgency not in ["low", "medium", "high", "critical"]:
        urgency = "medium"

    try:
        cache_key_params = {
            "container_name": container_name,
            "strategy": strategy,
            "request_type": request_type,
            "urgency": urgency,
            "custom_params": custom_params,
        }

        if use_cache:
            cached_result = analysis_cache.get(namespace, pod_name, cache_key_params)
            if cached_result:
                cached_result["cache_info"] = {
                    "cache_hit": True,
                    "cache_age_seconds": time.time() - start_timestamp,
                }
                return cached_result

        log_size_estimate = await StrategySelector.estimate_log_size(namespace, pod_name, k8s_core_api=k8s_core_api)

        context = LogAnalysisContext(
            log_size_estimate=log_size_estimate,
            pod_name=pod_name,
            namespace=namespace,
            request_type=request_type,
            urgency=urgency,
            time_sensitivity=(urgency in ["high", "critical"]),
            follow_up_analysis=False,
        )

        if strategy == "auto":
            selected_strategy = StrategySelector.select_strategy(
                context,
                [LogAnalysisStrategy.SMART_SUMMARY, LogAnalysisStrategy.STREAMING],
            )
        else:
            selected_strategy = {
                "smart_summary": LogAnalysisStrategy.SMART_SUMMARY,
                "streaming": LogAnalysisStrategy.STREAMING,
                "hybrid": LogAnalysisStrategy.HYBRID,
            }[strategy]

        strategy_params = (custom_params or {}).copy()
        strategy_params.update(
            {
                "namespace": namespace,
                "pod_name": pod_name,
                "container_name": container_name,
            }
        )

        primary_results: Optional[Dict[str, Any]] = None

        if selected_strategy == LogAnalysisStrategy.SMART_SUMMARY:
            if urgency in ["high", "critical"]:
                strategy_params.update(
                    {
                        "summary_level": "brief",
                        "max_context_tokens": 5000,
                        "time_segments": 3,
                    }
                )
            elif urgency == "low":
                strategy_params.update(
                    {
                        "summary_level": "comprehensive",
                        "max_context_tokens": 15000,
                        "time_segments": 10,
                    }
                )
            else:
                strategy_params.update(
                    {
                        "summary_level": "detailed",
                        "max_context_tokens": 8000,
                        "time_segments": 5,
                    }
                )
            primary_results = await smart_summarize_fn(**strategy_params)

        elif selected_strategy == LogAnalysisStrategy.STREAMING:
            if urgency == "critical":
                strategy_params.update(
                    {
                        "chunk_size": 1000,
                        "analysis_mode": "errors_only",
                        "max_chunks": 20,
                    }
                )
            elif request_type == "troubleshooting":
                strategy_params.update(
                    {
                        "chunk_size": 3000,
                        "analysis_mode": "errors_and_warnings",
                        "max_chunks": 30,
                    }
                )
            else:
                strategy_params.update(
                    {
                        "chunk_size": 5000,
                        "analysis_mode": "full_analysis",
                        "max_chunks": 50,
                    }
                )
            primary_results = await stream_analyze_fn(**strategy_params)

        elif selected_strategy == LogAnalysisStrategy.HYBRID:
            summary_params = {
                **strategy_params,
                "summary_level": "detailed",
                "max_context_tokens": 20000,
                "time_segments": 8,
            }
            streaming_params = {
                **strategy_params,
                "chunk_size": 4000,
                "analysis_mode": "errors_and_warnings",
                "max_chunks": 25,
            }
            summary_result = await smart_summarize_fn(**summary_params)
            streaming_result = await stream_analyze_fn(**streaming_params)
            primary_results = {
                "combined_analysis": {
                    "summary_analysis": summary_result,
                    "streaming_analysis": streaming_result,
                },
                "hybrid_insights": combine_analysis_results(summary_result, streaming_result),
            }

        supplementary_results = generate_supplementary_insights(primary_results, context)
        processing_time = time.time() - start_timestamp
        performance_metrics = {
            "processing_time_seconds": round(processing_time, 2),
            "strategy_selected": selected_strategy.value,
            "strategy_selection_reason": get_strategy_selection_reason(context, selected_strategy),
            "log_size_estimate": log_size_estimate,
            "cache_enabled": use_cache,
        }
        recommendations = generate_hybrid_recommendations(primary_results, context, selected_strategy)

        results = {
            "strategy_used": {
                "strategy": selected_strategy.value,
                "selection_reason": performance_metrics["strategy_selection_reason"],
                "context": {
                    "request_type": request_type,
                    "urgency": urgency,
                    "log_size_estimate": log_size_estimate,
                },
            },
            "analysis_results": primary_results,
            "supplementary_insights": supplementary_results,
            "performance_metrics": performance_metrics,
            "recommendations": recommendations,
            "cache_info": {
                "cache_hit": False,
                "cache_enabled": use_cache,
                "cache_key_generated": use_cache,
            },
        }

        if use_cache and primary_results and "error" not in primary_results:
            analysis_cache.set(namespace, pod_name, cache_key_params, results)

        return results

    except Exception as e:
        logger.error(f"[{tool_name}] Unexpected error: {e}", exc_info=True)
        return {
            "error": f"Unexpected error during hybrid log analysis: {str(e)}",
            "metadata": {
                "pod_name": pod_name,
                "namespace": namespace,
                "strategy_attempted": strategy,
                "processing_time": time.time() - start_timestamp,
            },
        }


# ---------------------------------------------------------------------------
# get_etcd_logs implementation
# ---------------------------------------------------------------------------


async def get_etcd_logs_impl(
    tail_lines,
    since_seconds,
    since_time,
    until_time,
    follow,
    timestamps,
    previous,
    clean_logs,
    *,
    k8s_core_api,
) -> "Dict[str, str]":
    """Implementation of get_etcd_logs, extracted for testability.

    Auto-detects cluster type (OpenShift vs standard Kubernetes) and uses the
    appropriate namespace / label selectors to find and fetch etcd pod logs.
    """
    import asyncio
    from datetime import datetime

    from kubernetes.client.rest import ApiException

    from helpers.utils import (
        _filter_logs_by_time_range,
        _get_logs_with_k8s_client,
        _handle_api_exception,
    )

    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}

    tool_name = "get_etcd_logs_k8s_client"
    logger.info(
        f"Tool '{tool_name}' started with params: tail_lines={tail_lines}, "
        f"since_seconds={since_seconds}, since_time={since_time}, until_time={until_time}, "
        f"follow={follow}, timestamps={timestamps}, previous={previous}, clean_logs={clean_logs}"
    )

    # ------------------------------------------------------------------
    # Input validation
    # ------------------------------------------------------------------
    parsed_since_time = None
    parsed_until_time = None

    if since_time:
        try:
            parsed_since_time = datetime.fromisoformat(since_time.replace("Z", "+00:00"))
        except ValueError as e:
            logger.error(f"[{tool_name}] Invalid since_time format: {since_time}")
            return {
                "critical_error": (
                    f"Invalid since_time format '{since_time}'. "
                    f"Use RFC3339 format (e.g., '2024-01-15T10:30:00Z'): {e}"
                )
            }

    if until_time:
        try:
            parsed_until_time = datetime.fromisoformat(until_time.replace("Z", "+00:00"))
        except ValueError as e:
            logger.error(f"[{tool_name}] Invalid until_time format: {until_time}")
            return {
                "critical_error": (
                    f"Invalid until_time format '{until_time}'. "
                    f"Use RFC3339 format (e.g., '2024-01-15T11:30:00Z'): {e}"
                )
            }

        if not since_time and not since_seconds:
            logger.error(f"[{tool_name}] until_time requires since_time or since_seconds")
            return {
                "critical_error": (
                    "until_time parameter requires either since_time or since_seconds " "to define a time range"
                )
            }

        if not timestamps:
            logger.warning(
                f"[{tool_name}] until_time specified but timestamps=False. "
                "Enabling timestamps for accurate filtering."
            )
            timestamps = True

        if parsed_since_time and parsed_until_time and parsed_until_time <= parsed_since_time:
            logger.error(f"[{tool_name}] until_time must be after since_time")
            return {
                "critical_error": (
                    f"Invalid time range: until_time ({until_time}) must be after " f"since_time ({since_time})"
                )
            }

    if since_seconds is not None and since_seconds < 0:
        logger.error(f"[{tool_name}] Invalid since_seconds: {since_seconds}")
        return {"critical_error": f"since_seconds must be non-negative, got: {since_seconds}"}

    if tail_lines is not None and tail_lines <= 0:
        logger.error(f"[{tool_name}] Invalid tail_lines: {tail_lines}")
        return {"critical_error": f"tail_lines must be positive, got: {tail_lines}"}

    accumulated_results: Dict[str, str] = {}
    strategies_attempted: List[str] = []
    logs_successfully_fetched = False

    log_params = {
        "tail_lines": tail_lines,
        "since_seconds": since_seconds,
        "since_time": since_time,
        "follow": follow,
        "timestamps": timestamps,
        "previous": previous,
        "clean_logs": clean_logs,
    }

    # ------------------------------------------------------------------
    # Strategy 1: OpenShift
    # ------------------------------------------------------------------
    os_namespace = "openshift-etcd"
    os_label_selector = "k8s-app=etcd"
    os_container = "etcd"
    strategies_attempted.append("OpenShift")

    logger.info(
        f"[{tool_name}] Attempting OpenShift etcd strategy: " f"ns='{os_namespace}', label='{os_label_selector}'"
    )
    try:
        pod_list_os = await asyncio.to_thread(
            k8s_core_api.list_namespaced_pod,
            namespace=os_namespace,
            label_selector=os_label_selector,
            timeout_seconds=10,
        )
        if pod_list_os.items:
            pod_names_os = [pod.metadata.name for pod in pod_list_os.items if pod.metadata and pod.metadata.name]
            logger.info(f"[{tool_name}] OpenShift strategy: Found {len(pod_names_os)} etcd pod(s). " "Fetching logs.")
            if await _get_logs_with_k8s_client(
                k8s_core_api,
                pod_names_os,
                os_namespace,
                os_container,
                accumulated_results,
                log_params,
            ):
                if parsed_until_time:
                    logger.info(f"[{tool_name}] Applying time range filter: until {until_time}")
                    for pod_name in list(accumulated_results.keys()):
                        if not pod_name.startswith(("error_", "info_")):
                            orig = len(accumulated_results[pod_name])
                            accumulated_results[pod_name] = _filter_logs_by_time_range(
                                accumulated_results[pod_name], parsed_until_time
                            )
                            logger.info(
                                f"[{tool_name}] Filtered logs for {pod_name}: "
                                f"{orig} -> {len(accumulated_results[pod_name])} chars"
                            )
                logger.info(f"[{tool_name}] Successfully fetched logs using OpenShift strategy")
                logs_successfully_fetched = True
            else:
                logger.warning(f"[{tool_name}] OpenShift strategy: Found pods but failed to fetch any logs")
        else:
            logger.info(f"[{tool_name}] OpenShift strategy: No etcd pods found")
            accumulated_results["info_openshift_no_pods"] = (
                f"No pods found in namespace '{os_namespace}' with label '{os_label_selector}'"
            )
    except ApiException as e:
        _handle_api_exception(
            e,
            tool_name,
            "OpenShift",
            os_namespace,
            os_label_selector,
            accumulated_results,
        )
    except Exception as e:
        logger.error(f"[{tool_name}] OpenShift strategy: Unexpected error: {e}", exc_info=True)
        accumulated_results["error_openshift_unexpected"] = str(e)

    if logs_successfully_fetched:
        return accumulated_results

    # ------------------------------------------------------------------
    # Strategy 2: Standard Kubernetes
    # ------------------------------------------------------------------
    kube_namespace = "kube-system"
    kube_label_selector = "component=etcd"
    kube_container = "etcd"
    strategies_attempted.append("StandardK8s")
    standard_k8s_results: Dict[str, str] = {}

    logger.info(
        f"[{tool_name}] Attempting standard Kubernetes etcd strategy: "
        f"ns='{kube_namespace}', label='{kube_label_selector}'"
    )
    try:
        pod_list_kube = await asyncio.to_thread(
            k8s_core_api.list_namespaced_pod,
            namespace=kube_namespace,
            label_selector=kube_label_selector,
            timeout_seconds=10,
        )
        if pod_list_kube.items:
            pod_names_kube = [pod.metadata.name for pod in pod_list_kube.items if pod.metadata and pod.metadata.name]
            logger.info(
                f"[{tool_name}] Standard K8s strategy: Found {len(pod_names_kube)} etcd pod(s). " "Fetching logs."
            )
            if await _get_logs_with_k8s_client(
                k8s_core_api,
                pod_names_kube,
                kube_namespace,
                kube_container,
                standard_k8s_results,
                log_params,
            ):
                if parsed_until_time:
                    logger.info(f"[{tool_name}] Applying time range filter: until {until_time}")
                    for pod_name in list(standard_k8s_results.keys()):
                        if not pod_name.startswith(("error_", "info_")):
                            orig = len(standard_k8s_results[pod_name])
                            standard_k8s_results[pod_name] = _filter_logs_by_time_range(
                                standard_k8s_results[pod_name], parsed_until_time
                            )
                            logger.info(
                                f"[{tool_name}] Filtered logs for {pod_name}: "
                                f"{orig} -> {len(standard_k8s_results[pod_name])} chars"
                            )
                logger.info(f"[{tool_name}] Successfully fetched logs using standard Kubernetes strategy")
                return standard_k8s_results
            else:
                logger.warning(f"[{tool_name}] Standard K8s strategy: Found pods but failed to fetch any logs")
                accumulated_results.update(standard_k8s_results)
        else:
            logger.info(f"[{tool_name}] Standard K8s strategy: No etcd pods found")
            accumulated_results["info_kube_no_pods"] = (
                f"No pods found in namespace '{kube_namespace}' with label '{kube_label_selector}'"
            )
    except ApiException as e:
        _handle_api_exception(
            e,
            tool_name,
            "StandardK8s",
            kube_namespace,
            kube_label_selector,
            accumulated_results,
        )
    except Exception as e:
        logger.error(f"[{tool_name}] Standard K8s strategy: Unexpected error: {e}", exc_info=True)
        accumulated_results["error_kube_unexpected"] = str(e)

    # ------------------------------------------------------------------
    # Final summary when no logs were retrieved
    # ------------------------------------------------------------------
    has_actual_logs = any(not key.startswith(("error_", "info_", "critical_")) for key in accumulated_results)
    if not has_actual_logs:
        summary_message = (
            f"Failed to fetch etcd logs from any cluster type. "
            f"Attempted strategies: {', '.join(strategies_attempted)}. "
            "Check RBAC permissions and cluster configuration."
        )
        if not accumulated_results:
            accumulated_results["final_summary"] = summary_message
        else:
            final_results = {"final_summary": summary_message}
            final_results.update(accumulated_results)
            accumulated_results = final_results

    logger.info(f"[{tool_name}] Log fetching complete. Results: {len(accumulated_results)} entries")
    return accumulated_results
