"""
Prometheus result formatting and query helper utilities.

These are pure formatting/string-manipulation functions extracted from server-mcp.py.
They have no dependency on module-level server state (no PrometheusEndpointCache,
no k8s_core_api, no mcp instance).
"""

import csv
import io
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("lumino-mcp")


def parse_time_parameter(time_param: str) -> str:
    """Parse time parameter to Unix timestamp for Prometheus API."""
    try:
        if time_param.isdigit():
            return time_param
        if "T" in time_param:
            dt = datetime.fromisoformat(time_param.replace("Z", "+00:00"))
            return str(int(dt.timestamp()))
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
            try:
                dt = datetime.strptime(time_param, fmt)
                return str(int(dt.timestamp()))
            except ValueError:
                continue
        return time_param
    except Exception as e:
        logger.warning(f"Error parsing time parameter '{time_param}': {e}")
        return time_param


def format_metric_value(metric_name: str, value: Optional[str]) -> str:
    """Format metric value with appropriate units."""
    if value is None:
        return "N/A"
    try:
        numeric_value = float(value)
        if "cpu" in metric_name.lower():
            if "seconds" in metric_name.lower():
                return f"{numeric_value:.3f} CPU seconds"
            else:
                return f"{numeric_value:.3f} CPU cores"
        elif "memory" in metric_name.lower() or "bytes" in metric_name.lower():
            if numeric_value >= 1024**3:
                return f"{numeric_value / (1024**3):.2f} GB"
            elif numeric_value >= 1024**2:
                return f"{numeric_value / (1024**2):.2f} MB"
            elif numeric_value >= 1024:
                return f"{numeric_value / 1024:.2f} KB"
            else:
                return f"{numeric_value:.0f} bytes"
        elif "percentage" in metric_name.lower() or "percent" in metric_name.lower():
            return f"{numeric_value:.1f}%"
        else:
            return f"{numeric_value:.3f}"
    except (ValueError, TypeError):
        return str(value)


def format_as_table(results: List[Dict], result_type: str) -> str:
    """Format results as a human-readable table."""
    if not results:
        return "No data returned"
    try:
        if result_type == "vector":
            headers = ["Metric"] + list(results[0].get("metric", {}).keys()) + ["Value"]
            rows = []
            for result in results:
                metric = result.get("metric", {})
                value = (
                    result.get("value", ["", ""])[1] if result.get("value") else "N/A"
                )
                metric_name = metric.get("__name__", "")
                row = (
                    [metric_name]
                    + [metric.get(key, "") for key in headers[1:-1]]
                    + [value]
                )
                rows.append(row)
        elif result_type == "matrix":
            headers = ["Metric", "Namespace", "Values (timestamp:value)"]
            rows = []
            for result in results:
                metric = result.get("metric", {})
                values = result.get("values", [])
                metric_name = metric.get("__name__", "")
                namespace = metric.get("namespace", "")
                value_pairs = [f"{ts}:{val}" for ts, val in values[:5]]
                if len(values) > 5:
                    value_pairs.append(f"... ({len(values) - 5} more)")
                rows.append([metric_name, namespace, ", ".join(value_pairs)])
        else:
            return f"Unsupported result type for table format: {result_type}"
        if not rows:
            return "No data to display"
        col_widths = [
            max(len(str(header)), max(len(str(row[i])) for row in rows))
            for i, header in enumerate(headers)
        ]
        table_lines = []
        header_line = " | ".join(
            header.ljust(col_widths[i]) for i, header in enumerate(headers)
        )
        table_lines.append(header_line)
        table_lines.append("-" * len(header_line))
        for row in rows:
            row_line = " | ".join(
                str(row[i]).ljust(col_widths[i]) for i in range(len(headers))
            )
            table_lines.append(row_line)
        return "\n".join(table_lines)
    except Exception as e:
        logger.error(f"Error formatting table: {e}")
        return f"Error formatting table: {e}"


def format_as_csv(results: List[Dict], result_type: str) -> str:
    """Format results as CSV."""
    if not results:
        return "No data returned"
    try:
        output = io.StringIO()
        if result_type == "vector":
            fieldnames = (
                ["metric_name"]
                + list(results[0].get("metric", {}).keys())
                + ["value", "timestamp"]
            )
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                metric = result.get("metric", {})
                value_data = result.get("value", ["", ""])
                row: Dict[str, Any] = {
                    "metric_name": metric.get("__name__", ""),
                    "value": value_data[1] if len(value_data) > 1 else "",
                    "timestamp": value_data[0] if len(value_data) > 0 else "",
                }
                row.update({k: v for k, v in metric.items() if k != "__name__"})
                writer.writerow(row)
        elif result_type == "matrix":
            fieldnames2 = ["metric_name", "namespace", "timestamp", "value"]
            if results:
                additional_labels: set = set()
                for result in results:
                    metric = result.get("metric", {})
                    additional_labels.update(
                        k for k in metric.keys() if k not in ["__name__", "namespace"]
                    )
                fieldnames2.extend(sorted(additional_labels))
            writer2 = csv.DictWriter(output, fieldnames=fieldnames2)
            writer2.writeheader()
            for result in results:
                metric = result.get("metric", {})
                values = result.get("values", [])
                base_row: Dict[str, Any] = {
                    "metric_name": metric.get("__name__", ""),
                    "namespace": metric.get("namespace", ""),
                }
                base_row.update(
                    {
                        k: v
                        for k, v in metric.items()
                        if k not in ["__name__", "namespace"]
                    }
                )
                for timestamp, value in values:
                    r = base_row.copy()
                    r.update({"timestamp": timestamp, "value": value})
                    writer2.writerow(r)
        return output.getvalue()
    except Exception as e:
        logger.error(f"Error formatting CSV: {e}")
        return f"Error formatting CSV: {e}"


def format_as_json(results: List[Dict], result_type: str) -> List[Dict]:
    """Format results as structured JSON."""
    try:
        formatted_results = []
        for result in results:
            metric = result.get("metric", {})
            if result_type == "vector":
                value_data = result.get("value", [])
                formatted_result: Dict[str, Any] = {
                    "metric": metric,
                    "value": value_data[1] if len(value_data) > 1 else None,
                    "timestamp": value_data[0] if len(value_data) > 0 else None,
                    "formatted_value": format_metric_value(
                        metric.get("__name__", ""),
                        value_data[1] if len(value_data) > 1 else None,
                    ),
                }
            elif result_type == "matrix":
                values = result.get("values", [])
                total_count = len(values)
                numeric_values = []
                for v in values:
                    try:
                        numeric_values.append(float(v[1]))
                    except (ValueError, TypeError, IndexError):
                        pass  # Skip non-numeric Prometheus values
                stats: Dict[str, Any] = {}
                if numeric_values:
                    sorted_vals = sorted(numeric_values)
                    stats = {
                        "min": round(min(numeric_values), 4),
                        "max": round(max(numeric_values), 4),
                        "avg": round(sum(numeric_values) / len(numeric_values), 4),
                        "latest": round(numeric_values[-1], 4),
                        "first": round(numeric_values[0], 4),
                        "p50": round(sorted_vals[len(sorted_vals) // 2], 4),
                        "p95": (
                            round(sorted_vals[int(len(sorted_vals) * 0.95)], 4)
                            if len(sorted_vals) > 1
                            else round(sorted_vals[0], 4)
                        ),
                    }
                MAX_DATAPOINTS = 50
                sampled_values = []
                if total_count > MAX_DATAPOINTS:
                    step = total_count / MAX_DATAPOINTS
                    for i in range(MAX_DATAPOINTS):
                        idx = int(i * step)
                        sampled_values.append(values[idx])
                else:
                    sampled_values = values
                formatted_result = {
                    "metric": metric,
                    "statistics": stats,
                    "values": sampled_values,
                    "value_count": total_count,
                    "sampled_count": len(sampled_values),
                    "downsampled": total_count > MAX_DATAPOINTS,
                    "time_range": {
                        "start": values[0][0] if values else None,
                        "end": values[-1][0] if values else None,
                    },
                }
            else:
                formatted_result = result
            formatted_results.append(formatted_result)
        return formatted_results
    except Exception as e:
        logger.error(f"Error formatting JSON: {e}")
        return [{"error": f"Error formatting results: {e}"}]


def generate_result_summary(results: List[Dict], result_type: str, query: str) -> str:
    """Generate human-readable summary of query results."""
    if not results:
        return f"No data returned for query: {query}"
    try:
        summary_parts = []
        summary_parts.append(f"Found {len(results)} metric series")
        namespaces: set = set()
        for result in results:
            metric = result.get("metric", {})
            if "namespace" in metric:
                namespaces.add(metric["namespace"])
        if namespaces:
            summary_parts.append(
                f"across {len(namespaces)} namespaces: {', '.join(sorted(list(namespaces))[:5])}"
            )
            if len(namespaces) > 5:
                summary_parts[-1] += f" and {len(namespaces) - 5} more"
        metric_names: set = set()
        for result in results:
            metric = result.get("metric", {})
            if "__name__" in metric:
                metric_names.add(metric["__name__"])
        if metric_names:
            summary_parts.append(
                f"Metric types: {', '.join(sorted(list(metric_names))[:3])}"
            )
            if len(metric_names) > 3:
                summary_parts[-1] += f" and {len(metric_names) - 3} more"
        return ". ".join(summary_parts) + "."
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        return f"Query returned {len(results)} results"


def generate_query_suggestions(query: str, error_message: str) -> List[str]:
    """Generate helpful suggestions based on query and error."""
    suggestions = []
    if "parse error" in error_message.lower():
        suggestions.extend(
            [
                "Check PromQL syntax - ensure proper use of operators and functions",
                "Verify metric names and label selectors are correctly formatted",
                'Example: up{job="node-exporter"} or rate(http_requests_total[5m])',
            ]
        )
    if (
        "unknown metric" in error_message.lower()
        or "not found" in error_message.lower()
    ):
        suggestions.extend(
            [
                "Check if the metric name is spelled correctly",
                'Try querying available metrics with: {__name__=~".*"}',
                "Verify the metric is actually being scraped by Prometheus",
            ]
        )
    if "timeout" in error_message.lower():
        suggestions.extend(
            [
                "Try a shorter time range for range queries",
                "Use more specific label selectors to reduce data volume",
                "Consider using recording rules for complex queries",
            ]
        )
    if "rate(" in query and "[" not in query:
        suggestions.append("rate() function requires a time range: rate(metric[5m])")
    if "{" in query and "}" in query:
        if "=~" in query:
            suggestions.append("Ensure regex patterns are valid and properly escaped")
    if not suggestions:
        suggestions.extend(
            [
                "Check Prometheus documentation for correct PromQL syntax",
                "Try a simpler query first to test connectivity",
                "Verify you have access to the metrics you're querying",
            ]
        )
    return suggestions


def generate_related_query_suggestions(
    original_query: str, results: List[Dict]
) -> List[str]:
    """Generate suggestions for related queries based on results."""
    suggestions = []
    try:
        if not results:
            suggestions.extend(
                [
                    "Try expanding the time range if using a range query",
                    'Check if the metric exists: {__name__=~".*metric_name.*"}',
                    'List all available metrics: {__name__=~".*"}',
                ]
            )
            return suggestions
        metric_names: set = set()
        namespaces: set = set()
        for result in results:
            metric = result.get("metric", {})
            if "__name__" in metric:
                metric_names.add(metric["__name__"])
            if "namespace" in metric:
                namespaces.add(metric["namespace"])
        if metric_names:
            example_metric = list(metric_names)[0]
            if "cpu" in example_metric:
                suggestions.append(
                    "Related memory usage: sum(container_memory_working_set_bytes) by (namespace)"
                )
            elif "memory" in example_metric:
                suggestions.append(
                    "Related CPU usage: sum(rate(container_cpu_usage_seconds_total[5m])) by (namespace)"
                )
            if "rate(" not in original_query and "_total" in example_metric:
                suggestions.append(f"Rate calculation: rate({example_metric}[5m])")
        if namespaces and len(namespaces) > 1:
            suggestions.append(
                f'Filter by specific namespace: {{namespace="{list(namespaces)[0]}"}}'
            )
        if "topk(" not in original_query:
            suggestions.append(f"Top 10 results: topk(10, {original_query})")
        if "range" not in original_query:
            suggestions.append(f"Historical data: {original_query} over time range")
    except Exception as e:
        logger.error(f"Error generating related suggestions: {e}")
    return suggestions[:5]


def filter_analysis_for_synthesis(
    pod_analysis: Dict[str, Any], focus_areas: List[str]
) -> Dict[str, Any]:
    """
    Filter pod analysis results to keep only essential data for synthesis, preventing token overflow.

    Args:
        pod_analysis: Full pod analysis results
        focus_areas: Areas to focus on for filtering

    Returns:
        Filtered analysis with only essential data
    """
    try:
        filtered: Dict[str, Any] = {
            "summary": pod_analysis.get("summary", {}),
            "metadata": {
                "total_log_lines": pod_analysis.get("metadata", {})
                .get("processing_metrics", {})
                .get("total_log_lines", 0),
                "patterns_extracted": pod_analysis.get("metadata", {})
                .get("processing_metrics", {})
                .get("patterns_extracted", 0),
                "processing_time_seconds": pod_analysis.get("metadata", {})
                .get("processing_metrics", {})
                .get("processing_time_seconds", 0),
            },
        }
        if "patterns" in pod_analysis:
            filtered["patterns"] = {}
            for area in focus_areas:
                if area in pod_analysis["patterns"] and pod_analysis["patterns"][area]:
                    filtered["patterns"][area] = pod_analysis["patterns"][area][:3]
        if "representative_samples" in pod_analysis:
            filtered["representative_samples"] = {}
            for area in focus_areas:
                if area in pod_analysis["representative_samples"]:
                    filtered["representative_samples"][area] = pod_analysis[
                        "representative_samples"
                    ][area][:2]
        return filtered
    except Exception as e:
        logger.warning(f"Error filtering analysis: {e}")
        return {
            "summary": pod_analysis.get(
                "summary", "Analysis available but filtered due to size"
            ),
            "metadata": {"filtered": True, "reason": "token_overflow_prevention"},
        }


def compress_events_for_synthesis(events_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compress event analysis results to essential information for synthesis.

    Args:
        events_result: Full event analysis results

    Returns:
        Compressed events data with only essential information
    """
    try:
        if not events_result or "error" in events_result:
            return events_result
        compressed: Dict[str, Any] = {
            "namespace": events_result.get("namespace"),
            "strategy_used": events_result.get("strategy_used"),
            "total_events": events_result.get("total_events", 0),
            "processed_events": events_result.get("processed_events", 0),
        }
        if "events" in events_result and events_result["events"]:
            sorted_events = sorted(
                events_result["events"],
                key=lambda e: (
                    e.get("severity") == "CRITICAL",
                    e.get("relevance_score", 0),
                ),
                reverse=True,
            )
            compressed["critical_events"] = sorted_events[:5]
        if "summary" in events_result:
            compressed["summary"] = events_result["summary"]
        if "insights" in events_result:
            compressed["insights"] = events_result["insights"][:3]
        if "recommendations" in events_result:
            compressed["recommendations"] = events_result["recommendations"][:3]
        return compressed
    except Exception as e:
        logger.warning(f"Error compressing events: {e}")
        return {
            "compressed": True,
            "total_events": events_result.get("total_events", 0),
        }
