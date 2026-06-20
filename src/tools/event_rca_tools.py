"""Kubernetes namespace event fetching tools — extracted from server-mcp.py.

Each function accepts injected Kubernetes API clients and helpers rather than
relying on module-level globals.  The thin ``@mcp.tool()`` wrappers that remain
in ``server-mcp.py`` simply forward to these implementations.

Fixes #80
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from kubernetes.client.rest import ApiException

from helpers.constants import SMART_EVENTS_CONFIG
from helpers.event_analysis import EventCategory, EventSeverity
from helpers.failure_analysis import (
    analyze_configuration_issues,
    analyze_generic_failure,
    analyze_pipeline_failure,
    analyze_pod_failure,
    analyze_resource_constraints,
    assess_failure_severity,
    build_failure_timeline,
    calculate_confidence_score,
    find_related_failures,
    generate_remediation_plan,
    identify_failure_context,
    perform_advanced_rca,
)
from helpers.utils import parse_time_period

logger = logging.getLogger("lumino-mcp-server")


async def _get_namespace_events_internal(
    namespace: str,
    last_n_events: Optional[int] = None,
    time_period: Optional[str] = None,
    max_fetch_limit: int = 5000,
    *,
    k8s_core_api: Any,
) -> Dict[str, Any]:
    """
    Internal function to fetch Kubernetes events from a namespace with optional filtering.

    Uses pagination to handle large event volumes efficiently and prevent connection timeouts.

    Args:
        namespace: Kubernetes namespace to fetch events from
        last_n_events: Limit to last N events (optional)
        time_period: Time period like '1h', '30m', '2d' (optional)
        max_fetch_limit: Maximum events to fetch per page
        k8s_core_api: Injected Kubernetes CoreV1Api client

    Returns:
        Dictionary with events list and metadata
    """
    logger.info(f"Fetching events from namespace '{namespace}'")
    if last_n_events is not None:
        logger.info(f"Will filter to last {last_n_events} events")
    if time_period is not None:
        logger.info(f"Will filter to events from last {time_period}")

    output: Dict[str, Any] = {
        "namespace": namespace,
        "events": [],
        "errors": [],
        "applied_filters": {},
    }
    events_list: List[str] = []
    errors_list: List[str] = []

    try:
        cutoff_time = None
        if time_period is not None:
            try:
                time_delta = parse_time_period(time_period)
                cutoff_time = datetime.now() - time_delta
                output["applied_filters"]["time_period"] = time_period
                output["applied_filters"]["cutoff_time"] = cutoff_time.isoformat()
            except Exception as e:
                errors_list.append(f"Error parsing time period: {str(e)}")
                logger.error(f"Error parsing time period: {e}")

        all_events = []
        continue_token = None
        page_count = 0
        MAX_PAGES = 20

        logger.info(
            f"Fetching events with pagination (limit={max_fetch_limit} per page)"
        )

        while page_count < MAX_PAGES:
            try:
                if continue_token:
                    event_list_response = await asyncio.to_thread(
                        k8s_core_api.list_namespaced_event,
                        namespace=namespace,
                        watch=False,
                        limit=max_fetch_limit,
                        _continue=continue_token,
                    )
                else:
                    event_list_response = await asyncio.to_thread(
                        k8s_core_api.list_namespaced_event,
                        namespace=namespace,
                        watch=False,
                        limit=max_fetch_limit,
                    )

                page_count += 1
                page_events = len(event_list_response.items)
                all_events.extend(event_list_response.items)

                logger.info(
                    f"Fetched page {page_count}: {page_events} events (total: {len(all_events)})"
                )

                continue_token = event_list_response.metadata._continue

                if not continue_token:
                    logger.info(f"All events fetched ({len(all_events)} total)")
                    break

                if last_n_events and len(all_events) >= last_n_events * 2:
                    logger.info("Fetched sufficient events for filtering")
                    break

                if cutoff_time and event_list_response.items:

                    def get_event_time(event):
                        timestamp = event.last_timestamp or event.first_timestamp
                        if timestamp is None:
                            return datetime.max
                        if timestamp.tzinfo is not None:
                            return timestamp.replace(tzinfo=None)
                        return timestamp

                    oldest_in_page = min(event_list_response.items, key=get_event_time)
                    oldest_time = get_event_time(oldest_in_page)

                    if oldest_time < cutoff_time:
                        logger.info("Reached events older than cutoff time")
                        break

            except ApiException as e:
                if e.status == 410:
                    logger.warning(f"Continue token expired at page {page_count}")
                    break
                else:
                    raise

        if page_count >= MAX_PAGES and continue_token:
            logger.warning(f"Reached maximum page limit ({MAX_PAGES} pages)")
            errors_list.append(
                f"Event fetching limited to {len(all_events)} events due to volume."
            )

        original_count = len(all_events)
        logger.info(f"Found {original_count} events in namespace '{namespace}'")

        def get_comparable_timestamp(event):
            timestamp = event.last_timestamp or event.first_timestamp
            if timestamp is None:
                return datetime.min.replace(tzinfo=None)
            if timestamp.tzinfo is not None:
                return timestamp.replace(tzinfo=None)
            return timestamp

        events = sorted(all_events, key=get_comparable_timestamp, reverse=True)

        if time_period is not None and cutoff_time is not None:
            filtered_events = []
            for event in events:
                event_time = get_comparable_timestamp(event)
                if event_time >= cutoff_time:
                    filtered_events.append(event)
            events = filtered_events
            logger.info(f"Filtered to {len(events)} events after time period filter")

        if last_n_events is not None and len(events) > last_n_events:
            events = events[:last_n_events]
            output["applied_filters"]["last_n_events"] = last_n_events
            logger.info(f"Limited to last {last_n_events} events")

        for event in events:
            try:
                timestamp = event.last_timestamp or event.first_timestamp or "Unknown"
                event_str = (
                    f"[{timestamp}] {event.type}: {event.reason} - {event.message}"
                )
                if event.involved_object:
                    event_str += f" (Object: {event.involved_object.kind}/{event.involved_object.name})"
                events_list.append(event_str)
            except Exception as e:
                errors_list.append(f"Error formatting event: {str(e)}")
                logger.error(f"Error formatting event: {e}")

        output["events"] = events_list
        output["errors"] = errors_list
        output["original_events_count"] = original_count
        output["filtered_events_count"] = len(events_list)
        output["pagination_info"] = {
            "pages_fetched": page_count,
            "hit_page_limit": page_count >= MAX_PAGES and continue_token is not None,
        }

        logger.info(f"Returning {len(events_list)} formatted events")
        return output

    except Exception as e:
        error_msg = f"Failed to fetch events from namespace '{namespace}': {str(e)}"
        logger.error(error_msg)
        return {
            "namespace": namespace,
            "events": [],
            "errors": [error_msg],
            "applied_filters": {},
        }


async def _get_namespace_events_as_dicts(
    namespace: str,
    limit: int = 100,
    time_period: Optional[str] = None,
    *,
    k8s_core_api: Any,
) -> List[Dict[str, Any]]:
    """
    Fetch Kubernetes events as dictionaries for use with FailureEventCollector.

    Unlike _get_namespace_events_internal which returns formatted strings,
    this function returns raw event data as dictionaries.

    Args:
        namespace: Kubernetes namespace to fetch events from
        limit: Maximum number of events to fetch
        time_period: Optional time period like '1h', '30m', '2d'
        k8s_core_api: Injected Kubernetes CoreV1Api client

    Returns:
        List of event dictionaries with keys: type, reason, message,
        involved_object, last_timestamp, first_timestamp, count, name
    """
    events_as_dicts: List[Dict[str, Any]] = []

    try:
        cutoff_time = None
        if time_period is not None:
            try:
                time_delta = parse_time_period(time_period)
                cutoff_time = datetime.now() - time_delta
            except Exception as e:
                logger.debug(f"Error parsing time period: {e}")

        event_list_response = await asyncio.to_thread(
            k8s_core_api.list_namespaced_event,
            namespace=namespace,
            watch=False,
            limit=limit,
        )

        for event in event_list_response.items:
            try:
                if cutoff_time:
                    event_time = event.last_timestamp or event.first_timestamp
                    if event_time:
                        if event_time.tzinfo is not None:
                            event_time = event_time.replace(tzinfo=None)
                        if event_time < cutoff_time:
                            continue

                event_dict = {
                    "type": event.type or "Normal",
                    "reason": event.reason or "",
                    "message": event.message or "",
                    "name": event.metadata.name if event.metadata else "",
                    "last_timestamp": (
                        event.last_timestamp.isoformat()
                        if event.last_timestamp
                        else None
                    ),
                    "first_timestamp": (
                        event.first_timestamp.isoformat()
                        if event.first_timestamp
                        else None
                    ),
                    "count": event.count or 1,
                    "involved_object": {},
                }

                if event.involved_object:
                    event_dict["involved_object"] = {
                        "name": event.involved_object.name or "",
                        "kind": event.involved_object.kind or "",
                        "namespace": event.involved_object.namespace or namespace,
                        "uid": event.involved_object.uid or "",
                    }

                events_as_dicts.append(event_dict)

            except Exception as e:
                logger.debug(f"Error converting event to dict: {e}")
                continue

        logger.debug(f"Fetched {len(events_as_dicts)} events as dicts from {namespace}")
        return events_as_dicts

    except Exception as e:
        logger.debug(f"Failed to fetch events as dicts from {namespace}: {e}")
        return []


async def smart_get_namespace_events_impl(
    namespace: str,
    last_n_events: Optional[int] = None,
    time_period: Optional[str] = None,
    strategy: str = "auto",
    focus_areas: Optional[List[str]] = None,
    max_context_tokens: int = 8000,
    include_summary: bool = True,
    *,
    k8s_core_api: Any,
    smart_sample_string_events_fn: Any,
    generate_string_events_summary_fn: Any,
    generate_string_events_insights_fn: Any,
    generate_string_events_recommendations_fn: Any,
) -> Dict[str, Any]:
    """
    Adaptive event analysis for a namespace with automatic volume management.

    When no constraints are specified, automatically estimates volume, applies
    smart time windows, prioritizes errors/warnings, and samples within token limits.

    Args:
        namespace: Kubernetes namespace to analyze.
        last_n_events: Exact event count (only if user specifies).
        time_period: Exact time window (only if user specifies).
        strategy: "auto" for adaptive behavior (default).
        focus_areas: Areas to emphasize (default: ["errors", "warnings", "failures"]).
        max_context_tokens: Max output tokens (default: 8000).
        include_summary: Include summary and insights (default: True).
        k8s_core_api: Injected Kubernetes CoreV1Api client.
        smart_sample_string_events_fn: Injected smart sampling helper.
        generate_string_events_summary_fn: Injected summary generation helper.
        generate_string_events_insights_fn: Injected insights generation helper.
        generate_string_events_recommendations_fn: Injected recommendations generation helper.

    Returns:
        Dict: Events with adaptive filtering, insights, and recommendations.
    """
    if focus_areas is None:
        focus_areas = ["errors", "warnings", "failures"]

    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}

    tool_name = "smart_get_namespace_events"
    logger.info(
        f"[{tool_name}] Starting smart event analysis for namespace '{namespace}'"
    )

    try:
        if not namespace or not namespace.strip():
            return {"error": "Namespace cannot be empty"}

        if max_context_tokens < 1000:
            logger.warning(
                f"[{tool_name}] Low token limit ({max_context_tokens}), setting to 1000"
            )
            max_context_tokens = 1000

        if strategy == "auto":
            strategy = "smart_summary"
            logger.info(f"[{tool_name}] Auto-selected strategy: {strategy}")

        if last_n_events is None and time_period is None:
            logger.info(f"[{tool_name}] No filters provided - activating ADAPTIVE MODE")

            try:
                recent_sample = await _get_namespace_events_internal(
                    namespace=namespace,
                    time_period="10m",
                    k8s_core_api=k8s_core_api,
                )

                sample_count = recent_sample.get("filtered_events_count", 0)
                estimated_hourly_events = sample_count * 6

                if estimated_hourly_events > 500:
                    time_period = "30m"
                    logger.info(
                        f"[{tool_name}] HIGH EVENT VOLUME detected (~{estimated_hourly_events}/hour) - using 30min window"
                    )
                    if "errors" not in focus_areas:
                        focus_areas = ["errors", "warnings"] + [
                            f for f in focus_areas if f not in ["errors", "warnings"]
                        ]
                elif estimated_hourly_events > 50:
                    time_period = "2h"
                    logger.info(
                        f"[{tool_name}] MEDIUM EVENT VOLUME detected (~{estimated_hourly_events}/hour) - using 2h window"
                    )
                else:
                    time_period = "6h"
                    logger.info(
                        f"[{tool_name}] LOW EVENT VOLUME detected (~{estimated_hourly_events}/hour) - using 6h window"
                    )

            except Exception as e:
                logger.warning(
                    f"[{tool_name}] Volume estimation failed, using safe default: {e}"
                )
                time_period = SMART_EVENTS_CONFIG["defaults"]["default_time_window"]

            logger.info(
                f"[{tool_name}] ADAPTIVE STRATEGY selected: {time_period} time window"
            )

        logger.info(
            f"[{tool_name}] Fetching events with filters: last_n={last_n_events}, time_period={time_period}"
        )

        raw_result = await _get_namespace_events_internal(
            namespace=namespace,
            last_n_events=last_n_events,
            time_period=time_period,
            k8s_core_api=k8s_core_api,
        )

        if "errors" in raw_result and raw_result["errors"]:
            return {"error": f"Failed to fetch events: {raw_result['errors']}"}

        events_count = raw_result.get("filtered_events_count", 0)
        events_list = raw_result.get("events", [])

        if focus_areas != ["errors", "warnings", "failures"] and events_list:
            focus_keywords = {
                "errors": ["error", "failed", "fatal", "crash", "backoff", "oom"],
                "warnings": ["warning", "warn", "unhealthy", "evict"],
                "failures": ["failed", "error", "backoff", "crashloop"],
            }
            keywords = set()
            for area in focus_areas:
                keywords.update(focus_keywords.get(area, []))
            if keywords:
                pre_filter_count = len(events_list)
                filtered = []
                for e in events_list:
                    text = e.get("event_string", e) if isinstance(e, dict) else str(e)
                    text_lower = text.lower()
                    if any(kw in text_lower for kw in keywords):
                        filtered.append(e)
                if filtered:
                    events_list = filtered
                    events_count = len(events_list)
                    logger.info(
                        f"[{tool_name}] focus_areas filter: {pre_filter_count} → {events_count} events"
                    )

        logger.info(
            f"[{tool_name}] Retrieved {events_count} events, processing with strategy: {strategy}"
        )

        if strategy == "smart_summary":

            if not events_list:
                return {
                    "namespace": namespace,
                    "strategy_used": "smart_summary",
                    "total_events": 0,
                    "processed_events": 0,
                    "events": [],
                    "summary": {
                        "total_events": 0,
                        "message": "No events found in the specified timeframe",
                    },
                    "insights": [
                        "No events found - this could indicate either a quiet period or issues with event generation"
                    ],
                    "recommendations": [
                        "Verify that applications are generating events as expected"
                    ],
                    "token_usage": {"total_estimated": 200},
                    "applied_filters": raw_result.get("applied_filters", {}),
                    "smart_features": {
                        "intelligent_defaults": (
                            time_period if last_n_events is None else None
                        ),
                        "context_overflow_prevention": True,
                        "focus_areas": focus_areas,
                    },
                }

            selected_events = smart_sample_string_events_fn(
                events_list, focus_areas, max_context_tokens
            )

            summary = {}
            if include_summary:
                summary = generate_string_events_summary_fn(
                    selected_events, focus_areas
                )

            insights = generate_string_events_insights_fn(selected_events)
            recommendations = generate_string_events_recommendations_fn(selected_events)

            total_tokens = sum(event["token_estimate"] for event in selected_events)
            summary_tokens = len(str(summary).split()) * 1.3 if summary else 0
            metadata_tokens = 200

            return {
                "namespace": namespace,
                "strategy_used": "smart_summary",
                "total_events": events_count,
                "processed_events": len(selected_events),
                "events": [
                    {
                        "event_string": event["event_string"],
                        "severity": event["severity"],
                        "category": event["category"],
                        "relevance_score": round(event["relevance_score"], 2),
                        "timestamp": event["timestamp"].isoformat(),
                        "token_estimate": event["token_estimate"],
                    }
                    for event in selected_events
                ],
                "summary": summary,
                "insights": insights,
                "recommendations": recommendations,
                "token_usage": {
                    "events_tokens": int(total_tokens),
                    "summary_tokens": int(summary_tokens),
                    "metadata_tokens": metadata_tokens,
                    "total_estimated": int(
                        total_tokens + summary_tokens + metadata_tokens
                    ),
                },
                "applied_filters": raw_result.get("applied_filters", {}),
                "smart_features": {
                    "intelligent_defaults": (
                        time_period if last_n_events is None else None
                    ),
                    "context_overflow_prevention": True,
                    "focus_areas": focus_areas,
                    "classification_applied": True,
                    "smart_sampling": True,
                },
                "classification_metadata": {
                    "severity_distribution": {
                        severity.value: len(
                            [
                                e
                                for e in selected_events
                                if e["severity"] == severity.value
                            ]
                        )
                        for severity in EventSeverity
                    },
                    "category_distribution": {
                        category.value: len(
                            [
                                e
                                for e in selected_events
                                if e["category"] == category.value
                            ]
                        )
                        for category in EventCategory
                    },
                },
            }

        elif strategy == "raw":
            max_raw = SMART_EVENTS_CONFIG["defaults"]["max_events_raw"]
            return {
                "namespace": namespace,
                "strategy_used": "raw_limited",
                "total_events": events_count,
                "processed_events": min(events_count, max_raw),
                "events": events_list[:max_raw] if events_list else [],
                "applied_limits": {
                    "max_raw_events": max_raw,
                    "truncated": events_count > max_raw,
                },
                "token_usage": {"total_estimated": min(events_count, max_raw) * 60},
                "note": "Raw strategy with safety limits applied to prevent context overflow",
            }

        else:  # progressive or fallback
            return {
                "namespace": namespace,
                "strategy_used": "progressive",
                "total_events": events_count,
                "note": "Progressive analysis strategy - showing overview",
                "events_overview": {
                    "total_found": events_count,
                    "time_period": time_period,
                    "preview": events_list[:5] if events_list else [],
                    "suggestion": "Use smart_summary strategy for detailed analysis",
                },
                "quick_insights": [
                    f"Found {events_count} events in namespace '{namespace}'",
                    "Use 'smart_summary' strategy for intelligent analysis",
                    "Progressive disclosure enables drilling down into specific issues",
                ],
            }

    except Exception as e:
        logger.error(f"[{tool_name}] Unexpected error: {str(e)}", exc_info=True)
        return {
            "error": f"Smart event analysis failed: {str(e)}",
            "fallback_suggestion": "Try using the original get_namespace_events tool with explicit filters",
        }


async def progressive_event_analysis_impl(
    namespace: str,
    analysis_level: str = "overview",
    time_period: Optional[str] = None,
    event_filters: Optional[Dict[str, Any]] = None,
    seed_event_id: Optional[str] = None,
    focus_areas: Optional[List[str]] = None,
    *,
    k8s_core_api: Any,
    smart_get_namespace_events_fn: Any,
    progressive_event_analyzer_cls: Any,
) -> Dict[str, Any]:
    """
    Implementation of progressive_event_analysis extracted from server-mcp.py.

    Performs progressive event analysis with multiple detail levels and correlation
    detection. Delegates to the injected smart_get_namespace_events_fn for event
    fetching and uses progressive_event_analyzer_cls for multi-level analysis.

    Args:
        namespace: Kubernetes namespace to analyze.
        analysis_level: "overview", "detailed", "correlation", or "deep_dive".
        time_period: Time window (e.g., "2h", "4h", "1d").
        event_filters: Filters like {"severity": ["CRITICAL"], "category": ["FAILURE"]}.
        seed_event_id: Event ID for correlation analysis.
        focus_areas: Areas to emphasize (default: ["errors", "warnings", "failures"]).
        k8s_core_api: Injected Kubernetes CoreV1Api client.
        smart_get_namespace_events_fn: Injected smart event fetching coroutine.
        progressive_event_analyzer_cls: Injected ProgressiveEventAnalyzer class.

    Returns:
        Dict: Analysis results based on selected level.
    """
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}

    if focus_areas is None:
        focus_areas = ["errors", "warnings", "failures"]

    tool_name = "progressive_event_analysis"
    logger.info(
        f"[{tool_name}] Starting {analysis_level} analysis for namespace '{namespace}'"
    )

    try:
        smart_result = await smart_get_namespace_events_fn(
            namespace=namespace,
            time_period=time_period,
            strategy="smart_summary",
            focus_areas=focus_areas,
            include_summary=False,
        )

        if "error" in smart_result:
            return {"error": f"Failed to fetch events: {smart_result['error']}"}

        classified_events = []
        for event in smart_result.get("events", []):
            classified_events.append(
                {
                    "event_string": event.get("event_string", ""),
                    "severity": event.get("severity"),
                    "category": event.get("category"),
                    "relevance_score": event.get("relevance_score", 0),
                    "timestamp": datetime.fromisoformat(
                        event.get("timestamp", datetime.now().isoformat())
                    ),
                    "token_estimate": event.get("token_estimate", 0),
                }
            )

        original_period = time_period or "default"
        if not classified_events:
            fallback_periods = ["12h", "24h", "7d"]
            for fallback_period in fallback_periods:
                if fallback_period == time_period:
                    continue
                logger.info(
                    f"[{tool_name}] No events with {original_period}, trying {fallback_period}"
                )
                fallback_result = await smart_get_namespace_events_fn(
                    namespace=namespace,
                    time_period=fallback_period,
                    strategy="smart_summary",
                    focus_areas=focus_areas,
                    include_summary=False,
                )
                for event in fallback_result.get("events", []):
                    classified_events.append(
                        {
                            "event_string": event.get("event_string", ""),
                            "severity": event.get("severity"),
                            "category": event.get("category"),
                            "relevance_score": event.get("relevance_score", 0),
                            "timestamp": datetime.fromisoformat(
                                event.get("timestamp", datetime.now().isoformat())
                            ),
                            "token_estimate": event.get("token_estimate", 0),
                        }
                    )
                if classified_events:
                    logger.info(
                        f"[{tool_name}] No events for '{original_period}', expanded to '{fallback_period}' ({len(classified_events)} events)"
                    )
                    time_period = fallback_period
                    break

            if not classified_events:
                return {
                    "namespace": namespace,
                    "analysis_level": analysis_level,
                    "message": "No events found for analysis",
                    "time_periods_tried": [original_period] + fallback_periods,
                    "suggestion": (
                        "No events in this namespace within the last 7 days. "
                        "The namespace may not generate Kubernetes events, or events have been garbage collected."
                    ),
                }

        analyzer = progressive_event_analyzer_cls(classified_events)

        analysis_result: Dict[str, Any] = {
            "namespace": namespace,
            "analysis_level": analysis_level,
            "total_events": len(classified_events),
            "time_period": time_period,
            "generated_at": datetime.now().isoformat(),
            "raw_events": classified_events,
        }
        if time_period != original_period:
            analysis_result["time_period_note"] = f"Expanded from '{original_period}' to '{time_period}' (no events in original window)"

        if analysis_level == "overview":
            analysis_result["overview"] = analyzer.get_overview()

        elif analysis_level == "detailed":
            analysis_result["detailed_analysis"] = analyzer.get_detailed_analysis(
                event_filters
            )

        elif analysis_level == "correlation":
            analysis_result["correlation_analysis"] = analyzer.get_correlation_analysis(
                seed_event_id
            )

        elif analysis_level == "deep_dive":
            analysis_result["overview"] = analyzer.get_overview()
            analysis_result["detailed_analysis"] = analyzer.get_detailed_analysis(
                event_filters
            )
            analysis_result["correlation_analysis"] = analyzer.get_correlation_analysis(
                seed_event_id
            )
            analysis_result["deep_dive_insights"] = [
                "Complete multi-level analysis performed",
                "Review all sections for comprehensive understanding",
                "Use correlation data for root cause analysis",
            ]

        else:
            return {"error": f"Unknown analysis level: {analysis_level}"}

        logger.info(f"[{tool_name}] Completed {analysis_level} analysis successfully")
        return analysis_result

    except Exception as e:
        logger.error(
            f"[{tool_name}] Error in progressive analysis: {str(e)}", exc_info=True
        )
        return {
            "error": f"Progressive analysis failed: {str(e)}",
            "suggestion": "Try a simpler analysis level like 'overview'",
        }


async def advanced_event_analytics_impl(
    namespace: str,
    time_period: Optional[str] = None,
    include_ml_patterns: bool = True,
    include_log_correlation: bool = True,
    include_metrics_correlation: bool = True,
    include_runbook_suggestions: bool = True,
    analysis_depth: str = "comprehensive",
    *,
    k8s_core_api: Any,
    progressive_event_analysis_fn: Any,
    ml_pattern_detector_cls: Any,
    log_metrics_integrator_cls: Any,
    runbook_suggestion_engine_cls: Any,
    generate_comprehensive_insights_fn: Any,
    assess_overall_risk_fn: Any,
    generate_strategic_recommendations_fn: Any,
) -> Dict[str, Any]:
    """
    Advanced ML-powered event analytics with log/metrics integration and runbook suggestions.

    Extracted from server-mcp.py as part of issue #85.

    Args:
        namespace: Kubernetes namespace to analyze.
        time_period: Time window (e.g., "4h", "1d", "12h").
        include_ml_patterns: Enable ML pattern detection (default: True).
        include_log_correlation: Correlate with log data (default: True).
        include_metrics_correlation: Correlate with metrics (default: True).
        include_runbook_suggestions: Generate runbook suggestions (default: True).
        analysis_depth: "basic", "comprehensive" (default), or "deep".
        k8s_core_api: Injected Kubernetes CoreV1Api client.
        progressive_event_analysis_fn: Injected progressive_event_analysis coroutine.
        ml_pattern_detector_cls: Injected MLPatternDetector class.
        log_metrics_integrator_cls: Injected LogMetricsIntegrator class.
        runbook_suggestion_engine_cls: Injected RunbookSuggestionEngine class.
        generate_comprehensive_insights_fn: Injected generate_comprehensive_insights coroutine.
        assess_overall_risk_fn: Injected assess_overall_risk function.
        generate_strategic_recommendations_fn: Injected generate_strategic_recommendations function.

    Returns:
        Dict: Advanced analytics with ML insights, correlations, and runbook suggestions.
    """
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}

    tool_name = "advanced_event_analytics"

    # Validate analysis_depth
    valid_depths = {"basic", "comprehensive", "deep"}
    if analysis_depth not in valid_depths:
        return {
            "error": f"Invalid analysis_depth '{analysis_depth}'. Must be one of: {', '.join(sorted(valid_depths))}"
        }

    logger.info(
        f"[{tool_name}] Starting advanced analytics for namespace '{namespace}'"
    )

    try:
        # Step 1: Get base event data — scale progressive analysis to depth
        depth_to_level = {"basic": "overview", "comprehensive": "detailed", "deep": "deep_dive"}
        base_level = depth_to_level.get(analysis_depth, "detailed")
        base_result = await progressive_event_analysis_fn(
            namespace=namespace, analysis_level=base_level, time_period=time_period
        )

        if "error" in base_result:
            return {"error": f"Failed to get base event data: {base_result['error']}"}

        # Extract events from progressive analysis results (avoid duplicate API calls)
        events_data = []
        for event in base_result.get("raw_events", []):
            events_data.append(
                {
                    "event_string": event.get("event_string", ""),
                    "severity": event.get("severity"),
                    "category": event.get("category"),
                    "timestamp": event.get("timestamp", datetime.now()),
                    "relevance_score": event.get("relevance_score", 0),
                }
            )

        if not events_data:
            # Fallback: even without events, try log and metrics correlation if enabled
            fallback_result = {
                "namespace": namespace,
                "analysis_type": "advanced_analytics",
                "analysis_depth": analysis_depth,
                "total_events_analyzed": 0,
                "time_period": time_period,
                "generated_at": datetime.now().isoformat(),
                "note": "No Kubernetes events found; performing log/metrics-only analysis",
            }
            has_fallback_data = False

            if include_log_correlation:
                try:
                    log_integrator = log_metrics_integrator_cls([])
                    log_correlation = await log_integrator.correlate_with_logs(
                        namespace, time_period or "2h"
                    )
                    fallback_result["log_correlation"] = log_correlation
                    has_fallback_data = True
                except Exception as e:
                    logger.warning(
                        f"[{tool_name}] Log correlation fallback failed: {e}"
                    )

            if include_metrics_correlation:
                try:
                    if not include_log_correlation:
                        log_integrator = log_metrics_integrator_cls([])
                    metrics_correlation = await log_integrator.correlate_with_metrics(
                        namespace
                    )
                    fallback_result["metrics_correlation"] = metrics_correlation
                    has_fallback_data = True
                except Exception as e:
                    logger.warning(
                        f"[{tool_name}] Metrics correlation fallback failed: {e}"
                    )

            if include_runbook_suggestions:
                fallback_result["runbook_suggestions"] = [
                    "No events detected — check if event generation is working in this namespace",
                    "Verify namespace has active workloads: kubectl get pods -n "
                    + namespace,
                    "Check if events are being garbage collected prematurely",
                ]
                has_fallback_data = True

            if not has_fallback_data:
                fallback_result["message"] = (
                    "No events available and fallback analysis produced no data"
                )
                fallback_result["suggestion"] = (
                    "Try a longer time period or different namespace"
                )

            return fallback_result

        # Initialize analysis result
        analytics_result = {
            "namespace": namespace,
            "analysis_type": "advanced_analytics",
            "analysis_depth": analysis_depth,
            "total_events_analyzed": len(events_data),
            "time_period": time_period,
            "generated_at": datetime.now().isoformat(),
            "base_analysis": base_result,
        }

        # Step 2: ML-powered pattern detection
        if include_ml_patterns:
            logger.info(f"[{tool_name}] Running ML pattern detection")
            ml_detector = ml_pattern_detector_cls(events_data)
            ml_patterns = ml_detector.detect_patterns()
            analytics_result["ml_patterns"] = ml_patterns
        else:
            analytics_result["ml_patterns"] = {"disabled": True}

        # Step 3: Log correlation
        if include_log_correlation:
            logger.info(f"[{tool_name}] Correlating with log data")
            log_integrator = log_metrics_integrator_cls(events_data)
            log_correlation = await log_integrator.correlate_with_logs(
                namespace, time_period or "2h"
            )
            analytics_result["log_correlation"] = log_correlation

        # Step 4: Metrics correlation
        if include_metrics_correlation:
            logger.info(f"[{tool_name}] Correlating with metrics")
            if not include_log_correlation:
                log_integrator = log_metrics_integrator_cls(events_data)
            metrics_correlation = await log_integrator.correlate_with_metrics(namespace)
            analytics_result["metrics_correlation"] = metrics_correlation

        # Step 5: Runbook suggestions
        if include_runbook_suggestions:
            logger.info(f"[{tool_name}] Generating runbook suggestions")
            runbook_engine = runbook_suggestion_engine_cls(
                events_data, analytics_result.get("ml_patterns", {})
            )
            runbook_suggestions = runbook_engine.suggest_runbooks()
            analytics_result["runbook_suggestions"] = runbook_suggestions

        # Step 6: Generate comprehensive insights
        analytics_result["comprehensive_insights"] = (
            await generate_comprehensive_insights_fn(analytics_result, analysis_depth)
        )

        # Step 7: Risk assessment and recommendations
        analytics_result["risk_assessment"] = assess_overall_risk_fn(analytics_result)
        analytics_result["strategic_recommendations"] = (
            generate_strategic_recommendations_fn(analytics_result)
        )

        logger.info(f"[{tool_name}] Advanced analytics completed successfully")
        return analytics_result

    except Exception as e:
        logger.error(
            f"[{tool_name}] Error in advanced analytics: {str(e)}", exc_info=True
        )
        return {
            "error": f"Advanced analytics failed: {str(e)}",
            "suggestion": "Try with reduced analysis scope or shorter time period",
        }


async def automated_triage_rca_report_generator_impl(
    failure_identifier: str,
    namespace: Optional[str] = None,
    investigation_depth: str = "standard",
    include_related_failures: bool = True,
    time_window: str = "2h",
    generate_timeline: bool = True,
    include_remediation: bool = True,
    *,
    k8s_core_api: Any,
    k8s_custom_api: Any,
    detect_tekton_namespaces: Callable,
    analyze_failed_pipeline: Any,
    analyze_pipeline_performance: Any,
    get_pod_logs: Any,
    analyze_logs: Any,
    detect_log_anomalies: Any,
    analyze_pipeline_dependencies: Any,
    list_pipelineruns: Any,
    smart_get_namespace_events: Any,
    categorize_errors: Any,
    recommend_actions: Any,
) -> Dict[str, Any]:
    """
    Generate automated Root Cause Analysis (RCA) report for pipeline/pod failures.

    Extracted from server-mcp.py as part of issue #86.

    All 9 steps of the RCA logic are preserved verbatim:
      1. identify_failure_context
      2. Core failure analysis (pipeline / pod / generic)
      3. Build failure timeline
      4. Correlate with related failures
      5. perform_advanced_rca
      6. analyze_resource_constraints + analyze_configuration_issues
      7. Compile comprehensive analysis
      8. generate_remediation_plan
      9. Calculate confidence and severity

    Args:
        failure_identifier: Pipeline run name, pod name, or failure event ID.
        namespace: Optional namespace where the failure occurred.
        investigation_depth: "quick", "standard" (default), or "deep".
        include_related_failures: Analyze related recent failures (default: True).
        time_window: Time window for related events (default: "2h").
        generate_timeline: Generate event timeline (default: True).
        include_remediation: Include remediation steps (default: True).
        k8s_core_api: Injected Kubernetes CoreV1Api client.
        k8s_custom_api: Injected Kubernetes CustomObjectsApi client.
        detect_tekton_namespaces: Injected async helper to detect Tekton namespaces.
        analyze_failed_pipeline: Injected pipeline failure analysis coroutine.
        analyze_pipeline_performance: Injected pipeline performance analysis coroutine.
        get_pod_logs: Injected pod log fetching coroutine.
        analyze_logs: Injected log analysis coroutine.
        detect_log_anomalies: Injected anomaly detection coroutine.
        analyze_pipeline_dependencies: Injected dependency analysis coroutine.
        list_pipelineruns: Injected pipeline run listing coroutine.
        smart_get_namespace_events: Injected smart event fetching coroutine.
        categorize_errors: Injected error categorisation function.
        recommend_actions: Injected remediation action recommendation function.

    Returns:
        Dict: RCA report with summary, timeline, root cause, diagnostics, and remediation.
    """
    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}
    # Validate investigation_depth
    valid_depths = {"quick", "standard", "deep"}
    if investigation_depth not in valid_depths:
        return {
            "error": f"Invalid investigation_depth '{investigation_depth}'. Must be one of: {', '.join(sorted(valid_depths))}"
        }

    try:
        logger.info(f"Starting automated RCA for failure: {failure_identifier}")
        investigation_start = datetime.now().isoformat()

        # Initialize report structure
        report = {
            "investigation_summary": {
                "failure_id": failure_identifier,
                "investigation_started": investigation_start,
                "failure_type": "Unknown",
                "severity": "Medium",
                "root_cause_confidence": 0.0,
            },
            "failure_timeline": [],
            "root_cause_analysis": {
                "primary_cause": {},
                "contributing_factors": [],
                "affected_systems": [],
            },
            "diagnostic_data": {
                "logs_analyzed": {},
                "resource_analysis": {},
                "configuration_issues": [],
                "dependency_failures": [],
            },
            "remediation_plan": {"immediate_actions": [], "preventive_measures": []},
            "related_incidents": [],
        }

        # Parse time window
        time_hours = 2
        if time_window.endswith("h"):
            time_hours = int(time_window[:-1])
        elif time_window.endswith("m"):
            time_hours = int(time_window[:-1]) / 60

        # Step 1: Identify failure type and locate namespace
        failure_context = await identify_failure_context(
            failure_identifier,
            detect_tekton_namespaces,
            k8s_custom_api,
            k8s_core_api,
            logger,
            namespace,
        )
        if not failure_context["found"]:
            report["investigation_summary"]["failure_type"] = "Not Found"
            report["investigation_summary"]["severity"] = "Low"
            report["investigation_summary"]["search_note"] = failure_context.get(
                "search_note",
                f"Resource '{failure_identifier}' not found in any namespace",
            )
            report["investigation_summary"]["namespaces_searched"] = (
                failure_context.get("namespaces_searched", [])
            )
            report["remediation_plan"] = {
                "immediate_actions": [
                    f"Verify the resource name '{failure_identifier}' is correct",
                    "The resource may have been garbage collected by Tekton pruner",
                    "Try using the query_kubearchive tool to retrieve archived logs",
                    "Check if there are related events: kubectl get events -n <namespace> --field-selector involvedObject.name=<name>",
                ],
                "preventive_measures": [
                    "Investigate sooner after failures (before GC runs)",
                    "Consider increasing Tekton resource retention period",
                ],
            }
            return report

        # Handle GC'd resources found via events
        gc_detected = failure_context.get("gc_detected", False)
        target_namespace = failure_context["namespace"]
        failure_type = failure_context["type"]
        report["investigation_summary"]["failure_type"] = failure_type

        if gc_detected:
            report["investigation_summary"]["gc_detected"] = True
            report["investigation_summary"]["note"] = (
                f"Resource was garbage collected but {failure_context.get('event_count', 0)} "
                f"event(s) were found. Analysis is based on available event data."
            )
            # Populate timeline from the events we found
            gc_events = failure_context.get("events", [])
            report["failure_timeline"] = [
                {
                    "timestamp": ev.get("last_timestamp", "unknown"),
                    "event": ev.get("reason", "unknown"),
                    "message": ev.get("message", ""),
                    "type": ev.get("type", "Normal"),
                    "source": "kubernetes_event",
                }
                for ev in gc_events
            ]

        # Step 2: Core failure analysis based on type
        if failure_type == "pipelinerun":
            primary_analysis = await analyze_pipeline_failure(
                target_namespace,
                failure_identifier,
                investigation_depth,
                analyze_failed_pipeline,
                analyze_pipeline_performance,
                get_pod_logs,
                analyze_logs,
                detect_log_anomalies,
                analyze_pipeline_dependencies,
                logger,
            )
        elif failure_type == "pod":
            primary_analysis = await analyze_pod_failure(
                target_namespace,
                failure_identifier,
                investigation_depth,
                k8s_core_api,
                get_pod_logs,
                analyze_logs,
                detect_log_anomalies,
                smart_get_namespace_events,
                logger,
            )
        else:
            primary_analysis = await analyze_generic_failure(
                target_namespace,
                failure_identifier,
                investigation_depth,
                smart_get_namespace_events,
                logger,
            )

        # Step 3: Build failure timeline
        timeline_events = []
        if generate_timeline:
            timeline_events = await build_failure_timeline(
                target_namespace,
                failure_identifier,
                time_hours,
                smart_get_namespace_events,
                logger,
            )
            if timeline_events:
                report["failure_timeline"] = timeline_events
            # If no new timeline events found but we have GC events, keep those
            elif not report.get("failure_timeline"):
                report["failure_timeline"] = []

        # Step 4: Correlate with related failures
        related_failures = []
        if include_related_failures:
            related_failures = await find_related_failures(
                target_namespace,
                failure_identifier,
                time_hours,
                investigation_depth,
                list_pipelineruns,
                logger,
            )
            report["related_incidents"] = [
                f for f in related_failures
                if f.get("incident_id", "") != failure_identifier
            ]

        # Step 5: Advanced correlation and root cause analysis
        root_cause_data = await perform_advanced_rca(
            primary_analysis,
            timeline_events,
            related_failures,
            investigation_depth,
            categorize_errors,
            logger,
        )

        # Step 6: Resource and configuration analysis
        resource_analysis = await analyze_resource_constraints(
            target_namespace, failure_identifier, k8s_core_api, logger
        )
        config_analysis = await analyze_configuration_issues(
            target_namespace, failure_identifier, logger
        )

        # Step 7: Compile comprehensive analysis
        report["root_cause_analysis"] = root_cause_data["root_cause_analysis"]
        report["diagnostic_data"] = {
            "logs_analyzed": primary_analysis.get("logs_analyzed", {}),
            "resource_analysis": resource_analysis,
            "configuration_issues": config_analysis,
            "dependency_failures": root_cause_data.get("dependency_failures", []),
        }

        # Step 8: Generate remediation plan
        if include_remediation:
            remediation_plan = await generate_remediation_plan(
                root_cause_data,
                primary_analysis,
                resource_analysis,
                config_analysis,
                recommend_actions,
                logger,
            )
            report["remediation_plan"] = remediation_plan

        # Step 9: Calculate confidence and severity
        confidence_score = calculate_confidence_score(
            primary_analysis, root_cause_data, timeline_events
        )
        severity_analysis = assess_failure_severity(
            primary_analysis, root_cause_data, resource_analysis, config_analysis
        )
        severity = severity_analysis["severity_level"]
        high_events = sum(1 for e in timeline_events if e.get("severity") in ("HIGH", "CRITICAL", "high", "critical"))
        if high_events >= 5 and severity == "LOW":
            severity = "MEDIUM"
            severity_analysis["severity_level"] = "MEDIUM"
            severity_analysis.get("severity_factors", []).append(f"{high_events} HIGH/CRITICAL timeline events")
        elif high_events >= 10 and severity in ("LOW", "MEDIUM"):
            severity = "HIGH"
            severity_analysis["severity_level"] = "HIGH"
            severity_analysis.get("severity_factors", []).append(f"{high_events} HIGH/CRITICAL timeline events")

        report["investigation_summary"]["root_cause_confidence"] = confidence_score
        report["investigation_summary"]["severity"] = severity

        logger.info(
            f"RCA completed for {failure_identifier} with confidence: {confidence_score:.2f}"
        )
        return report

    except Exception as e:
        logger.error(
            f"Error in automated RCA for {failure_identifier}: {str(e)}", exc_info=True
        )
        return {
            "investigation_summary": {
                "failure_id": failure_identifier,
                "investigation_started": datetime.now().isoformat(),
                "failure_type": "Error",
                "severity": "High",
                "root_cause_confidence": 0.0,
            },
            "failure_timeline": [],
            "root_cause_analysis": {
                "primary_cause": {"error": str(e)},
                "contributing_factors": [],
                "affected_systems": [],
            },
            "diagnostic_data": {
                "logs_analyzed": {},
                "resource_analysis": {},
                "configuration_issues": [],
                "dependency_failures": [],
            },
            "remediation_plan": {
                "immediate_actions": ["Check tool logs for detailed error information"],
                "preventive_measures": [],
            },
            "related_incidents": [],
        }
