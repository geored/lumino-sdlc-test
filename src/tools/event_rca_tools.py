"""Kubernetes namespace event fetching tools — extracted from server-mcp.py.

Each function accepts injected Kubernetes API clients and helpers rather than
relying on module-level globals.  The thin ``@mcp.tool()`` wrappers that remain
in ``server-mcp.py`` simply forward to these implementations.

Fixes #80
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from kubernetes.client.rest import ApiException

from helpers.utils import parse_time_period
from helpers.constants import SMART_EVENTS_CONFIG
from helpers.event_analysis import EventSeverity, EventCategory

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
        "applied_filters": {}
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

        logger.info(f"Fetching events with pagination (limit={max_fetch_limit} per page)")

        while page_count < MAX_PAGES:
            try:
                if continue_token:
                    event_list_response = await asyncio.to_thread(
                        k8s_core_api.list_namespaced_event,
                        namespace=namespace,
                        watch=False,
                        limit=max_fetch_limit,
                        _continue=continue_token
                    )
                else:
                    event_list_response = await asyncio.to_thread(
                        k8s_core_api.list_namespaced_event,
                        namespace=namespace,
                        watch=False,
                        limit=max_fetch_limit
                    )

                page_count += 1
                page_events = len(event_list_response.items)
                all_events.extend(event_list_response.items)

                logger.info(f"Fetched page {page_count}: {page_events} events (total: {len(all_events)})")

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
            errors_list.append(f"Event fetching limited to {len(all_events)} events due to volume.")

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
                event_str = f"[{timestamp}] {event.type}: {event.reason} - {event.message}"
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
            "hit_page_limit": page_count >= MAX_PAGES and continue_token is not None
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
            "applied_filters": {}
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
            limit=limit
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
                    "last_timestamp": event.last_timestamp.isoformat() if event.last_timestamp else None,
                    "first_timestamp": event.first_timestamp.isoformat() if event.first_timestamp else None,
                    "count": event.count or 1,
                    "involved_object": {}
                }

                if event.involved_object:
                    event_dict["involved_object"] = {
                        "name": event.involved_object.name or "",
                        "kind": event.involved_object.kind or "",
                        "namespace": event.involved_object.namespace or namespace,
                        "uid": event.involved_object.uid or ""
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
    logger.info(f"[{tool_name}] Starting smart event analysis for namespace '{namespace}'")

    try:
        if not namespace or not namespace.strip():
            return {"error": "Namespace cannot be empty"}

        if max_context_tokens < 1000:
            logger.warning(f"[{tool_name}] Low token limit ({max_context_tokens}), setting to 1000")
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
                    logger.info(f"[{tool_name}] HIGH EVENT VOLUME detected (~{estimated_hourly_events}/hour) - using 30min window")
                    if "errors" not in focus_areas:
                        focus_areas = ["errors", "warnings"] + [f for f in focus_areas if f not in ["errors", "warnings"]]
                elif estimated_hourly_events > 50:
                    time_period = "2h"
                    logger.info(f"[{tool_name}] MEDIUM EVENT VOLUME detected (~{estimated_hourly_events}/hour) - using 2h window")
                else:
                    time_period = "6h"
                    logger.info(f"[{tool_name}] LOW EVENT VOLUME detected (~{estimated_hourly_events}/hour) - using 6h window")

            except Exception as e:
                logger.warning(f"[{tool_name}] Volume estimation failed, using safe default: {e}")
                time_period = SMART_EVENTS_CONFIG["defaults"]["default_time_window"]

            logger.info(f"[{tool_name}] ADAPTIVE STRATEGY selected: {time_period} time window")

        logger.info(f"[{tool_name}] Fetching events with filters: last_n={last_n_events}, time_period={time_period}")

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

        logger.info(f"[{tool_name}] Retrieved {events_count} events, processing with strategy: {strategy}")

        if strategy == "smart_summary":

            if not events_list:
                return {
                    "namespace": namespace,
                    "strategy_used": "smart_summary",
                    "total_events": 0,
                    "processed_events": 0,
                    "events": [],
                    "summary": {"total_events": 0, "message": "No events found in the specified timeframe"},
                    "insights": ["No events found - this could indicate either a quiet period or issues with event generation"],
                    "recommendations": ["Verify that applications are generating events as expected"],
                    "token_usage": {"total_estimated": 200},
                    "applied_filters": raw_result.get("applied_filters", {}),
                    "smart_features": {
                        "intelligent_defaults": time_period if last_n_events is None else None,
                        "context_overflow_prevention": True,
                        "focus_areas": focus_areas
                    }
                }

            selected_events = smart_sample_string_events_fn(events_list, focus_areas, max_context_tokens)

            summary = {}
            if include_summary:
                summary = generate_string_events_summary_fn(selected_events, focus_areas)

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
                        "token_estimate": event["token_estimate"]
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
                    "total_estimated": int(total_tokens + summary_tokens + metadata_tokens)
                },
                "applied_filters": raw_result.get("applied_filters", {}),
                "smart_features": {
                    "intelligent_defaults": time_period if last_n_events is None else None,
                    "context_overflow_prevention": True,
                    "focus_areas": focus_areas,
                    "classification_applied": True,
                    "smart_sampling": True
                },
                "classification_metadata": {
                    "severity_distribution": {
                        severity.value: len([e for e in selected_events if e["severity"] == severity.value])
                        for severity in EventSeverity
                    },
                    "category_distribution": {
                        category.value: len([e for e in selected_events if e["category"] == category.value])
                        for category in EventCategory
                    }
                }
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
                    "truncated": events_count > max_raw
                },
                "token_usage": {
                    "total_estimated": min(events_count, max_raw) * 60
                },
                "note": "Raw strategy with safety limits applied to prevent context overflow"
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
                    "suggestion": "Use smart_summary strategy for detailed analysis"
                },
                "quick_insights": [
                    f"Found {events_count} events in namespace '{namespace}'",
                    "Use 'smart_summary' strategy for intelligent analysis",
                    "Progressive disclosure enables drilling down into specific issues"
                ]
            }

    except Exception as e:
        logger.error(f"[{tool_name}] Unexpected error: {str(e)}", exc_info=True)
        return {
            "error": f"Smart event analysis failed: {str(e)}",
            "fallback_suggestion": "Try using the original get_namespace_events tool with explicit filters"
        }
