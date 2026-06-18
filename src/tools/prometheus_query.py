"""
LUMINO MCP Server - Prometheus Query Tool and Internal Query Helpers

Extracted from server-mcp.py as part of issue #72 (sub-task of #60).

Contains:
  - _get_k8s_bearer_token               : bearer-token acquisition fallback chain
  - _execute_prometheus_query_internal  : low-level PromQL executor used by other tools
  - _process_prometheus_results         : result formatter / namespace-filter / limiter
  - prometheus_query_impl               : implementation of the public @mcp.tool()

Dependencies on module-level server state (k8s_core_api, k8s_custom_api,
MAX_SERIES_LIMIT) are injected at call time to keep this module independently
importable and testable.
"""

import asyncio
import logging
import os
import re
import time
from typing import Any, Dict, Optional

import aiohttp
from kubernetes.client import Configuration

from helpers.config import (MAX_SERIES_LIMIT, SA_TOKEN_PATH,
                            get_prometheus_token_from_env)
from helpers.prometheus_formatters import (format_as_csv, format_as_json,
                                           format_as_table,
                                           generate_query_suggestions,
                                           generate_related_query_suggestions,
                                           generate_result_summary,
                                           parse_time_parameter)
from tools.prometheus_helpers import discover_prometheus_endpoint

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _get_k8s_bearer_token() -> Optional[str]:
    """
    Get bearer token for Prometheus authentication from Kubernetes client config.

    Fallback chain:
    1. Extract from configured Kubernetes client (kubeconfig or in-cluster)
    2. Read from ServiceAccount token file (in-cluster)
    3. Environment variable (PROMETHEUS_TOKEN, OPENSHIFT_TOKEN, OC_TOKEN)

    Returns:
        Bearer token string, or None if no token could be obtained.
    """
    # Method 1: Extract token from Kubernetes client configuration
    try:
        k8s_config = Configuration.get_default_copy()
        if k8s_config.api_key and k8s_config.api_key.get("authorization"):
            auth_header = k8s_config.api_key["authorization"]
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]
                logger.info(
                    "Successfully obtained bearer token from Kubernetes client config"
                )
                return token
    except Exception as e:
        logger.debug(
            f"Could not extract token from k8s client config: {type(e).__name__}"
        )

    # Method 2: Read from ServiceAccount token file (in-cluster scenario)
    try:
        if os.path.exists(SA_TOKEN_PATH):
            with open(SA_TOKEN_PATH, "r") as f:
                token = f.read().strip()
                if token:
                    logger.info(
                        "Successfully obtained token from ServiceAccount token file"
                    )
                    return token
    except Exception as e:
        logger.debug(f"Could not read ServiceAccount token: {type(e).__name__}")

    # Method 3: Environment variable fallback
    env_token = get_prometheus_token_from_env()
    if env_token:
        logger.info("Using token from environment variable")
        return env_token

    logger.error("Could not obtain authentication token from any source")
    return None


async def _execute_prometheus_query_internal(
    query: str,
    timeout: int = 30,
    *,
    k8s_core_api: Optional[Any] = None,
    k8s_custom_api: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Internal helper to execute Prometheus/Thanos instant queries from other tools.

    Args:
        query:          PromQL query string.
        timeout:        Query timeout in seconds (default: 30).
        k8s_core_api:   CoreV1Api client for endpoint discovery (may be None).
        k8s_custom_api: CustomObjectsApi client for endpoint discovery (may be None).

    Returns:
        Dict with keys:
          ``success`` (bool), ``data`` (list), ``endpoint_type`` (str|None),
          ``error`` (str|None).
    """
    try:
        prometheus_url, endpoint_type = await discover_prometheus_endpoint(
            k8s_core_api=k8s_core_api,
            k8s_custom_api=k8s_custom_api,
        )
        if not prometheus_url:
            return {
                "success": False,
                "data": [],
                "endpoint_type": None,
                "error": "Could not discover Prometheus/Thanos endpoint",
            }

        auth_token = await _get_k8s_bearer_token()

        api_path = "/api/v1/query"
        params: Dict[str, Any] = {"query": query, "timeout": f"{timeout}s"}
        if endpoint_type == "thanos":
            params["dedup"] = "true"

        query_url = f"{prometheus_url}{api_path}"
        headers: Dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": "LUMINO-MCP/1.0",
        }
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout + 10)
        ) as session:
            async with session.get(
                query_url, params=params, headers=headers, ssl=False
            ) as response:
                if response.status == 200:
                    response_data = await response.json()
                    result_data = response_data.get("data", {})
                    raw_results = result_data.get("result", [])
                    return {
                        "success": True,
                        "data": raw_results,
                        "endpoint_type": endpoint_type,
                        "error": None,
                    }
                else:
                    error_text = await response.text()
                    logger.warning(
                        f"Prometheus query failed with status {response.status}: {error_text}"
                    )
                    return {
                        "success": False,
                        "data": [],
                        "endpoint_type": endpoint_type,
                        "error": f"HTTP {response.status}: {error_text}",
                    }

    except Exception as e:
        logger.error(f"Error executing internal Prometheus query: {type(e).__name__}")
        return {
            "success": False,
            "data": [],
            "endpoint_type": None,
            "error": f"Internal query failed: {type(e).__name__}",
        }


async def _process_prometheus_results(
    response_data: Dict[str, Any],
    format_type: str,
    namespace_filter: Optional[str],
    limit: Optional[int],
    original_query: str,
    query_type: str,
) -> Dict[str, Any]:
    """
    Process and format raw Prometheus HTTP response data.

    Applies namespace filtering, hard limits, and the requested output format.

    Args:
        response_data:    Raw dict from a Prometheus HTTP response (parsed JSON).
        format_type:      One of "json", "table", or "csv".
        namespace_filter: Optional regex applied to the ``namespace`` metric label.
        limit:            Optional max number of series to return.
        original_query:   Original PromQL string (used for summary/suggestions).
        query_type:       "instant" or "range" (passed through to metadata).

    Returns:
        Processed result dict ready to be returned by the @mcp.tool().
    """
    try:
        result_data = response_data.get("data", {})
        result_type = result_data.get("resultType", "")
        raw_results = result_data.get("result", [])

        # Namespace filtering
        if namespace_filter:
            try:
                namespace_pattern = re.compile(namespace_filter)
                raw_results = [
                    r
                    for r in raw_results
                    if namespace_pattern.search(
                        r.get("metric", {}).get("namespace", "")
                    )
                ]
                logger.info(
                    f"Applied namespace filter '{namespace_filter}', "
                    f"{len(raw_results)} results remain"
                )
            except re.error as e:
                logger.warning(
                    f"Invalid namespace filter regex '{namespace_filter}': {e}"
                )

        # Caller-requested limit
        if limit and len(raw_results) > limit:
            raw_results = raw_results[:limit]
            logger.info(f"Limited results to {limit} items")

        # Safety hard cap (MAX_SERIES_LIMIT = 500 from helpers.config)
        if len(raw_results) > MAX_SERIES_LIMIT:
            logger.warning(
                f"Truncating {len(raw_results)} series to {MAX_SERIES_LIMIT} "
                "to prevent excessive response size"
            )
            raw_results = raw_results[:MAX_SERIES_LIMIT]

        # Format selection
        if format_type == "table":
            formatted_data = format_as_table(raw_results, result_type)
        elif format_type == "csv":
            formatted_data = format_as_csv(raw_results, result_type)
        else:  # json (default)
            formatted_data = format_as_json(raw_results, result_type)

        summary = generate_result_summary(raw_results, result_type, original_query)
        suggestions = generate_related_query_suggestions(original_query, raw_results)

        return {
            "result_count": len(raw_results),
            "result_type": result_type,
            "data": formatted_data,
            "summary": summary,
            "suggestions": suggestions,
            "errors": [],
            "metadata": {
                "namespace_filter": namespace_filter,
                "limit": limit,
                "format": format_type,
                "query_type": query_type,
            },
        }

    except Exception as e:
        logger.error(f"Error processing Prometheus results: {e}")
        return {
            "result_count": 0,
            "result_type": "unknown",
            "data": [],
            "summary": "Error processing results",
            "suggestions": ["Check query syntax", "Try simpler query"],
            "errors": [str(e)],
        }


# ---------------------------------------------------------------------------
# Public tool implementation
# ---------------------------------------------------------------------------


async def prometheus_query_impl(
    query: str,
    query_type: str = "instant",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    step: str = "300s",
    cluster: Optional[str] = None,
    format: str = "json",
    namespace_filter: Optional[str] = None,
    limit: Optional[int] = None,
    timeout: int = 30,
    *,
    k8s_core_api: Optional[Any] = None,
    k8s_custom_api: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Execute PromQL queries against Prometheus or Thanos for cluster metrics.

    Supports instant and range queries with automatic endpoint discovery and
    authentication.

    Args:
        query:            PromQL query string.
        query_type:       "instant" or "range" (default: "instant").
        start_time:       Start for range queries (ISO 8601 or Unix timestamp).
        end_time:         End for range queries (ISO 8601 or Unix timestamp).
        step:             Step interval for range queries (default: "300s").
        cluster:          Cluster domain override for predefined endpoint lookup.
        format:           "json", "table", or "csv" (default: "json").
        namespace_filter: Regex to filter results by namespace label.
        limit:            Max number of result series to return.
        timeout:          Query timeout in seconds (default: 30).
        k8s_core_api:     CoreV1Api client (may be None).
        k8s_custom_api:   CustomObjectsApi client (may be None).

    Returns:
        Dict with query results, execution metadata, and analysis suggestions.
    """
    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}

    start_execution_time = time.time()
    tool_name = "mcp__lumino__prometheus_query"

    logger.info(f"[{tool_name}] Starting Prometheus query execution")
    logger.info(f"[{tool_name}] Query: {query}")
    logger.info(f"[{tool_name}] Type: {query_type}, Format: {format}")

    try:
        # --- Input validation ---
        if not query or not query.strip():
            return {
                "status": "error",
                "error_type": "invalid_query",
                "message": "Query parameter is required and cannot be empty",
                "query_executed": "",
                "execution_time": 0,
                "result_count": 0,
                "data": [],
                "suggestions": [
                    "Provide a valid PromQL query",
                    'Example: up{job="node-exporter"}',
                ],
                "errors": ["Empty query provided"],
            }

        if query_type not in ("instant", "range"):
            return {
                "status": "error",
                "error_type": "invalid_query_type",
                "message": f"Invalid query_type '{query_type}'. Must be 'instant' or 'range'",
                "query_executed": query,
                "execution_time": 0,
                "result_count": 0,
                "data": [],
                "suggestions": [
                    "Use query_type='instant' for current values",
                    "Use query_type='range' for time series",
                ],
                "errors": [f"Invalid query_type: {query_type}"],
            }

        if query_type == "range" and (not start_time or not end_time):
            return {
                "status": "error",
                "error_type": "missing_time_range",
                "message": "Range queries require both start_time and end_time parameters",
                "query_executed": query,
                "execution_time": 0,
                "result_count": 0,
                "data": [],
                "suggestions": [
                    "Provide start_time and end_time for range queries",
                    "Use ISO 8601 format: '2024-01-01T00:00:00Z'",
                    "Or Unix timestamps: '1704067200'",
                ],
                "errors": ["Missing time range parameters for range query"],
            }

        # --- Authentication ---
        auth_token = await _get_k8s_bearer_token()
        if not auth_token:
            logger.info(
                f"[{tool_name}] No bearer token available - will attempt "
                "unauthenticated request (common for vanilla Kubernetes Prometheus)"
            )

        # --- Endpoint discovery ---
        prometheus_url, endpoint_type = await discover_prometheus_endpoint(
            cluster_override=cluster,
            k8s_core_api=k8s_core_api,
            k8s_custom_api=k8s_custom_api,
        )
        if not prometheus_url:
            return {
                "status": "error",
                "error_type": "endpoint_discovery_failed",
                "message": "Could not discover Prometheus endpoint",
                "query_executed": query,
                "execution_time": 0,
                "result_count": 0,
                "data": [],
                "suggestions": [
                    "Check if Prometheus or Thanos Query is deployed "
                    "(openshift-monitoring, monitoring, thanos, or observability namespace)",
                    "Verify Prometheus Operator CRDs are installed if using Prometheus Operator",
                    "Ensure OpenShift Routes are accessible if on OpenShift",
                    "Set THANOS_URL or PROMETHEUS_URL environment variable to specify endpoint directly",
                    "Try adding a predefined endpoint in OPENSHIFT_PROMETHEUS_ENDPOINTS config",
                ],
                "errors": ["Prometheus/Thanos endpoint not found"],
            }

        logger.info(f"[{tool_name}] Using {endpoint_type} endpoint: {prometheus_url}")

        # --- Build request ---
        if query_type == "instant":
            api_path = "/api/v1/query"
            params: Dict[str, Any] = {"query": query}
            if timeout:
                params["timeout"] = f"{timeout}s"
        else:  # range
            api_path = "/api/v1/query_range"
            params = {
                "query": query,
                "start": parse_time_parameter(start_time),  # type: ignore[arg-type]
                "end": parse_time_parameter(end_time),  # type: ignore[arg-type]
                "step": step,
            }
            if timeout:
                params["timeout"] = f"{timeout}s"

        if endpoint_type == "thanos":
            params["dedup"] = "true"

        query_url = f"{prometheus_url}{api_path}"
        headers: Dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": "LUMINO-MCP/1.0",
        }
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        logger.info(f"[{tool_name}] Executing query against: {query_url}")

        # --- Execute ---
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout + 10)
        ) as session:
            async with session.get(
                query_url, params=params, headers=headers, ssl=False
            ) as response:
                execution_time = round((time.time() - start_execution_time) * 1000, 2)

                if response.status == 200:
                    response_data = await response.json()
                    logger.info(
                        f"[{tool_name}] Query executed successfully in {execution_time}ms"
                    )
                    processed_results = await _process_prometheus_results(
                        response_data,
                        format,
                        namespace_filter,
                        limit,
                        query,
                        query_type,
                    )
                    processed_results.update(
                        {
                            "status": "success",
                            "query_executed": query,
                            "execution_time": execution_time,
                            "prometheus_endpoint": prometheus_url,
                            "endpoint_type": endpoint_type,
                            "query_type": query_type,
                            "parameters": params,
                        }
                    )
                    return processed_results

                elif response.status == 400:
                    error_text = await response.text()
                    logger.warning(f"[{tool_name}] Bad request (400): {error_text}")
                    suggestions = generate_query_suggestions(query, error_text)
                    return {
                        "status": "error",
                        "error_type": "invalid_query",
                        "message": f"PromQL query error: {error_text}",
                        "query_executed": query,
                        "execution_time": execution_time,
                        "result_count": 0,
                        "data": [],
                        "suggestions": suggestions,
                        "errors": [error_text],
                    }

                elif response.status == 401:
                    logger.error(f"[{tool_name}] Authentication failed (401)")
                    return {
                        "status": "error",
                        "error_type": "authentication_failed",
                        "message": "Authentication failed - invalid or expired token",
                        "query_executed": query,
                        "execution_time": execution_time,
                        "result_count": 0,
                        "data": [],
                        "suggestions": [
                            "Refresh your Kubernetes credentials (kubeconfig or ServiceAccount)",
                            "Check if token has expired",
                            "Set PROMETHEUS_TOKEN environment variable with a valid token",
                            "Verify cluster access permissions",
                        ],
                        "errors": ["Authentication failed"],
                    }

                elif response.status == 403:
                    logger.error(f"[{tool_name}] Access forbidden (403)")
                    return {
                        "status": "error",
                        "error_type": "permission_denied",
                        "message": "Access denied - insufficient permissions",
                        "query_executed": query,
                        "execution_time": execution_time,
                        "result_count": 0,
                        "data": [],
                        "suggestions": [
                            "Check RBAC permissions for metrics access",
                            "Verify cluster-monitoring-view role binding",
                            "Contact cluster administrator for monitoring access",
                        ],
                        "errors": ["Permission denied"],
                    }

                else:
                    error_text = await response.text()
                    logger.error(
                        f"[{tool_name}] HTTP error {response.status}: {error_text}"
                    )
                    return {
                        "status": "error",
                        "error_type": "http_error",
                        "message": f"HTTP {response.status}: {error_text}",
                        "query_executed": query,
                        "execution_time": execution_time,
                        "result_count": 0,
                        "data": [],
                        "suggestions": [
                            "Check Prometheus service availability",
                            "Verify cluster connectivity",
                            "Try again in a few minutes",
                        ],
                        "errors": [f"HTTP {response.status}: {error_text}"],
                    }

    except asyncio.TimeoutError:
        execution_time = round((time.time() - start_execution_time) * 1000, 2)
        logger.error(f"[{tool_name}] Query timeout after {timeout}s")
        return {
            "status": "error",
            "error_type": "timeout",
            "message": f"Query timed out after {timeout} seconds",
            "query_executed": query,
            "execution_time": execution_time,
            "result_count": 0,
            "data": [],
            "suggestions": [
                "Try a simpler query with shorter time range",
                "Increase timeout parameter",
                "Use more specific label selectors to reduce data",
            ],
            "errors": [f"Timeout after {timeout}s"],
        }

    except Exception as e:
        execution_time = round((time.time() - start_execution_time) * 1000, 2)
        safe_error = f"Unexpected error during query execution: {type(e).__name__}"
        logger.error(f"[{tool_name}] {safe_error}", exc_info=True)
        return {
            "status": "error",
            "error_type": "unexpected_error",
            "message": safe_error,
            "query_executed": query,
            "execution_time": execution_time,
            "result_count": 0,
            "data": [],
            "suggestions": [
                "Check system logs for details",
                "Verify cluster connectivity",
                "Try a simpler query first",
            ],
            "errors": [f"Unexpected error: {type(e).__name__}"],
        }
