"""Centralized configuration, constants, and environment variable handling.

Extracted from server-mcp.py as part of issue #51 (sub-task of #30).

Design notes:
- Environment variables are read lazily via helper functions, NOT at import time,
  so that tests can set env vars after importing this module.
- Mutable module-level state (caches) is defined here so that all modules share
  the same object by reference.
"""

import logging
import os
import time
from typing import Any, Dict, Optional

logger = logging.getLogger("lumino-mcp-server")

# ---------------------------------------------------------------------------
# Service-account token path (Kubernetes in-cluster)
# ---------------------------------------------------------------------------
SA_TOKEN_PATH: str = "/var/run/secrets/kubernetes.io/serviceaccount/token"

# ---------------------------------------------------------------------------
# Prometheus local Tekton component endpoints
# ---------------------------------------------------------------------------
PROMETHEUS_ENDPOINTS: Dict[str, str] = {
    "tekton-operator": "http://localhost:9092/metrics",
    "tekton-chains-metrics": "http://localhost:9093/metrics",
    "tekton-events-controller": "http://localhost:9094/metrics",
    "tekton-pipelines-controller": "http://localhost:9097/metrics",
    "tekton-pipelines-remote-resolvers": "http://localhost:9100/metrics",
    "tekton-pipelines-webhook": "http://localhost:9103/metrics",
    "tekton-results-api-service": "http://localhost:9108/metrics",
    "tekton-results-watcher": "http://localhost:9110/metrics",
}

# OpenShift cluster Prometheus endpoints (fallback dict)
OPENSHIFT_PROMETHEUS_ENDPOINTS: Dict[str, Any] = {
    # Format: "cluster-name": {"url": "https://prometheus-endpoint-url"}
}

# ---------------------------------------------------------------------------
# Prometheus query limits
# ---------------------------------------------------------------------------
MAX_SERIES_LIMIT: int = 500

# ---------------------------------------------------------------------------
# Prometheus token env-var names (ordered by priority)
# ---------------------------------------------------------------------------
PROMETHEUS_TOKEN_ENV_VARS: tuple = ("PROMETHEUS_TOKEN", "OPENSHIFT_TOKEN", "OC_TOKEN")

# ---------------------------------------------------------------------------
# KubeArchive host discovery cache
# ---------------------------------------------------------------------------
_kubearchive_host_cache: Dict[str, Any] = {"host": None, "ts": 0}
KUBEARCHIVE_CACHE_TTL_SEC: int = 300

# ---------------------------------------------------------------------------
# Namespace cache (shared mutable state — 24-hour TTL)
# ---------------------------------------------------------------------------
_namespace_cache: Dict[str, Any] = {"namespaces": None, "timestamp": 0}
_NAMESPACE_CACHE_TTL: int = 86400  # 1 day in seconds


# ---------------------------------------------------------------------------
# PrometheusEndpointCache
# ---------------------------------------------------------------------------
class PrometheusEndpointCache:
    """Cache for discovered Prometheus/Thanos endpoints with TTL."""

    def __init__(self, ttl_seconds: int = 300):  # 5 minute default cache
        self._cache: Dict[str, tuple] = {}  # key -> (endpoint, endpoint_type, timestamp)
        self._ttl = ttl_seconds

    def get(self, cluster_key: str = "default") -> Optional[tuple]:
        """Get cached endpoint if valid. Returns (url, endpoint_type) or None."""
        if cluster_key in self._cache:
            endpoint, endpoint_type, timestamp = self._cache[cluster_key]
            if time.time() - timestamp < self._ttl:
                logger.debug(f"Cache hit for {endpoint_type} endpoint: {endpoint}")
                return (endpoint, endpoint_type)
            else:
                del self._cache[cluster_key]
        return None

    def set(
        self,
        endpoint: str,
        cluster_key: str = "default",
        endpoint_type: str = "prometheus",
    ) -> None:
        """Cache endpoint with its type."""
        self._cache[cluster_key] = (endpoint, endpoint_type, time.time())
        logger.debug(f"Cached {endpoint_type} endpoint: {endpoint}")

    def invalidate(self, cluster_key: str = "default") -> None:
        """Invalidate cache entry."""
        if cluster_key in self._cache:
            del self._cache[cluster_key]


# Global singleton — all modules share this instance
_prometheus_endpoint_cache: PrometheusEndpointCache = PrometheusEndpointCache()


# ---------------------------------------------------------------------------
# Lazy environment-variable accessors
# ---------------------------------------------------------------------------


def get_thanos_url() -> Optional[str]:
    """Return THANOS_URL env var value, or None. Evaluated lazily."""
    return os.getenv("THANOS_URL")


def get_prometheus_url() -> Optional[str]:
    """Return PROMETHEUS_URL env var value, or None. Evaluated lazily."""
    return os.getenv("PROMETHEUS_URL")


def get_prometheus_token_from_env() -> Optional[str]:
    """Search PROMETHEUS_TOKEN / OPENSHIFT_TOKEN / OC_TOKEN env vars.

    Returns the first non-empty value, or None.
    """
    for env_var in PROMETHEUS_TOKEN_ENV_VARS:
        token = os.getenv(env_var, "").strip()
        if token:
            return token
    return None


# ---------------------------------------------------------------------------
# Cluster detection helper
# ---------------------------------------------------------------------------


def is_running_in_cluster() -> bool:
    """Check if we're running inside a Kubernetes cluster."""
    return os.path.exists(SA_TOKEN_PATH)
