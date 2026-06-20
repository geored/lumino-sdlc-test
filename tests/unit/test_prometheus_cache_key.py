"""Tests for discover_prometheus_endpoint cache key with k8s_core_api identity.

Fixes #228: cache key ignores k8s_core_api identity when cluster_override is
not set, causing multi-cluster callers to share cached endpoints.
"""

import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

# We test the helpers in isolation; patch away the import-time side-effects
# by inserting the helpers and config modules into sys.path.
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from tools.prometheus_helpers import (
    _extract_api_server_url,
    discover_prometheus_endpoint,
)
from helpers.config import PrometheusEndpointCache


# ---------------------------------------------------------------------------
# _extract_api_server_url unit tests
# ---------------------------------------------------------------------------

class TestExtractApiServerUrl:
    """Unit tests for the _extract_api_server_url helper."""

    def test_returns_none_when_client_is_none(self):
        assert _extract_api_server_url(None) is None

    def test_returns_host_from_configuration(self):
        mock_client = MagicMock()
        mock_client.api_client.configuration.host = "https://cluster-a:6443"
        assert _extract_api_server_url(mock_client) == "https://cluster-a:6443"

    def test_returns_none_on_attribute_error(self):
        """If the client object has an unexpected shape, return None."""
        mock_client = MagicMock(spec=[])  # no attributes at all
        # Accessing .api_client on a spec=[] mock raises AttributeError
        assert _extract_api_server_url(mock_client) is None

    def test_different_clients_return_different_urls(self):
        client_a = MagicMock()
        client_a.api_client.configuration.host = "https://cluster-a:6443"
        client_b = MagicMock()
        client_b.api_client.configuration.host = "https://cluster-b:6443"
        assert _extract_api_server_url(client_a) != _extract_api_server_url(client_b)


# ---------------------------------------------------------------------------
# Cache key isolation tests
# ---------------------------------------------------------------------------

class TestCacheKeyIsolation:
    """Verify that two k8s_core_api instances pointing at different clusters
    do NOT share cached Prometheus endpoints (the core bug in #228)."""

    def setup_method(self):
        """Fresh cache for every test."""
        self.cache = PrometheusEndpointCache(ttl_seconds=300)

    def _make_core_api(self, host: str) -> MagicMock:
        m = MagicMock()
        m.api_client.configuration.host = host
        return m

    def test_different_clusters_get_different_cache_keys(self):
        """Two callers with different API server URLs must not collide."""
        client_a = self._make_core_api("https://cluster-a:6443")
        client_b = self._make_core_api("https://cluster-b:6443")

        url_a = _extract_api_server_url(client_a)
        url_b = _extract_api_server_url(client_b)
        key_a = f"default:{url_a}"
        key_b = f"default:{url_b}"

        assert key_a != key_b

        self.cache.set("http://prom-a:9090", key_a, endpoint_type="prometheus")
        self.cache.set("http://prom-b:9090", key_b, endpoint_type="prometheus")

        assert self.cache.get(key_a)[0] == "http://prom-a:9090"
        assert self.cache.get(key_b)[0] == "http://prom-b:9090"

    def test_none_client_uses_bare_default_key(self):
        """When k8s_core_api is None the key should be plain 'default'."""
        url = _extract_api_server_url(None)
        key = f"default:{url}" if url else "default"
        assert key == "default"

    def test_cluster_override_takes_precedence(self):
        """When cluster_override is set, the API-server URL is irrelevant."""
        # The cache key should just be the override string
        cache_key = "my-cluster"  # cluster_override path
        self.cache.set("http://prom:9090", cache_key, endpoint_type="prometheus")
        assert self.cache.get(cache_key) is not None
        assert self.cache.get("default") is None


# ---------------------------------------------------------------------------
# Integration-level: discover_prometheus_endpoint respects API-server URL
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestDiscoverEndpointCacheKeyIntegration:
    """End-to-end tests confirming discover_prometheus_endpoint builds the
    correct cache key from the k8s_core_api identity."""

    async def test_second_cluster_not_served_stale_cache(self):
        """Regression test for #228: two callers with different k8s_core_api
        instances must not share the cached endpoint."""

        client_a = MagicMock()
        client_a.api_client.configuration.host = "https://cluster-a:6443"
        client_b = MagicMock()
        client_b.api_client.configuration.host = "https://cluster-b:6443"

        with patch("tools.prometheus_helpers.get_thanos_url", return_value=None), \
             patch("tools.prometheus_helpers.get_prometheus_url", return_value=None), \
             patch("tools.prometheus_helpers.OPENSHIFT_PROMETHEUS_ENDPOINTS", {}), \
             patch("tools.prometheus_helpers._prometheus_endpoint_cache") as mock_cache, \
             patch("tools.prometheus_helpers._discover_thanos_via_services", return_value=None), \
             patch("tools.prometheus_helpers._discover_prometheus_via_services", return_value=None), \
             patch("tools.prometheus_helpers._discover_prometheus_via_operator_crd", return_value=None), \
             patch("tools.prometheus_helpers._discover_prometheus_via_routes", return_value=None), \
             patch("tools.prometheus_helpers.is_running_in_cluster", return_value=False):

            # Simulate: cache has entry for cluster-a
            def fake_get(key):
                if key == "default:https://cluster-a:6443":
                    return ("http://prom-a:9090", "prometheus")
                return None

            mock_cache.get.side_effect = fake_get
            mock_cache.set = MagicMock()

            # Caller A should get prom-a from cache
            result_a = await discover_prometheus_endpoint(
                cluster_override=None, k8s_core_api=client_a
            )
            assert result_a == ("http://prom-a:9090", "prometheus")

            # Caller B should NOT get prom-a (different cluster)
            result_b = await discover_prometheus_endpoint(
                cluster_override=None, k8s_core_api=client_b
            )
            # It should be (None, None) because nothing was discovered
            assert result_b == (None, None)

    async def test_cluster_override_bypasses_api_url_extraction(self):
        """When cluster_override is set, the cache key is just the override."""

        client_a = MagicMock()
        client_a.api_client.configuration.host = "https://cluster-a:6443"

        with patch("tools.prometheus_helpers.get_thanos_url", return_value=None), \
             patch("tools.prometheus_helpers.get_prometheus_url", return_value=None), \
             patch("tools.prometheus_helpers.OPENSHIFT_PROMETHEUS_ENDPOINTS", {
                 "my-cluster": {"url": "http://prom-override:9090", "type": "prometheus"}
             }):

            result = await discover_prometheus_endpoint(
                cluster_override="my-cluster", k8s_core_api=client_a
            )
            assert result == ("http://prom-override:9090", "prometheus")
