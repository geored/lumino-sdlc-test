"""
Tests for Issue #199 — concurrent-safe _namespace_cache updates in list_namespaces_impl.

Verifies:
- Lock exists at module level in helpers.config
- Cache hit avoids API call
- Concurrent callers make exactly one API call (no TOCTOU race)
- Double-checked locking: second coroutine inside the lock reuses the result
- ApiException 403/401 returns empty list without propagation
- Unexpected exception returns empty list without propagation
"""
import asyncio
import importlib
import sys
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_k8s_core_api(ns_names=("alpha", "beta", "gamma"), *, delay=0.0):
    """Return a mock CoreV1Api whose list_namespace() returns fake namespaces."""
    mock_api = MagicMock()
    ns_items = []
    for name in ns_names:
        ns = MagicMock()
        ns.metadata.name = name
        ns_items.append(ns)
    ns_list = MagicMock()
    ns_list.items = ns_items

    call_count = {"n": 0}

    def _list_namespace():
        call_count["n"] += 1
        if delay:
            import time as _time
            _time.sleep(delay)
        return ns_list

    mock_api.list_namespace = _list_namespace
    mock_api._call_count = call_count
    return mock_api


def _fresh_cache(mod):
    """Reset the module-level namespace cache to pristine state."""
    mod._namespace_cache["namespaces"] = None
    mod._namespace_cache["timestamp"] = 0


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def k8s_tools_mod():
    """Import kubernetes_tools with a fresh config cache each test."""
    config_mod = importlib.import_module("helpers.config")
    # Reset cache before each test
    config_mod._namespace_cache["namespaces"] = None
    config_mod._namespace_cache["timestamp"] = 0
    # Reset lock to ensure it is not held
    config_mod._namespace_cache_lock = asyncio.Lock()
    mod = importlib.import_module("tools.kubernetes_tools")
    # Patch the cache reference inside kubernetes_tools to use the fresh one
    mod._namespace_cache = config_mod._namespace_cache
    mod._namespace_cache_lock = config_mod._namespace_cache_lock
    yield mod
    # Cleanup
    config_mod._namespace_cache["namespaces"] = None
    config_mod._namespace_cache["timestamp"] = 0


# ---------------------------------------------------------------------------
# Tests: lock exists
# ---------------------------------------------------------------------------

def test_namespace_cache_lock_exists_in_config():
    config_mod = importlib.import_module("helpers.config")
    assert hasattr(config_mod, "_namespace_cache_lock"), (
        "_namespace_cache_lock must be defined at module level in helpers.config"
    )
    assert isinstance(config_mod._namespace_cache_lock, asyncio.Lock)


# ---------------------------------------------------------------------------
# Tests: basic happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_sorted_namespace_list(k8s_tools_mod):
    api = _make_k8s_core_api(["zeta", "alpha", "beta"])
    result = await k8s_tools_mod.list_namespaces_impl(api)
    assert result == ["alpha", "beta", "zeta"]
    assert api._call_count["n"] == 1


@pytest.mark.asyncio
async def test_cache_hit_skips_api_call(k8s_tools_mod):
    api = _make_k8s_core_api(["ns1", "ns2"])
    # Populate cache
    first = await k8s_tools_mod.list_namespaces_impl(api)
    assert api._call_count["n"] == 1
    # Second call — should hit cache
    second = await k8s_tools_mod.list_namespaces_impl(api)
    assert api._call_count["n"] == 1, "API should NOT be called again on cache hit"
    assert first == second


@pytest.mark.asyncio
async def test_stale_cache_triggers_refresh(k8s_tools_mod):
    api = _make_k8s_core_api(["ns1"])
    config_mod = importlib.import_module("helpers.config")
    # Seed cache with expired timestamp
    config_mod._namespace_cache["namespaces"] = ["old-ns"]
    config_mod._namespace_cache["timestamp"] = time.time() - config_mod._NAMESPACE_CACHE_TTL - 1
    k8s_tools_mod._namespace_cache = config_mod._namespace_cache

    result = await k8s_tools_mod.list_namespaces_impl(api)
    assert result == ["ns1"]
    assert api._call_count["n"] == 1


# ---------------------------------------------------------------------------
# Tests: concurrent callers — the core TOCTOU fix
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_callers_make_exactly_one_api_call(k8s_tools_mod):
    """Ten concurrent coroutines must trigger exactly one list_namespace() call."""
    api = _make_k8s_core_api(["ns-a", "ns-b"], delay=0.02)

    results = await asyncio.gather(
        *[k8s_tools_mod.list_namespaces_impl(api) for _ in range(10)]
    )

    assert api._call_count["n"] == 1, (
        f"Expected exactly 1 API call due to lock, got {api._call_count['n']}"
    )
    # All results must be identical
    for r in results:
        assert r == ["ns-a", "ns-b"]


@pytest.mark.asyncio
async def test_all_concurrent_results_are_identical(k8s_tools_mod):
    """Every concurrent result must be the exact same list object (cache reuse)."""
    api = _make_k8s_core_api(["x", "y", "z"], delay=0.01)
    results = await asyncio.gather(
        *[k8s_tools_mod.list_namespaces_impl(api) for _ in range(5)]
    )
    first = results[0]
    for r in results[1:]:
        assert r == first


# ---------------------------------------------------------------------------
# Tests: error paths — never propagate exceptions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_none_api_client_returns_empty_list(k8s_tools_mod):
    result = await k8s_tools_mod.list_namespaces_impl(None)
    assert result == []


@pytest.mark.asyncio
async def test_api_exception_403_returns_empty_list(k8s_tools_mod):
    api = MagicMock()
    api.list_namespace.side_effect = ApiException(status=403, reason="Forbidden")
    result = await k8s_tools_mod.list_namespaces_impl(api)
    assert result == []


@pytest.mark.asyncio
async def test_api_exception_401_returns_empty_list(k8s_tools_mod):
    api = MagicMock()
    api.list_namespace.side_effect = ApiException(status=401, reason="Unauthorized")
    result = await k8s_tools_mod.list_namespaces_impl(api)
    assert result == []


@pytest.mark.asyncio
async def test_api_exception_500_returns_empty_list(k8s_tools_mod):
    api = MagicMock()
    api.list_namespace.side_effect = ApiException(status=500, reason="Internal Server Error")
    result = await k8s_tools_mod.list_namespaces_impl(api)
    assert result == []


@pytest.mark.asyncio
async def test_unexpected_exception_returns_empty_list(k8s_tools_mod):
    api = MagicMock()
    api.list_namespace.side_effect = RuntimeError("unexpected boom")
    result = await k8s_tools_mod.list_namespaces_impl(api)
    assert result == []
