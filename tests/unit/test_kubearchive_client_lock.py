"""
Tests for Issue #197 — concurrent-safe KubeArchive client initialisation.
"""
import asyncio
import sys
import importlib
from unittest.mock import MagicMock
import pytest


def _make_discovery_mock(endpoint="https://kubearchive.example.com"):
    mock = MagicMock()
    async def _discover():
        await asyncio.sleep(0)
        return endpoint
    mock.discover_endpoint = _discover
    return mock


def _make_discovery_mock_none():
    mock = MagicMock()
    async def _discover():
        return None
    mock.discover_endpoint = _discover
    return mock


@pytest.fixture
def fresh_module():
    mod_name = "helpers.kubearchive_integration"
    old_mod = sys.modules.pop(mod_name, None)
    mod = importlib.import_module(mod_name)
    fake_client_cls = MagicMock(name="KubeArchiveClientMock")
    fake_client_cls.side_effect = lambda **kw: MagicMock(name="instance")
    mod.KubeArchiveClient = fake_client_cls
    mod.ka_client = None
    mod._ka_client_lock = asyncio.Lock()
    yield mod, fake_client_cls
    if old_mod is not None:
        sys.modules[mod_name] = old_mod
    else:
        sys.modules.pop(mod_name, None)


@pytest.mark.asyncio
async def test_single_call_creates_client(fresh_module):
    mod, cls = fresh_module
    result = await mod.setup_kubearchive_client(_make_discovery_mock(), MagicMock())
    assert result is not None
    cls.assert_called_once()
    assert mod.ka_client is result


@pytest.mark.asyncio
async def test_second_call_reuses_client(fresh_module):
    mod, cls = fresh_module
    first = await mod.setup_kubearchive_client(_make_discovery_mock(), MagicMock())
    second = await mod.setup_kubearchive_client(_make_discovery_mock(), MagicMock())
    cls.assert_called_once()
    assert first is second


@pytest.mark.asyncio
async def test_concurrent_calls_create_exactly_one_client(fresh_module):
    mod, cls = fresh_module
    results = await asyncio.gather(
        *[mod.setup_kubearchive_client(_make_discovery_mock(), MagicMock()) for _ in range(10)]
    )
    assert all(r is not None for r in results)
    assert len(set(id(r) for r in results)) == 1
    assert cls.call_count == 1, f"Expected 1 construction, got {cls.call_count}"


@pytest.mark.asyncio
async def test_returns_none_when_endpoint_not_discovered(fresh_module):
    mod, cls = fresh_module
    result = await mod.setup_kubearchive_client(_make_discovery_mock_none(), MagicMock())
    assert result is None
    cls.assert_not_called()
    assert mod.ka_client is None


@pytest.mark.asyncio
async def test_lock_exists_at_module_level(fresh_module):
    mod, _ = fresh_module
    assert hasattr(mod, "_ka_client_lock")
    assert isinstance(mod._ka_client_lock, asyncio.Lock)
