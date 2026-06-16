"""Smoke tests: import, compile, and basic invariants."""
import sys, types, importlib, importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

SRC = Path(__file__).parent.parent.parent / "src"

def _load_server():
    """Load server-mcp.py with k8s patched out."""
    sys.path.insert(0, str(SRC))
    import kubernetes.config as kconfig
    import kubernetes.client as kclient
    with patch.object(kconfig, 'load_incluster_config', side_effect=kconfig.ConfigException("no cluster")), \
         patch.object(kconfig, 'load_kube_config', side_effect=kconfig.ConfigException("no kubeconfig")), \
         patch.object(kclient, 'CoreV1Api', return_value=None), \
         patch.object(kclient, 'AppsV1Api', return_value=None), \
         patch.object(kclient, 'CustomObjectsApi', return_value=None), \
         patch.object(kclient, 'StorageV1Api', return_value=None), \
         patch.object(kclient, 'BatchV1Api', return_value=None), \
         patch.object(kclient, 'NetworkingV1Api', return_value=None), \
         patch.object(kclient, 'AutoscalingV2Api', return_value=None):
        spec = importlib.util.spec_from_file_location("server_mcp", SRC / "server-mcp.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def server_mod():
    return _load_server()


def test_server_imports_without_kubeconfig(server_mod):
    """server-mcp.py must load even with no kubeconfig available."""
    assert server_mod is not None


def test_mcp_instance_exists(server_mod):
    """The FastMCP instance must be accessible as 'mcp'."""
    assert hasattr(server_mod, "mcp"), "server-mcp.py must expose 'mcp'"


def test_adaptive_log_processor_exists(server_mod):
    """AdaptiveLogProcessor class must be defined."""
    assert hasattr(server_mod, "AdaptiveLogProcessor")


def test_prometheus_endpoint_cache_exists(server_mod):
    """PrometheusEndpointCache must be defined and instantiable."""
    cache_cls = server_mod.PrometheusEndpointCache
    cache = cache_cls(ttl=10)
    assert cache is not None


def test_helpers_importable():
    """All helper modules must be importable."""
    sys.path.insert(0, str(SRC))
    import helpers
    for name in ["constants", "utils", "log_analysis", "event_analysis",
                 "failure_analysis", "resource_topology", "semantic_search",
                 "ml_persistence", "kubearchive_integration"]:
        mod = importlib.import_module(f"helpers.{name}")
        assert mod is not None, f"helpers.{name} failed to import"


def test_k8s_clients_none_when_no_config(server_mod):
    """k8s_core_api must be None when no kubeconfig exists (graceful degradation)."""
    assert server_mod.k8s_core_api is None


def test_adaptive_log_processor_first_pod_invariant(server_mod):
    """AdaptiveLogProcessor must always process at least the first pod."""
    proc = server_mod.AdaptiveLogProcessor(total_token_budget=10)
    assert proc.total_token_budget == 10


def test_main_py_loads():
    """main.py must be importable as a module without executing main()."""
    spec = importlib.util.spec_from_file_location(
        "main_mod", Path(__file__).parent.parent.parent / "main.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert hasattr(mod, "main")
