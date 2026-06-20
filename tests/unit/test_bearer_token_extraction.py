"""
Unit tests for _get_k8s_bearer_token in src/tools/prometheus_query.py.

Covers:
  - "authorization" key with "Bearer <token>" prefix → token extracted correctly
  - "BearerToken" key with raw token (no prefix) → token used directly
  - "BearerToken" key with "Bearer <token>" prefix → token extracted correctly
  - "authorization" key empty, "BearerToken" key present → falls back to BearerToken
  - api_key is non-empty but contains neither key → falls through to Method 2
  - api_key is None / empty → falls through to Method 2
"""

from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_k8s_config(api_key: dict):
    """Return a mock Configuration object with the given api_key dict."""
    cfg = MagicMock()
    cfg.api_key = api_key
    return cfg


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGetK8sBearerToken:

    @pytest.mark.asyncio
    async def test_authorization_key_with_bearer_prefix_returns_raw_token(self):
        """api_key['authorization'] = 'Bearer mytoken123' → returns 'mytoken123'."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({"authorization": "Bearer mytoken123"})
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            result = await _get_k8s_bearer_token()

        assert result == "mytoken123"

    @pytest.mark.asyncio
    async def test_bearer_token_key_raw_no_prefix_returns_token(self):
        """api_key['BearerToken'] = 'rawtoken456' (no prefix) → returns 'rawtoken456'."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({"BearerToken": "rawtoken456"})
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            result = await _get_k8s_bearer_token()

        assert result == "rawtoken456"

    @pytest.mark.asyncio
    async def test_bearer_token_key_with_bearer_prefix_returns_raw_token(self):
        """api_key['BearerToken'] = 'Bearer prefixed789' → strips prefix, returns 'prefixed789'."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({"BearerToken": "Bearer prefixed789"})
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            result = await _get_k8s_bearer_token()

        assert result == "prefixed789"

    @pytest.mark.asyncio
    async def test_empty_authorization_falls_back_to_bearer_token_key(self):
        """authorization is empty string, BearerToken is present → uses BearerToken."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({"authorization": "", "BearerToken": "fallbacktoken"})
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            result = await _get_k8s_bearer_token()

        assert result == "fallbacktoken"

    @pytest.mark.asyncio
    async def test_api_key_with_unrelated_keys_falls_through_to_sa_file(self):
        """api_key has neither 'authorization' nor 'BearerToken' → falls to Method 2 (SA file)."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({"someOtherKey": "something"})
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            # Method 2: SA token file does not exist
            with patch("tools.prometheus_query.os.path.exists", return_value=False):
                # Method 3: no env token
                with patch("tools.prometheus_query.get_prometheus_token_from_env", return_value=None):
                    result = await _get_k8s_bearer_token()

        assert result is None

    @pytest.mark.asyncio
    async def test_empty_api_key_dict_falls_through_to_sa_file(self):
        """api_key is an empty dict (falsy) → skips Method 1, falls to Method 2."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({})
        # Empty dict is falsy, so the `if k8s_config.api_key:` guard skips the block.
        # cfg.api_key = {} — but MagicMock({}) is truthy; set it explicitly.
        cfg.api_key = {}
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            with patch("tools.prometheus_query.os.path.exists", return_value=False):
                with patch("tools.prometheus_query.get_prometheus_token_from_env", return_value=None):
                    result = await _get_k8s_bearer_token()

        assert result is None

    @pytest.mark.asyncio
    async def test_none_api_key_falls_through_cleanly(self):
        """api_key is None → skips Method 1, tries Method 2 and 3."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config(None)
        cfg.api_key = None
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            with patch("tools.prometheus_query.os.path.exists", return_value=False):
                with patch("tools.prometheus_query.get_prometheus_token_from_env", return_value=None):
                    result = await _get_k8s_bearer_token()

        assert result is None

    @pytest.mark.asyncio
    async def test_sa_file_used_when_k8s_config_has_no_token(self):
        """No token in api_key → falls through and reads SA file successfully."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({})
        cfg.api_key = {}
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            with patch("tools.prometheus_query.os.path.exists", return_value=True):
                m = mock_open(read_data="sa-file-token")
                with patch("tools.prometheus_query.open", m):
                    result = await _get_k8s_bearer_token()

        assert result == "sa-file-token"
