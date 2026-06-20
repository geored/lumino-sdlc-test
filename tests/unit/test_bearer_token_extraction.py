"""
Unit tests for _get_k8s_bearer_token in src/tools/prometheus_query.py.

Covers:
  - "authorization" key with "Bearer <token>" prefix → token extracted correctly
  - "authorization" key with raw token (no prefix) → token used directly
  - "BearerToken" key with raw token (no prefix) → token used directly
  - "BearerToken" key with "Bearer <token>" prefix → token extracted correctly
  - "authorization" key empty, "BearerToken" key present → falls back to BearerToken
  - api_key is non-empty but contains neither key → falls through to Method 2
  - api_key is None / empty → falls through to Method 2
"""

from unittest.mock import MagicMock, mock_open, patch

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
    async def test_authorization_key_raw_no_prefix_returns_token(self):
        """api_key['authorization'] = 'rawtoken_no_prefix' → returns 'rawtoken_no_prefix' directly."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({"authorization": "rawtoken_no_prefix"})
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            result = await _get_k8s_bearer_token()

        assert result == "rawtoken_no_prefix"

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
    async def test_nonempty_authorization_takes_priority_over_bearer_token_key(self):
        """Both keys present: non-empty authorization must win over BearerToken."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({"authorization": "Bearer winner", "BearerToken": "loser"})
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            result = await _get_k8s_bearer_token()

        assert result == "winner"

    @pytest.mark.asyncio
    async def test_api_key_with_unrelated_keys_falls_through_to_sa_file(self):
        """api_key has neither 'authorization' nor 'BearerToken' → falls to Method 2 (SA file)."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({"someOtherKey": "something"})
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            with patch("tools.prometheus_query.os.path.exists", return_value=False):
                with patch("tools.prometheus_query.get_prometheus_token_from_env", return_value=None):
                    result = await _get_k8s_bearer_token()

        assert result is None

    @pytest.mark.asyncio
    async def test_empty_api_key_dict_falls_through_to_sa_file(self):
        """api_key is an empty dict (falsy) → skips Method 1, falls to Method 2."""
        from tools.prometheus_query import _get_k8s_bearer_token

        cfg = _make_k8s_config({})
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
        with patch("tools.prometheus_query.Configuration.get_default_copy", return_value=cfg):
            with patch("tools.prometheus_query.os.path.exists", return_value=True):
                m = mock_open(read_data="sa-file-token")
                with patch("tools.prometheus_query.open", m):
                    result = await _get_k8s_bearer_token()

        assert result == "sa-file-token"


# ---------------------------------------------------------------------------
# Tests for KubeArchiveClient._extract_token_from_client  (Fixes #233)
# ---------------------------------------------------------------------------


class TestKubeArchiveClientExtractToken:
    """Tests for KubeArchiveClient._extract_token_from_client.

    The method tries api_key['authorization'] first, then api_key['BearerToken'].
    Both paths must strip the 'Bearer ' prefix so callers always receive a bare token.
    """

    def _make_client(self, api_key: dict):
        from unittest.mock import MagicMock
        from helpers.kubearchive_integration import KubeArchiveClient

        mock_config = MagicMock()
        mock_config.api_key = api_key

        mock_api_client = MagicMock()
        mock_api_client.configuration = mock_config

        mock_core_api = MagicMock()
        mock_core_api.api_client = mock_api_client

        client = KubeArchiveClient.__new__(KubeArchiveClient)
        client.k8s_core_api = mock_core_api
        return client

    def test_authorization_key_with_bearer_prefix_strips_prefix(self):
        """api_key['authorization'] = 'Bearer mytoken' -> returns 'mytoken'."""
        client = self._make_client({"authorization": "Bearer mytoken"})
        assert client._extract_token_from_client() == "mytoken"

    def test_authorization_key_raw_no_prefix_returns_token(self):
        """api_key['authorization'] = 'rawtoken' -> returns 'rawtoken' directly."""
        client = self._make_client({"authorization": "rawtoken"})
        assert client._extract_token_from_client() == "rawtoken"

    def test_bearer_token_key_with_bearer_prefix_strips_prefix(self):
        """api_key['BearerToken'] = 'Bearer prefixed789' -> strips prefix, returns 'prefixed789'."""
        client = self._make_client({"BearerToken": "Bearer prefixed789"})
        assert client._extract_token_from_client() == "prefixed789"

    def test_bearer_token_key_raw_no_prefix_returns_token(self):
        """api_key['BearerToken'] = 'rawtoken456' -> returns 'rawtoken456' directly."""
        client = self._make_client({"BearerToken": "rawtoken456"})
        assert client._extract_token_from_client() == "rawtoken456"

    def test_authorization_takes_priority_over_bearer_token_key(self):
        """Both keys present: non-empty 'authorization' wins over 'BearerToken'."""
        client = self._make_client({"authorization": "Bearer winner", "BearerToken": "loser"})
        assert client._extract_token_from_client() == "winner"

    def test_empty_authorization_falls_back_to_bearer_token_key(self):
        """authorization is empty -> falls back to BearerToken."""
        client = self._make_client({"authorization": "", "BearerToken": "fallbacktoken"})
        assert client._extract_token_from_client() == "fallbacktoken"

    def test_no_k8s_core_api_returns_none(self):
        """k8s_core_api is None -> returns None without raising."""
        from helpers.kubearchive_integration import KubeArchiveClient
        client = KubeArchiveClient.__new__(KubeArchiveClient)
        client.k8s_core_api = None
        assert client._extract_token_from_client() is None

    def test_neither_key_present_returns_none(self):
        """api_key has neither 'authorization' nor 'BearerToken' -> returns None."""
        client = self._make_client({"someOtherKey": "value"})
        assert client._extract_token_from_client() is None
