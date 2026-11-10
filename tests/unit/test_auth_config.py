from __future__ import annotations

import json

import pytest
from dataone.auth import (
    ACCESS_MODE_AUTHENTICATED,
    ACCESS_MODE_OPEN,
    ACCESS_MODE_READ_ONLY,
    ConfigurationError,
)

from ogdc_runner.service import auth


def test_authenticated_mode_loads_explicit_secrets_file(tmp_path, monkeypatch):
    secrets_path = tmp_path / "client_secrets.json"
    expected = {
        "server_metadata_url": "https://auth.example/realms/test/.well-known/openid-configuration",
        "client_id": "ogdc-test",
        "client_secret": "not-a-real-secret",
        "redirect_uris": ["https://ogdc.example/api/authorize"],
    }
    secrets_path.write_text(json.dumps(expected))
    monkeypatch.setenv("ACCESS_MODE", ACCESS_MODE_AUTHENTICATED)
    monkeypatch.setenv("OIDC_CLIENT_SECRETS_FILE", str(secrets_path))

    assert auth.load_oidc_client_secrets() == expected


def test_authenticated_mode_requires_explicit_secrets_file(monkeypatch):
    monkeypatch.setenv("ACCESS_MODE", ACCESS_MODE_AUTHENTICATED)
    monkeypatch.delenv("OIDC_CLIENT_SECRETS_FILE", raising=False)

    with pytest.raises(ConfigurationError, match="OIDC_CLIENT_SECRETS_FILE"):
        auth.load_oidc_client_secrets()


@pytest.mark.parametrize("access_mode", [ACCESS_MODE_OPEN, ACCESS_MODE_READ_ONLY])
def test_unauthenticated_modes_do_not_require_oidc_secret(access_mode, monkeypatch):
    monkeypatch.setenv("ACCESS_MODE", access_mode)
    monkeypatch.delenv("OIDC_CLIENT_SECRETS_FILE", raising=False)

    assert auth.load_oidc_client_secrets() == {}
