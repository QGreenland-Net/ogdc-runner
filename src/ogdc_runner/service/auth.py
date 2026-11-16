"""Authentication service for OIDC integration."""

from __future__ import annotations

import os
from typing import Any

from dataone.auth import (
    ACCESS_MODE_AUTHENTICATED,
    AuthFactory,
    ConfigurationError,
    get_access_mode,
    load_client_secrets,
)

SCOPE_ADMIN = os.getenv("OGDC_SCOPE_ADMIN", "ogdc:admin")
scopes = [SCOPE_ADMIN]


def load_oidc_client_secrets() -> dict[str, Any]:
    """Load mounted OIDC credentials when authentication is enabled."""
    if get_access_mode() != ACCESS_MODE_AUTHENTICATED:
        return {}

    secrets_file = os.getenv("OIDC_CLIENT_SECRETS_FILE")
    if not secrets_file:
        msg = (
            "OIDC_CLIENT_SECRETS_FILE must point to a client secrets JSON file "
            "when ACCESS_MODE=authenticated"
        )
        raise ConfigurationError(msg)

    return load_client_secrets(secrets_file)


secrets = load_oidc_client_secrets()
auth_client = AuthFactory.create_client("fastapi", secrets, scopes)
