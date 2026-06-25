"""Authentication service for OIDC integration."""

from __future__ import annotations

import os

from dataone.auth import AuthFactory, load_client_secrets

SCOPE_ADMIN = os.getenv("OGDC_SCOPE_ADMIN", "odgc:admin")
scopes = [SCOPE_ADMIN]

# load secrets and instantiate the client
secrets = load_client_secrets()
auth_client = AuthFactory.create_client("fastapi", secrets, scopes)
