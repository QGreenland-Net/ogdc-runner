"""Authentication service for OIDC integration."""

from __future__ import annotations

from dataone.auth import AuthFactory, load_client_secrets

ACCESS_MODE_AUTHENTICATED = "authenticated"
scopes = ["ogdc:admin"]

# load secrets and instantiate the client
secrets = load_client_secrets()
auth_client = AuthFactory.create_client("fastapi", secrets, scopes)
