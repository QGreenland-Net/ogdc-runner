"""Signed, expiring links to objects served by the OGDC service under `/storage`.

Presigned S3 URLs bind the signature to the S3 host and path, so they cannot be
re-hosted under the public OGDC hostname. Instead, we sign
`"{s3_key}\\n{expires}"` with an HMAC key that only the service knows, and the
service streams the object from S3 when a valid link is requested.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from urllib.parse import quote

from ogdc_runner.exceptions import (
    OgdcMissingEnvvar,
)

# Time in seconds that a link remains valid: 2 hours.
LINK_TTL_SECONDS = 60 * 120


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        err_msg = f"{name} must be set."
        raise OgdcMissingEnvvar(err_msg)
    return value


def sign(s3_key: str, expires: int) -> str:
    """Return the hex HMAC-SHA256 signature for the given key and expiry."""
    signing_key = _require_env("OGDC_STORAGE_LINK_SIGNING_KEY").encode()
    message = f"{s3_key}\n{expires}".encode()
    return hmac.new(signing_key, message, hashlib.sha256).hexdigest()


def build_signed_url(s3_key: str, *, now: float | None = None) -> str:
    """Return a public, signed link for the given s3 key."""
    public_base = _require_env("PUBLIC_S3_ENDPOINT_URL").rstrip("/")
    expires = int((time.time() if now is None else now) + LINK_TTL_SECONDS)
    signature = sign(s3_key, expires)
    return f"{public_base}/{quote(s3_key)}?expires={expires}&signature={signature}"
