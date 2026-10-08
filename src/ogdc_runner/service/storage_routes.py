"""Unauthenticated route serving objects behind signed `/storage` links.

The signed link is the capability: it is minted by the (authenticated)
`/output/{recipe_workflow_name}` route and verified here. See
{mod}`ogdc_runner.storage_links` for why S3 presigned URLs are not used.
"""

from __future__ import annotations

from collections.abc import Iterator
from posixpath import basename
from typing import Any
from urllib.parse import quote

from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger

from ogdc_runner import storage_links
from ogdc_runner.exceptions import (
    OgdcInvalidStorageLink,
    OgdcMissingEnvvar,
    OgdcStorageLinkExpired,
)
from ogdc_runner.publish import get_s3_bucket, get_s3_client

router = APIRouter()

_CHUNK_SIZE = 1024 * 1024


def _content_disposition(s3_key: str) -> str:
    name = basename(s3_key)
    # Header values must be latin-1; offer an ASCII fallback plus RFC 5987 name.
    fallback = "".join(c if c.isascii() and c.isprintable() else "_" for c in name)
    fallback = fallback.replace('"', "_").replace("\\", "_")
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(name)}"


def _iter_body(body: Any) -> Iterator[bytes]:
    try:
        yield from body.iter_chunks(chunk_size=_CHUNK_SIZE)
    finally:
        body.close()


@router.get("/{s3_key:path}")
def get_stored_object(
    s3_key: str,
    expires: str | None = None,
    signature: str | None = None,
) -> StreamingResponse:
    """Stream the object for a valid, unexpired signed link."""
    try:
        storage_links.verify(s3_key, expires, signature)
    except OgdcStorageLinkExpired as e:
        raise HTTPException(status_code=410, detail="Link has expired.") from e
    except OgdcInvalidStorageLink as e:
        raise HTTPException(status_code=403, detail="Invalid link.") from e
    except OgdcMissingEnvvar as e:
        logger.error(f"Storage links are not configured: {e}")
        raise HTTPException(
            status_code=503, detail="Storage links are not configured."
        ) from e

    try:
        response = get_s3_client().get_object(Bucket=get_s3_bucket(), Key=s3_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            raise HTTPException(status_code=404, detail="Object not found.") from e
        logger.error(f"Failed to fetch s3 object {s3_key}: {code}")
        raise HTTPException(status_code=502, detail="Storage backend error.") from e

    return StreamingResponse(
        _iter_body(response["Body"]),
        media_type=response.get("ContentType", "application/octet-stream"),
        headers={
            "Content-Length": str(response["ContentLength"]),
            "Content-Disposition": _content_disposition(s3_key),
        },
    )
