"""Unauthenticated route serving objects behind signed `/storage` links.

The signed link is the capability: it is minted by the (authenticated)
`/output/{recipe_workflow_name}` route and verified here. See
{mod}`ogdc_runner.storage_links` for why S3 presigned URLs are not used.
"""

from __future__ import annotations

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

    response = get_s3_client().get_object(Bucket=get_s3_bucket(), Key=s3_key)
    return StreamingResponse(
        response["Body"].iter_chunks(chunk_size=1024 * 1024),
        media_type=response.get("ContentType", "application/octet-stream"),
        headers={"Content-Length": str(response["ContentLength"])},
    )
