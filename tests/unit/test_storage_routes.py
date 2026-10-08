from __future__ import annotations

import io
import time

import boto3
import pytest
from botocore.config import Config
from botocore.response import StreamingBody
from botocore.stub import Stubber
from fastapi.testclient import TestClient

from ogdc_runner import storage_links
from ogdc_runner.service import storage_routes
from ogdc_runner.service.main import app

KEY = "wf-1/wf-1-publish-data/published_zip"
BODY = b"zip-bytes"


@pytest.fixture
def stubber(monkeypatch):
    client = boto3.client(
        "s3",
        endpoint_url="http://s3.example.test",
        region_name="us-east-1",
        aws_access_key_id="id",
        aws_secret_access_key="secret",
        config=Config(s3={"addressing_style": "path"}),
    )
    monkeypatch.setattr(storage_routes, "get_s3_client", lambda: client)
    monkeypatch.setenv("S3_BUCKET", "ogdc-data")
    monkeypatch.setenv("OGDC_STORAGE_LINK_SIGNING_KEY", "route-test-key")
    monkeypatch.setenv("PUBLIC_S3_ENDPOINT_URL", "https://ogdc.example.test/storage")
    with Stubber(client) as stub:
        yield stub
        stub.assert_no_pending_responses()


@pytest.fixture
def client(mock_db):  # noqa: ARG001
    with TestClient(app) as c:
        yield c


def _link(key: str = KEY, age: int = 0) -> str:
    """Return the path+query of a signed link, as a client would request it."""
    url = storage_links.build_signed_url(key, now=time.time() - age)
    return url.removeprefix("https://ogdc.example.test")


def test_streams_object_for_valid_link(client, stubber):
    stubber.add_response(
        "get_object",
        {
            "Body": StreamingBody(io.BytesIO(BODY), len(BODY)),
            "ContentLength": len(BODY),
            "ContentType": "application/zip",
        },
        {"Bucket": "ogdc-data", "Key": KEY},
    )

    response = client.get(_link())

    assert response.status_code == 200
    assert response.content == BODY
    assert response.headers["content-type"] == "application/zip"
    assert response.headers["content-length"] == str(len(BODY))


def test_bad_signature_is_403_and_never_reaches_s3(client, stubber):  # noqa: ARG001
    path = _link().rpartition("&signature=")[0]

    assert client.get(f"{path}&signature={'0' * 64}").status_code == 403
