from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from botocore.exceptions import ClientError

from ogdc_runner import publish, storage_links
from ogdc_runner.argo import ARGO_WORKFLOW_SERVICE
from ogdc_runner.exceptions import OgdcMissingEnvvar, OgdcOutputDataRetrievalError

KEY = "wf-1/wf-1-publish-data/published_zip"


class FakeS3:
    def __init__(self, *, exists: bool = True) -> None:
        self.exists = exists
        self.heads: list[dict[str, str]] = []

    def head_object(self, **kwargs):
        self.heads.append(kwargs)
        if not self.exists:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": 3}


@pytest.fixture
def workflow(monkeypatch):
    artifact = SimpleNamespace(name="published_zip", s3=SimpleNamespace(key=KEY))
    node = SimpleNamespace(outputs=SimpleNamespace(artifacts=[artifact]))
    fake = SimpleNamespace(status=SimpleNamespace(nodes={"n": node}))
    monkeypatch.setattr(
        ARGO_WORKFLOW_SERVICE,
        "get_workflow",
        lambda name: fake,  # noqa: ARG005
    )


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(publish, "get_s3_client", lambda: fake)
    monkeypatch.setenv("S3_BUCKET", "ogdc-data")
    monkeypatch.setenv("OGDC_STORAGE_LINK_SIGNING_KEY", "k")
    monkeypatch.setenv("PUBLIC_S3_ENDPOINT_URL", "https://ogdc.example.test/storage")
    return fake


def test_output_url_is_signed_storage_link(workflow, s3):  # noqa: ARG001
    url = publish.get_temporary_output_data_url(workflow_name="wf-1")

    parts = urlsplit(url)
    query = parse_qs(parts.query)
    assert parts.path == f"/storage/{KEY}"
    assert query["signature"] == [storage_links.sign(KEY, int(query["expires"][0]))]
    assert s3.heads == [{"Bucket": "ogdc-data", "Key": KEY}]


def test_output_url_fails_closed_without_signing_key(workflow, s3, monkeypatch):  # noqa: ARG001
    monkeypatch.delenv("OGDC_STORAGE_LINK_SIGNING_KEY")

    with pytest.raises(OgdcMissingEnvvar):
        publish.get_temporary_output_data_url(workflow_name="wf-1")


def test_output_url_requires_existing_object(workflow, s3):  # noqa: ARG001
    s3.exists = False

    with pytest.raises(OgdcOutputDataRetrievalError):
        publish.get_temporary_output_data_url(workflow_name="wf-1")
