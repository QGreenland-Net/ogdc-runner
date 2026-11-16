from __future__ import annotations

import pytest

from ogdc_runner import publish
from ogdc_runner.exceptions import OgdcMissingEnvvar

S3_ENVVARS = (
    "S3_ENDPOINT_URL",
    "INTERNAL_S3_ENDPOINT_URL",
    "S3_BUCKET",
    "S3_REGION",
    "S3_ADDRESSING_STYLE",
    "S3_SECRET_KEY_ID",
    "S3_SECRET_KEY_KEY",
)


@pytest.fixture(autouse=True)
def clean_s3_env(monkeypatch):
    """Start every test with no S3 config and a fresh (uncached) client."""
    for name in S3_ENVVARS:
        monkeypatch.delenv(name, raising=False)
    publish.get_s3_client.cache_clear()
    yield
    publish.get_s3_client.cache_clear()


@pytest.fixture
def s3_credentials(monkeypatch):
    monkeypatch.setenv("S3_SECRET_KEY_ID", "test-key-id")
    monkeypatch.setenv("S3_SECRET_KEY_KEY", "test-key-secret")


def test_client_defaults(monkeypatch, s3_credentials):  # noqa: ARG001
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://s3.example.test:9000")

    client = publish.get_s3_client()

    assert client.meta.endpoint_url == "http://s3.example.test:9000"
    assert client.meta.region_name == "us-east-1"
    assert client.meta.config.s3["addressing_style"] == "path"
    assert client.meta.config.signature_version == "s3v4"


def test_client_honors_region_and_addressing_style(monkeypatch, s3_credentials):  # noqa: ARG001
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://s3.example.test:9000")
    monkeypatch.setenv("S3_REGION", "default")
    monkeypatch.setenv("S3_ADDRESSING_STYLE", "virtual")

    client = publish.get_s3_client()

    assert client.meta.region_name == "default"
    assert client.meta.config.s3["addressing_style"] == "virtual"


def test_client_falls_back_to_legacy_internal_endpoint(monkeypatch, s3_credentials):  # noqa: ARG001
    monkeypatch.setenv("INTERNAL_S3_ENDPOINT_URL", "http://legacy.example.test:9000")

    assert (
        publish.get_s3_client().meta.endpoint_url == "http://legacy.example.test:9000"
    )


def test_s3_endpoint_url_wins_over_legacy(monkeypatch, s3_credentials):  # noqa: ARG001
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://new.example.test")
    monkeypatch.setenv("INTERNAL_S3_ENDPOINT_URL", "http://legacy.example.test")

    assert publish.get_s3_client().meta.endpoint_url == "http://new.example.test"


def test_client_requires_endpoint(s3_credentials):  # noqa: ARG001
    with pytest.raises(OgdcMissingEnvvar, match="S3_ENDPOINT_URL"):
        publish.get_s3_client()


@pytest.mark.parametrize("missing", ["S3_SECRET_KEY_ID", "S3_SECRET_KEY_KEY"])
def test_client_requires_credentials(monkeypatch, s3_credentials, missing):  # noqa: ARG001
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://s3.example.test")
    monkeypatch.delenv(missing)

    with pytest.raises(OgdcMissingEnvvar, match=missing):
        publish.get_s3_client()


def test_bucket_defaults_to_argo_workflows():
    assert publish.get_s3_bucket() == "argo-workflows"


def test_bucket_from_env(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "ogdc-data")

    assert publish.get_s3_bucket() == "ogdc-data"
