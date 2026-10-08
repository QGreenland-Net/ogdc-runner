from __future__ import annotations

import hashlib
import hmac
from urllib.parse import parse_qs, urlsplit

import pytest

from ogdc_runner import storage_links
from ogdc_runner.exceptions import OgdcMissingEnvvar

SIGNING_KEY = "unit-test-signing-key"
KEY = "workflow-abc/published_zip.tgz"
NOW = 1_800_000_000


@pytest.fixture(autouse=True)
def storage_env(monkeypatch):
    monkeypatch.setenv("OGDC_STORAGE_LINK_SIGNING_KEY", SIGNING_KEY)
    monkeypatch.setenv("PUBLIC_S3_ENDPOINT_URL", "https://ogdc.example.test/storage")


def _signed(key: str = KEY, expires: int = NOW + 60) -> str:
    return storage_links.sign(key, expires)


def test_signature_is_hmac_sha256_of_key_and_expiry():
    expected = hmac.new(
        SIGNING_KEY.encode(), f"{KEY}\n{NOW}".encode(), hashlib.sha256
    ).hexdigest()

    assert storage_links.sign(KEY, NOW) == expected


def test_url_format_and_two_hour_expiry():
    url = storage_links.build_signed_url(KEY, now=NOW)

    parts = urlsplit(url)
    query = parse_qs(parts.query)
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == (
        f"https://ogdc.example.test/storage/{KEY}"
    )
    assert query["expires"] == [str(NOW + 2 * 60 * 60)]
    assert query["signature"] == [storage_links.sign(KEY, NOW + 2 * 60 * 60)]


def test_url_tolerates_trailing_slash_on_public_base(monkeypatch):
    monkeypatch.setenv("PUBLIC_S3_ENDPOINT_URL", "https://ogdc.example.test/storage/")

    url = storage_links.build_signed_url(KEY, now=NOW)

    assert url.startswith(f"https://ogdc.example.test/storage/{KEY}?")


def test_url_quotes_unsafe_key_characters():
    url = storage_links.build_signed_url("a b/c?d.zip", now=NOW)

    assert urlsplit(url).path == "/storage/a%20b/c%3Fd.zip"


@pytest.mark.parametrize(
    "missing", ["OGDC_STORAGE_LINK_SIGNING_KEY", "PUBLIC_S3_ENDPOINT_URL"]
)
def test_url_fails_closed_without_config(monkeypatch, missing):
    monkeypatch.delenv(missing)

    with pytest.raises(OgdcMissingEnvvar, match=missing):
        storage_links.build_signed_url(KEY, now=NOW)
