from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from httpx import HTTPError

from ogdc_runner import __version__
from ogdc_runner.service.auth import AUTH_TOKEN_URL
from ogdc_runner.service.main import app


def test_version(mock_db):  # noqa: ARG001
    with TestClient(app) as client:
        response = client.get("/version")
        assert response.status_code == 200
        assert response.json() == {"ogdc_runner_version": __version__}


def test_token(mock_db):  # noqa: ARG001
    with TestClient(app) as client:
        response = client.post(
            AUTH_TOKEN_URL,
            data={
                "username": "admin",
                "password": "password",
            },
        )
        assert response.status_code == 200
        assert "access_token" in response.json()

        token = response.json()["access_token"]
        response = client.get(
            "/user",
            headers={"Authorization": f"Bearer {token}"},
        )

        response.raise_for_status()


def test_bad_token_fails(mock_db):  # noqa: ARG001
    with TestClient(app) as client:
        bad_token = "faketoken!"
        response = client.get(
            "/user",
            headers={"Authorization": f"Bearer {bad_token}"},
        )

        with pytest.raises(HTTPError):
            response.raise_for_status()
