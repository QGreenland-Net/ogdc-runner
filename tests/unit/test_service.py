from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from httpx import HTTPError, Response

from ogdc_runner import __version__
from ogdc_runner.service.auth import AUTH_TOKEN_URL
from ogdc_runner.service.main import app
from ogdc_runner.service.user import ADMIN_USERNAME


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
                "username": ADMIN_USERNAME,
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


def test_create_new_user(mock_db):  # noqa: ARG001
    def _create_foo_user(client: TestClient, admin_token: str) -> Response:
        response = client.post(
            "/create_user",
            data={
                "username": "foo",
                "password": "bar",
            },
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        return response

    with TestClient(app) as client:
        response = client.post(
            AUTH_TOKEN_URL,
            data={
                "username": ADMIN_USERNAME,
                "password": "password",
            },
        )

        admin_token = response.json()["access_token"]

        # Create a user `foo`.
        response = _create_foo_user(client, admin_token)
        response.raise_for_status()

        # Now test that an error is raised if we try creating the same user
        # again.
        response = _create_foo_user(client, admin_token)

        assert response.status_code == 409
        with pytest.raises(HTTPError):
            response.raise_for_status()


def test_create_user_non_admin_token(mock_db):  # noqa: ARG001
    """Test to ensure that non-admins cannot create a user."""
    with TestClient(app) as client:
        response = client.post(
            AUTH_TOKEN_URL,
            data={
                "username": ADMIN_USERNAME,
                "password": "password",
            },
        )
        response.raise_for_status()

        admin_token = response.json()["access_token"]

        # Create a "foo" user
        foo_username = "foo"
        foo_password = "bar"
        response = client.post(
            "/create_user",
            data={
                "username": foo_username,
                "password": foo_password,
            },
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        response.raise_for_status()

        # Get the "foo" user token
        response = client.post(
            AUTH_TOKEN_URL,
            data={
                "username": foo_username,
                "password": foo_password,
            },
        )
        response.raise_for_status()

        foo_token = response.json()["access_token"]

        # now try to create a new user as foo:
        response = client.post(
            "/create_user",
            data={
                "username": "foo2",
                "password": "bar2",
            },
            headers={"Authorization": f"Bearer {foo_token}"},
        )

        assert response.status_code == 401

        with pytest.raises(HTTPError):
            response.raise_for_status()
