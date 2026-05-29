from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from httpx import HTTPError, Response

from ogdc_runner import __version__
from ogdc_runner.service.main import app


def test_version(mock_db):  # noqa: ARG001
    with TestClient(app) as client:
        response = client.get("/version")
        assert response.status_code == 200
        assert response.json() == {"ogdc_runner_version": __version__}