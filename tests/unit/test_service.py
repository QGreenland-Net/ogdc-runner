from __future__ import annotations

from fastapi.testclient import TestClient

from ogdc_runner import __version__
from ogdc_runner.service import app


def test_version(mock_db):  # noqa: ARG001
    with TestClient(app) as client:
        response = client.get("/version")
        assert response.status_code == 200
        assert response.json() == {"ogdc_runner_version": __version__}
