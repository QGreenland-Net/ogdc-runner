from __future__ import annotations

from functools import cache

from fastapi.testclient import TestClient
from sqlmodel import create_engine

from ogdc_runner import __version__, db
from ogdc_runner.service import app


def test_version(monkeypatch):
    @cache
    def mock_get_engine():
        return create_engine("sqlite:///:memory:")

    monkeypatch.setattr(db, "_get_engine", mock_get_engine)
    monkeypatch.setenv("OGDC_ADMIN_PASSWORD", "password")
    with TestClient(app) as client:
        response = client.get("/version")
        assert response.status_code == 200
        assert response.json() == {"ogdc_runner_version": __version__}
