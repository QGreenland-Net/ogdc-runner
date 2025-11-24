from __future__ import annotations

from fastapi.testclient import TestClient

from ogdc_runner.service import app

client = TestClient(app)


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World!"}
