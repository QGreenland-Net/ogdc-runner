from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pydantic
import pytest
from fastapi.testclient import TestClient

from ogdc_runner import __version__
from ogdc_runner.exceptions import (
    OgdcInvalidRecipeConfig,
    OgdcInvalidRecipeDir,
    OgdcWorkflowExecutionError,
)
from ogdc_runner.service import auth_routes
from ogdc_runner.service.main import app


def test_version(mock_db):  # noqa: ARG001
    with TestClient(app) as client:
        response = client.get("/version")
        assert response.status_code == 200
        assert response.json() == {"ogdc_runner_version": __version__}


@pytest.fixture
def submit_client(mock_db, monkeypatch):  # noqa: ARG001
    """Client whose recipe staging/submission are replaced by a stub."""
    outcome: dict[str, Exception] = {}

    @contextmanager
    def fake_stage(_location):
        yield Path("/nonexistent")

    def fake_submit(**_kwargs):
        if "raises" in outcome:
            raise outcome["raises"]
        return "wf-name"

    monkeypatch.setattr(auth_routes, "stage_ogdc_recipe", fake_stage)
    monkeypatch.setattr(auth_routes, "submit_ogdc_recipe", fake_submit)
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client, outcome


def _post(client: TestClient) -> Any:
    return client.post("/workflows", json={"recipe_path": "github://o/r@m/recipes/x"})


def _validation_error() -> pydantic.ValidationError:
    class M(pydantic.BaseModel):
        n: int

    with pytest.raises(pydantic.ValidationError) as exc_info:
        M.model_validate({"n": "x"})
    return exc_info.value


def test_submit_success_body_unchanged(submit_client):
    client, _ = submit_client
    response = _post(client)
    assert response.status_code == 201
    assert response.json() == {
        "message": "Successfully submitted recipe with recipe_workflow_name='wf-name'",
        "recipe_workflow_name": "wf-name",
    }


@pytest.mark.parametrize(
    "error",
    [
        _validation_error(),
        OgdcInvalidRecipeConfig("bad config"),
        OgdcInvalidRecipeDir("no such dir"),
    ],
)
def test_submit_invalid_recipe_is_422(submit_client, error):
    client, outcome = submit_client
    outcome["raises"] = error
    response = _post(client)
    assert response.status_code == 422
    assert response.json()["detail"]


def test_submit_other_runner_errors_keep_message(submit_client):
    client, outcome = submit_client
    outcome["raises"] = OgdcWorkflowExecutionError("argo said no")
    response = _post(client)
    assert response.status_code == 500
    assert "argo said no" in response.json()["detail"]


def test_submit_unexpected_error_is_sanitized_500(submit_client):
    client, outcome = submit_client
    outcome["raises"] = RuntimeError("secret-token-abc at /internal/path")
    response = _post(client)
    assert response.status_code == 500
    detail = response.json()["detail"]
    assert "secret-token-abc" not in detail
    request_id = response.headers["X-Request-ID"]
    assert request_id in detail


def test_submit_steps_recipe_is_422_not_executable(mock_db, monkeypatch):  # noqa: ARG001
    recipe_dir = (
        Path(__file__).parents[1] / "test_steps_recipe_repo/recipes/sea-ice-conc"
    )

    @contextmanager
    def fake_stage(_location):
        yield recipe_dir

    monkeypatch.setattr(auth_routes, "stage_ogdc_recipe", fake_stage)
    with TestClient(app, raise_server_exceptions=False) as client:
        response = _post(client)
    assert response.status_code == 422
    assert "not executable yet" in response.json()["detail"]
