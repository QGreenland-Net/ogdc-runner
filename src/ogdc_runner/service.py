"""Basic service interface for the ogdc-runner.

This service sits between a user's recipe and the Argo workflows service that
does the work a user's recipe requests. The service translates the user recipe
into one or more Argo workflows that are executed.
"""

from __future__ import annotations

import datetime as dt
from typing import Literal

import pydantic
from fastapi import FastAPI

from ogdc_runner import __version__
from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_workflow_status
from ogdc_runner.recipe import stage_ogdc_recipe

app = FastAPI(docs_url="/")


class VersionResponse(pydantic.BaseModel):
    ogdc_runner_version: str = __version__


@app.get("/version")
def version() -> VersionResponse:
    """Return the OGDC runner version."""
    return VersionResponse()


class SubmitRecipeInput(pydantic.BaseModel):
    recipe_path: str
    overwrite: bool = False


class SubmitRecipeResponse(pydantic.BaseModel):
    status: Literal["success", "failed"]
    message: str
    recipe_workflow_name: str


@app.post("/submit")
def submit(submit_recipe_input: SubmitRecipeInput) -> SubmitRecipeResponse:
    """Submit a recipe to OGDC for execution."""
    with stage_ogdc_recipe(submit_recipe_input.recipe_path) as recipe_dir:
        recipe_workflow_name = submit_ogdc_recipe(
            recipe_dir=recipe_dir,
            # Submitting a recipe should never wait - the api should be
            # responsive and async.
            wait=False,
            overwrite=submit_recipe_input.overwrite,
        )
        return SubmitRecipeResponse(
            status="success",
            message=f"Successfully submitted recipe with {recipe_workflow_name=}",
            recipe_workflow_name=recipe_workflow_name,
        )

    # TODO: handle failure case


class StatusResponse(pydantic.BaseModel):
    recipe_workflow_name: str
    status: str | None
    timestamp: dt.datetime = dt.datetime.now()


@app.get("/status/{recipe_workflow_name}")
def status(recipe_workflow_name: str) -> StatusResponse:
    """Check an argo workflow's status."""
    status = get_workflow_status(recipe_workflow_name)
    return StatusResponse(
        recipe_workflow_name=recipe_workflow_name,
        status=status,
    )
