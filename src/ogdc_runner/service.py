"""Basic service interface for the ogdc-runner.

This service sits between a user's recipe and the Argo workflows service that
does the work a user's recipe requests. The service translates the user recipe
into one or more Argo workflows that are executed.
"""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from typing import Annotated

import pydantic
from fastapi import Depends, FastAPI, HTTPException
from sqlmodel import Session

from ogdc_runner import __version__
from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_workflow_status
from ogdc_runner.db import create_db_and_tables, get_session
from ogdc_runner.recipe import stage_ogdc_recipe


@asynccontextmanager
async def lifespan(_app: FastAPI):  # type: ignore[no-untyped-def]
    create_db_and_tables()
    yield


app = FastAPI(
    docs_url="/",
    version=__version__,
    title="Open Geospatial Data Cloud (OGDC) API",
    lifespan=lifespan,
)


SessionDep = Annotated[Session, Depends(get_session)]


class VersionResponse(pydantic.BaseModel):
    ogdc_runner_version: str = __version__


@app.get("/version")
def version() -> VersionResponse:
    """Return the OGDC runner version."""
    return VersionResponse()


class SubmitRecipeRequest(pydantic.BaseModel):
    recipe_path: str
    overwrite: bool = False


class SubmitRecipeResponse(pydantic.BaseModel):
    message: str
    recipe_workflow_name: str | None


@app.post("/submit")
def submit(submit_recipe_request: SubmitRecipeRequest) -> SubmitRecipeResponse:
    """Submit a recipe to OGDC for execution."""
    try:
        with stage_ogdc_recipe(submit_recipe_request.recipe_path) as recipe_dir:
            recipe_workflow_name = submit_ogdc_recipe(
                recipe_dir=recipe_dir,
                # Submitting a recipe should never wait - the api should be
                # responsive and async.
                wait=False,
                overwrite=submit_recipe_request.overwrite,
            )
            return SubmitRecipeResponse(
                message=f"Successfully submitted recipe with {recipe_workflow_name=}",
                recipe_workflow_name=recipe_workflow_name,
            )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit recipe with error: {e}.",
        ) from e


class StatusResponse(pydantic.BaseModel):
    recipe_workflow_name: str
    status: str | None
    timestamp: dt.datetime = pydantic.Field(default_factory=dt.datetime.now)


@app.get("/status/{recipe_workflow_name}")
def status(recipe_workflow_name: str) -> StatusResponse:
    """Check an argo workflow's status."""
    status = get_workflow_status(recipe_workflow_name)
    return StatusResponse(
        recipe_workflow_name=recipe_workflow_name,
        status=status,
    )
