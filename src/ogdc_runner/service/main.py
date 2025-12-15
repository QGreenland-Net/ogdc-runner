"""Basic service interface for the ogdc-runner.

This service sits between a user's recipe and the Argo workflows service that
does the work a user's recipe requests. The service translates the user recipe
into one or more Argo workflows that are executed.
"""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager

import pydantic
from fastapi import FastAPI, HTTPException
from loguru import logger

from ogdc_runner import __version__
from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_workflow_status
from ogdc_runner.recipe import stage_ogdc_recipe
from ogdc_runner.service import auth
from ogdc_runner.service.db import (
    close_db,
    init_db,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):  # type: ignore[no-untyped-def]
    """Lifespan context manager for the FastAPI app.

    See: https://fastapi.tiangolo.com/advanced/events/#lifespan-function

    Ensures database tables are created.


    Code before the `yield` happens before the server is ready to take requests.
    Code after the `yield` happens as a final step as the server is shutdown.
    """
    logger.info("FastAPI Lifespan start")
    init_db()
    yield
    close_db()
    logger.info("FastAPI Lifespan end")


app = FastAPI(
    docs_url="/",
    version=__version__,
    title="Open Geospatial Data Cloud (OGDC) API",
    lifespan=lifespan,
)

app.include_router(auth.router)


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
def submit(
    submit_recipe_request: SubmitRecipeRequest,
    # Ensure submissions require an authenticated user.
    _current_user: auth.AuthenticatedUserDependency,
) -> SubmitRecipeResponse:
    """Submit a recipe to OGDC for execution.

    Requires a valid auth token.
    """
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


@app.get("/user")
async def get_current_user(
    current_user: auth.AuthenticatedUserDependency,
) -> dict[str, str]:
    """Return the current authenticated user.

    Useful for testing that authentication is working as expected.
    """
    return {"current_user": current_user.name}
