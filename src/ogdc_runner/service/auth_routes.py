"""Module containing FastAPI routes requiring an access token."""

from __future__ import annotations

import datetime as dt
import os

import pydantic
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer

from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_workflow_status
from ogdc_runner.exceptions import OgdcOutputDataRetrievalError
from ogdc_runner.publish import get_temporary_output_data_url
from ogdc_runner.recipe import stage_ogdc_recipe
from ogdc_runner.service.auth import auth_client

bearer_schema = HTTPBearer(auto_error=False)

SCOPE_ADMIN = os.getenv("OGDC_SCOPE_ADMIN", "ogdc:admin")

router = APIRouter(
    prefix="/workflows",
    tags=["workflows"],
    # Require that all routes in this module be authenticated via an access
    # token with appropriate scopes
    dependencies=[
        Depends(bearer_schema),
        Depends(auth_client.require_scope(SCOPE_ADMIN)),
    ],
)


class WorkflowSubmitRequest(pydantic.BaseModel):
    recipe_path: str
    overwrite: bool = False

class WorkflowSubmitResponse(pydantic.BaseModel):
    message: str
    recipe_workflow_name: str | None

@router.post(
    "",
    response_model=WorkflowSubmitResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_workflow(payload: WorkflowSubmitRequest) -> WorkflowSubmitResponse:
    """Submit a recipe to OGDC for execution.

    Requires a valid auth token and scope.
    """
    try:
        with stage_ogdc_recipe(payload.recipe_path) as recipe_dir:
            recipe_workflow_name = submit_ogdc_recipe(
                recipe_dir=recipe_dir,
                # Submitting a recipe should never wait - the api should be
                # responsive and async.
                wait=False,
                overwrite=payload.overwrite,
            )
            return WorkflowSubmitResponse(
                message=f"Successfully submitted recipe with {recipe_workflow_name=}",
                recipe_workflow_name=recipe_workflow_name,
            )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit recipe with error: {e}.",
        ) from e


class WorkflowDetailResponse(pydantic.BaseModel):
    recipe_workflow_name: str
    status: str | None
    timestamp: dt.datetime = pydantic.Field(default_factory=dt.datetime.now)


@router.get(
    "/{recipe_workflow_name}",
    response_model=WorkflowDetailResponse,
)
def get_workflow(recipe_workflow_name: str) -> WorkflowDetailResponse:
    """Check an argo workflow's status."""
    status = get_workflow_status(recipe_workflow_name)
    return WorkflowDetailResponse(
        recipe_workflow_name=recipe_workflow_name,
        status=status,
    )


class WorkflowOutputResponse(pydantic.BaseModel):
    data_url: str

@router.get(
    "/{recipe_workflow_name}/output",
    response_model=WorkflowOutputResponse,
)
def get_workflow_output(recipe_workflow_name: str) -> WorkflowOutputResponse:
    """Get a presigned s3 url for the outputs of the given recipe workflow.

    TODO: support other output types. This assumes a temporary output stored on
    Argo's artifact s3 storage.
    """
    try:
        s3_location = get_temporary_output_data_url(workflow_name=recipe_workflow_name)
    except OgdcOutputDataRetrievalError as e:
        raise HTTPException(
            status_code=404,
            detail=f"The requested output for {recipe_workflow_name} was not found: {e}.",
        ) from e

    return WorkflowOutputResponse(data_url=s3_location)
