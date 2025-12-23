"""Module containing FastAPI routes requiring an access token."""

from __future__ import annotations

import datetime as dt

import pydantic
from fastapi import APIRouter, Depends, HTTPException

from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_workflow_status
from ogdc_runner.recipe import stage_ogdc_recipe
from ogdc_runner.service import auth

router = APIRouter(
    # Require that all routes in this module be authenticated via an access
    # token.
    dependencies=[Depends(auth.get_user_by_auth_token)],
)


class SubmitRecipeRequest(pydantic.BaseModel):
    recipe_path: str
    overwrite: bool = False


class SubmitRecipeResponse(pydantic.BaseModel):
    message: str
    recipe_workflow_name: str | None


@router.post("/submit")
def submit(
    submit_recipe_request: SubmitRecipeRequest,
    # Ensure submissions require an authenticated user.
    # _current_user: auth.AuthenticatedUserDependency,
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


@router.get("/status/{recipe_workflow_name}")
def status(recipe_workflow_name: str) -> StatusResponse:
    """Check an argo workflow's status."""
    status = get_workflow_status(recipe_workflow_name)
    return StatusResponse(
        recipe_workflow_name=recipe_workflow_name,
        status=status,
    )


@router.get("/user")
async def get_current_user(
    current_user: auth.AuthenticatedUserDependency,
) -> dict[str, str]:
    """Return the current authenticated user.

    Useful for testing that authentication is working as expected.
    """
    return {"current_user": current_user.name}
