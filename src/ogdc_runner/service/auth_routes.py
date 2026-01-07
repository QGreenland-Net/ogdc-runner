"""Module containing FastAPI routes requiring an access token."""

from __future__ import annotations

import datetime as dt
from typing import Annotated

import pydantic
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm

from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_workflow_status
from ogdc_runner.recipe import stage_ogdc_recipe
from ogdc_runner.service import auth, db, user

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


class CreateUserResponse(pydantic.BaseModel):
    message: str


@router.post("/create_user")
def create_user_route(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    session: db.SessionDependency,
    authenticated_user: auth.AuthenticatedUserDependency,
) -> CreateUserResponse:
    """Create a user with the given username and password.

    Requires an valid access token for the `admin` user.

    Returns a 409 status code if the user already exists.
    """
    # First, ensure that the authenticated user is the admin. Only admin gets to
    # create new users.
    if authenticated_user.name != user.ADMIN_USERNAME:
        raise HTTPException(
            status_code=401,
            detail="Access token must belong to admin.",
        )

    # Check if an existing user already exists with the provided username.
    existing_user = user.get_user(
        session=session,
        name=form_data.username,
    )

    if existing_user is not None:
        raise HTTPException(
            status_code=409,
            detail=f"User with username {form_data.username} already exists.",
        )

    # Create the new user
    new_user = user.create_user(
        session=session, username=form_data.username, password=form_data.password
    )

    return CreateUserResponse(message=f'User with username "{new_user.name}" created.')
