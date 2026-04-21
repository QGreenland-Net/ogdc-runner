"""Module containing FastAPI routes requiring an access token."""

from __future__ import annotations

import datetime as dt
from typing import Annotated

import pydantic
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, FastAPI
from fastapi.security import OAuth2PasswordRequestForm

import shutil
import uuid

from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_workflow_status
from ogdc_runner.exceptions import OgdcOutputDataRetrievalError
from ogdc_runner.publish import get_temporary_output_data_url
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
    recipe_identifier: str | None

@router.post("/submit")
def submit(
    submit_recipe_request: SubmitRecipeRequest,
    background_tasks: BackgroundTasks,
) -> SubmitRecipeResponse:
    """Submit a recipe to OGDC for execution."""
    try:
        recipe_dir = stage_ogdc_recipe(submit_recipe_request.recipe_path)

        identifier = str(uuid.uuid4())    
        # use background_tasks to run the submission logic
        background_tasks.add_task(
            submit_ogdc_recipe,
            recipe_dir=recipe_dir,
            overwrite=submit_recipe_request.overwrite,
            identifier=identifier
        )

        # schedule the cleanup to run after the submission
        background_tasks.add_task(shutil.rmtree, recipe_dir)
            
        # return a generic success message immediately
        return SubmitRecipeResponse(
            message="Recipe submission accepted.",
            recipe_identifier=identifier
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initiate submission: {e}.",
        ) from e

class StatusResponse(pydantic.BaseModel):
    identifier: str
    status: str | None
    timestamp: dt.datetime = pydantic.Field(default_factory=dt.datetime.now)


@router.get("/status/{identifier}")
def status(identifier: str) -> StatusResponse:
    """Check an argo workflow's status."""
    status = get_workflow_status(identifier)
    return StatusResponse(
        identifier=identifier,
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


class OutputResponse(pydantic.BaseModel):
    data_url: str


@router.get("/output/{recipe_workflow_name}")
def get_output(recipe_workflow_name: str) -> OutputResponse:
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

    return OutputResponse(data_url=s3_location)
