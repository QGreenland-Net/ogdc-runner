"""Basic service interface for the ogdc-runner.

This service sits between a user's recipe and the Argo workflows service that
does the work a user's recipe requests. The service translates the user recipe
into one or more Argo workflows that are executed.
"""

from __future__ import annotations

import datetime as dt
import os
from contextlib import asynccontextmanager
from functools import cache
from typing import Annotated

import jwt
import pydantic
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jwt.exceptions import InvalidTokenError
from loguru import logger
from sqlmodel import Session

from ogdc_runner import __version__
from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_workflow_status
from ogdc_runner.db import User, close_db, get_auth_user, get_session, get_user, init_db
from ogdc_runner.exceptions import OgdcMissingEnvvar
from ogdc_runner.recipe import stage_ogdc_recipe

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


async def authenticated_user(
    token: Annotated[str, Depends(oauth2_scheme)],
    session: SessionDep,
) -> User:
    auth_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, _get_jwt_secret_key(), algorithms=[JWT_ALGORITHM])
        # Get the subject AKA username
        username = payload.get(JWT_USERNAME_KEY)
        if username is None:
            raise auth_exception

    except InvalidTokenError as e:
        raise auth_exception from e

    user = get_user(session=session, name=username)
    if not user:
        raise auth_exception

    return user


@cache
def _get_jwt_secret_key() -> str:
    jwt_secret_key = os.environ.get("OGDC_JWT_SECRET_KEY")
    if not jwt_secret_key:
        err_msg = "OGDC_JWT_SECRET_KEY envvar must be set."
        raise OgdcMissingEnvvar(err_msg)

    return jwt_secret_key


JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_TIMEDELTA = dt.timedelta(30)
JWT_USERNAME_KEY = "sub"


class Token(pydantic.BaseModel):
    access_token: str
    token_type: str = "Bearer"


def create_access_token(user: User) -> str:
    expire = dt.datetime.now() + ACCESS_TOKEN_EXPIRE_TIMEDELTA
    to_encode = {
        # "sub" is short for "subject", and is part of the JWT spec. We use it here
        # to just store the username, which should be unique.
        # See:
        # https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#technical-details-about-the-jwt-subject-sub
        "sub": user.name,
        "exp": expire,
    }
    encoded_jwt = jwt.encode(to_encode, _get_jwt_secret_key(), algorithm=JWT_ALGORITHM)

    return encoded_jwt


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
def submit(
    submit_recipe_request: SubmitRecipeRequest,
    # Ensure submissions require an authenticated user.
    _current_user: Annotated[str, Depends(authenticated_user)],
) -> SubmitRecipeResponse:
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


@app.get("/user")
async def get_current_user(
    current_user: Annotated[User, Depends(authenticated_user)],
) -> dict[str, str]:
    return {"current_user": current_user.name}


@app.post("/token")
def token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    session: SessionDep,
) -> Token:
    user = get_auth_user(
        session=session,
        name=form_data.username,
        password=form_data.password,
    )
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password.")

    access_token = create_access_token(user)

    return Token(access_token=access_token)
