"""Module containing code related to authenticating an OGDC user.

Users authenticate by submitting a username and password to the `token/`
endpoint. If the username/password pair are valid, the response will contain a
JSON Web Token (JWT) that can be used to authenticate other endpoints via a
`Authorization: Bearer token` header.
"""

from __future__ import annotations

import datetime as dt
import os
from functools import cache
from typing import Annotated

import jwt
import pydantic
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jwt.exceptions import InvalidTokenError

from ogdc_runner.exceptions import OgdcMissingEnvvar
from ogdc_runner.service.db import (
    SessionDependency,
    User,
    get_user,
    get_user_with_password,
)

router = APIRouter()

# JWT token constants
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_TIMEDELTA = dt.timedelta(minutes=30)
JWT_USERNAME_KEY = "sub"


AUTH_TOKEN_URL = "/token"

# Create OAuth2 scheme for the application. The tokenUrl points to the `token/`
# route below.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=AUTH_TOKEN_URL)


async def get_user_by_auth_token(
    token: Annotated[str, Depends(oauth2_scheme)],
    session: SessionDependency,
) -> User:
    """Given a valid token, return the matching user."""
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


# Authenticated user depednency for FastAPI routes. Using this as a type
# annotation in an argument to a route results in that route needing to be
# passed a valid token.
AuthenticatedUserDependency = Annotated[User, Depends(get_user_by_auth_token)]


@cache
def _get_jwt_secret_key() -> str:
    """Get the JWT secret key used to encode and decode JWT tokens used by the
    app for auth.

    Requires the `OGDC_JWT_SECRET_KEY` be set.
    """
    jwt_secret_key = os.environ.get("OGDC_JWT_SECRET_KEY")
    if not jwt_secret_key:
        err_msg = "OGDC_JWT_SECRET_KEY envvar must be set."
        raise OgdcMissingEnvvar(err_msg)

    jwt_secret_key = str(jwt_secret_key)

    return jwt_secret_key


def create_access_token(user: User) -> tuple[str, dt.datetime]:
    """Create a JWT access token for authentication.

    Returns a tuple with the first element being the JWT access token and the
    second the expiration datetime in UTC.
    """
    expire = dt.datetime.utcnow() + ACCESS_TOKEN_EXPIRE_TIMEDELTA
    to_encode = {
        # "sub" is short for "subject", and is part of the JWT spec. We use it here
        # to just store the username, which should be unique.
        # See:
        # https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#technical-details-about-the-jwt-subject-sub
        "sub": user.name,
        "exp": expire,
    }
    encoded_jwt = jwt.encode(to_encode, _get_jwt_secret_key(), algorithm=JWT_ALGORITHM)

    return encoded_jwt, expire


class TokenResponse(pydantic.BaseModel):
    """Model representing token data returned by the app when auth is successful."""

    access_token: str
    token_type: str = "Bearer"
    utc_expiration: dt.datetime


@router.post(AUTH_TOKEN_URL)
def token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    session: SessionDependency,
) -> TokenResponse:
    """Given a username and password matching an existing user, return an access token.

    Token can be used to authenticate with other endpoints that use the
    `AuthenticatedUserDependency`.
    """
    user = get_user_with_password(
        session=session,
        name=form_data.username,
        password=form_data.password,
    )
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password.")

    access_token, utc_expiration_datetime = create_access_token(user)

    return TokenResponse(
        access_token=access_token,
        utc_expiration=utc_expiration_datetime,
    )
