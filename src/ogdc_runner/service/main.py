"""Basic service interface for the ogdc-runner.

This service sits between a user's recipe and the Argo workflows service that
does the work a user's recipe requests. The service translates the user recipe
into one or more Argo workflows that are executed.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import pydantic
from fastapi import FastAPI, Request
from loguru import logger
from starlette.middleware.sessions import SessionMiddleware
from typing import Any

from ogdc_runner import __version__
from ogdc_runner.service import auth_routes, db
from ogdc_runner.service.auth import auth_client


@asynccontextmanager
async def lifespan(_app: FastAPI):  # type: ignore[no-untyped-def]
    """Lifespan context manager for the FastAPI app.

    See: https://fastapi.tiangolo.com/advanced/events/#lifespan-function

    Ensures database tables are created.


    Code before the `yield` happens before the server is ready to take requests.
    Code after the `yield` happens as a final step as the server is shutdown.
    """
    logger.info("FastAPI Lifespan start")
    # Initialize the database.
    db.init_db()
    yield
    db.close_db()
    logger.info("FastAPI Lifespan end")


app = FastAPI(
    docs_url="/",
    version=__version__,
    title="Open Geospatial Data Cloud (OGDC) API",
    lifespan=lifespan,
    root_path=os.environ.get("API_ROOT_PATH", "/"),
)

app.add_middleware(
    SessionMiddleware, secret_key=os.getenv("SECRET_KEY", os.urandom(32).hex())
)

app.include_router(auth_routes.router)


class VersionResponse(pydantic.BaseModel):
    ogdc_runner_version: str = __version__


@app.get("/version")
def version() -> VersionResponse:
    """Return the OGDC runner version."""
    return VersionResponse()


@app.get("/login")
async def login(request: Request) -> Any:
    return await auth_client.login(
        redirect_uri=str(request.url_for("authorize")), request=request)


@app.get("/authorize")
async def authorize(request: Request) -> Any:
    return await auth_client.authorize(request=request)


@app.post("/refresh")
async def refresh(request: Request) -> Any:
    return await auth_client.refresh(await request.json())
