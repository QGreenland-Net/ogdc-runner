"""Basic service interface for the ogdc-runner.

This service sits between a user's recipe and the Argo workflows service that
does the work a user's recipe requests. The service translates the user recipe
into one or more Argo workflows that are executed.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pydantic
from fastapi import FastAPI
from loguru import logger

from ogdc_runner import __version__
from ogdc_runner.service import auth, auth_routes
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
app.include_router(auth_routes.router)


class VersionResponse(pydantic.BaseModel):
    ogdc_runner_version: str = __version__


@app.get("/version")
def version() -> VersionResponse:
    """Return the OGDC runner version."""
    return VersionResponse()
