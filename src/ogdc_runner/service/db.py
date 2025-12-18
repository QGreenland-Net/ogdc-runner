"""Module containing code for interacting with the OGDC database."""

from __future__ import annotations

import os
from collections.abc import Generator
from functools import cache
from typing import Annotated

import sqlalchemy
from fastapi import Depends
from loguru import logger
from sqlmodel import Session, SQLModel, create_engine

from ogdc_runner.exceptions import OgdcMissingEnvvar


@cache
def get_engine() -> sqlalchemy.engine.base.Engine:
    db_user = os.environ.get("OGDC_DB_USERNAME")
    db_pass = os.environ.get("OGDC_DB_PASSWORD")
    if not db_user or not db_pass:
        err_msg = "`OGDC_DB_USERNAME` and `OGDC_DB_PASSWORD` must be set."
        raise OgdcMissingEnvvar(err_msg)

    engine = create_engine(
        f"postgresql://{db_user}:{db_pass}@ogdc-db-cnpg-rw/ogdc",
    )

    return engine


# Create a FastAPI dependency on the database session.
def _get_session() -> Generator[Session, None, None]:
    """Yield a database session for FastAPI routes."""
    with Session(get_engine()) as session:
        yield session


SessionDependency = Annotated[Session, Depends(_get_session)]


def close_db() -> None:
    logger.info("Disposing of Database engine.")
    get_engine().dispose()


def init_db() -> None:
    """Initialize the database with tables."""
    logger.info("Ensuring database is ready on app startup...")
    SQLModel.metadata.create_all(get_engine())
    logger.info("Database tables are ready.")
