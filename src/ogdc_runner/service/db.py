"""Module containing code for interacting with the OGDC database.

Defines database tables, configures a connection to the database, and includes
user creation code.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from functools import cache
from typing import Annotated

import sqlalchemy
from fastapi import Depends
from loguru import logger
from pwdlib import PasswordHash
from sqlmodel import Field, Session, SQLModel, create_engine, select

from ogdc_runner.exceptions import OgdcMissingEnvvar, OgdcUserAlreadyExists


class User(SQLModel, table=True):
    """Model representing the `users` table in the OGDC database."""

    __tablename__ = "users"
    # `id` field is auto-created
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    password_hash: str


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


def get_user(*, session: Session, name: str) -> User | None:
    results = session.exec(select(User).where(User.name == name)).one_or_none()

    return results


def get_user_with_password(
    *,
    session: Session,
    name: str,
    password: str,
) -> None | User:
    """Get the user record by name and password."""
    user = get_user(session=session, name=name)
    if user is None or not verify_password(
        password=password, hashed_password=user.password_hash
    ):
        return None

    return user


password_hash = PasswordHash.recommended()


def hash_password(password: str) -> str:
    return password_hash.hash(password)


def verify_password(*, password: str, hashed_password: str) -> bool:
    return password_hash.verify(password, hashed_password)


def create_user(*, username: str, password: str) -> User:
    """Create a user with the given username and password.

    Raises `OgdcUserAlreadyExists` if the given username is already in use.
    """
    with Session(get_engine()) as session:
        user = get_user(session=session, name=username)
        if user:
            err_msg = f"User with {username=} already created."
            raise OgdcUserAlreadyExists(err_msg)

        new_user = User(
            name="admin",
            password_hash=hash_password(password),
        )
        session.add(new_user)
        session.commit()

    logger.info(f"User {username} created.")

    return new_user


def create_admin_user() -> None:
    """Create the admin user if it does not already exist.

    Requires that the `OGDC_ADMIN_PASSWORD` envvar be set.
    """
    admin_password = os.environ.get("OGDC_ADMIN_PASSWORD")
    if not admin_password:
        err_msg = "`OGDC_ADMIN_PASSWORD` envvar must be set."
        raise OgdcMissingEnvvar(err_msg)

    try:
        create_user(username="admin", password=admin_password)
    except OgdcUserAlreadyExists:
        logger.info("Admin user already created.")


def init_db() -> None:
    """Initialize the database with tables and an admin user."""
    logger.info("Ensuring database is ready on app startup...")
    SQLModel.metadata.create_all(get_engine())
    logger.info("Database tables are ready.")
    create_admin_user()
    logger.info("Admin user is created.")
