"""Module containing code for managing OGDC users."""

from __future__ import annotations

import os

from loguru import logger
from pwdlib import PasswordHash
from sqlmodel import Field, Session, SQLModel, select

from ogdc_runner.exceptions import OgdcMissingEnvvar, OgdcUserAlreadyExists
from ogdc_runner.service import db

password_hash = PasswordHash.recommended()


ADMIN_USERNAME = "admin"


class User(SQLModel, table=True):
    """Model representing the `users` table in the OGDC database."""

    __tablename__ = "users"
    # `id` field is auto-created
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    password_hash: str


def hash_password(password: str) -> str:
    return password_hash.hash(password)


def verify_password(*, password: str, hashed_password: str) -> bool:
    return password_hash.verify(password, hashed_password)


def get_user(*, session: Session, name: str) -> User | None:
    results = session.exec(select(User).where(User.name == name)).one_or_none()

    return results


def get_user_with_password(
    *,
    session: Session,
    name: str,
    password: str,
) -> None | User:
    """Get the user record by name and password.


    Returns `None` if no matching user is found.
    """
    user = get_user(session=session, name=name)
    if user is None or not verify_password(
        password=password, hashed_password=user.password_hash
    ):
        return None

    return user


def create_user(*, session: Session, username: str, password: str) -> User:
    """Create a user with the given username and password.

    Raises `OgdcUserAlreadyExists` if the given username is already in use.
    """
    user = get_user(session=session, name=username)
    if user:
        err_msg = f"User with {username=} already created."
        raise OgdcUserAlreadyExists(err_msg)

    new_user = User(
        name=username,
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
        with Session(db.get_engine()) as session:
            create_user(
                session=session,
                username=ADMIN_USERNAME,
                password=admin_password,
            )
    except OgdcUserAlreadyExists:
        logger.info("Admin user already created.")
