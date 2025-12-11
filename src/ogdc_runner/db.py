from __future__ import annotations

import os
from collections.abc import Generator
from functools import cache

import sqlalchemy
from loguru import logger
from pwdlib import PasswordHash
from sqlmodel import Field, Session, SQLModel, create_engine, select

from ogdc_runner.exceptions import OgdcMissingEnvvar


class User(SQLModel, table=True):
    __tablename__ = "users"
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    password_hash: str


@cache
def _get_engine() -> sqlalchemy.engine.base.Engine:
    db_user = os.environ.get("OGDC_DB_USERNAME")
    db_pass = os.environ.get("OGDC_DB_PASSWORD")
    if not db_user or not db_pass:
        err_msg = "`OGDC_DB_USERNAME` and `OGDC_DB_PASSWORD` must be set."
        raise OgdcMissingEnvvar(err_msg)

    engine = create_engine(
        f"postgresql://{db_user}:{db_pass}@ogdc-db-cnpg-rw/ogdc",
    )

    return engine


# TODO: this is specifically for use with fastapi dependency injection, so maybe
# it belongs in `service.py`?
def get_session() -> Generator[Session, None, None]:
    with Session(_get_engine()) as session:
        yield session


def close_db() -> None:
    logger.info("Disposing of Database engine.")
    _get_engine().dispose()


def get_user(*, session: Session, name: str) -> User | None:
    results = session.exec(select(User).where(User.name == name)).one_or_none()

    return results


def get_auth_user(session: Session, name: str, password: str) -> None | User:
    user = get_user(session=session, name=name)
    if user is None or not verify_password(
        password=password, hashed_password=user.password_hash
    ):
        return None

    return user


# to get a string like this run:
# openssl rand -hex 32
password_hash = PasswordHash.recommended()


def hash_password(password: str) -> str:
    return password_hash.hash(password)


def verify_password(*, password: str, hashed_password: str) -> bool:
    return password_hash.verify(password, hashed_password)


def create_admin_user() -> None:
    with Session(_get_engine()) as session:
        user = get_user(session=session, name="admin")
        if user:
            logger.info("Admin user already created.")
            return

        logger.info("OGDC admin user is being created.")
        admin_password = os.environ.get("OGDC_ADMIN_PASSWORD")
        if not admin_password:
            err_msg = "`OGDC_ADMIN_PASSWORD` envvar must be set."
            raise OgdcMissingEnvvar(err_msg)

        session.add(
            User(
                name="admin",
                password_hash=hash_password(admin_password),
            )
        )
        session.commit()

    logger.info("OGDC admin user created.")


def init_db() -> None:
    logger.info("Ensuring database is ready on app startup...")
    SQLModel.metadata.create_all(_get_engine())
    logger.info("Database tables are ready.")
    create_admin_user()
    logger.info("Admin user is created.")
