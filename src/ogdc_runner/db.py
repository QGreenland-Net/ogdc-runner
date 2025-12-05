from __future__ import annotations

import os
from collections.abc import Generator

from loguru import logger
from sqlmodel import Field, Session, SQLModel, create_engine, select


class User(SQLModel, table=True):
    __tablename__ = "users"
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    password_hash: str


# TODO: inject user/pass and service URL via envvars
ENGINE = create_engine(
    "postgresql://admin:password@postgres-service/ogdc",
)


# TODO: this is specifically for use with fastapi dependency injection, so maybe
# it belongs in `service.py`?
def get_session() -> Generator[Session, None, None]:
    with Session(ENGINE) as session:
        yield session


def create_admin_user() -> None:
    with Session(ENGINE) as session:
        results = session.exec(select(User).where(User.name == "admin")).one_or_none()
        if results:
            logger.info("Admin user already created.")
            return

        logger.info("OGDC admin user is being created.")
        admin_password = os.environ.get("OGDC_ADMIN_PASSWORD")
        if not admin_password:
            err_msg = "`OGDC_ADMIN_PASSWORD` envvar must be set."
            # TODO: more appropriate error than runtime
            raise RuntimeError(err_msg)

        session.add(
            User(
                name="admin",
                # TODO: actually hash the password!!
                password_hash=admin_password,
            )
        )
        session.commit()

    logger.info("OGDC admin user created.")


def init_db() -> None:
    logger.info("Ensuring database is ready on app startup...")
    SQLModel.metadata.create_all(ENGINE)
    create_admin_user()
    logger.info("Database tables are ready.")
