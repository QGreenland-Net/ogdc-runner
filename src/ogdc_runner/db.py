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


db_user = os.environ.get("OGDC_DB_USERNAME")
db_pass = os.environ.get("OGDC_DB_PASSWORD")
if not db_user or not db_pass:
    # TODO: more appropriate error than runtime
    err_msg = "`OGDC_DB_USERNAME` and `OGDC_DB_PASSWORD` must be set."
    raise RuntimeError(err_msg)


ENGINE = create_engine(
    f"postgresql://{db_user}:{db_pass}@ogdc-db-cnpg-rw/ogdc",
)


# TODO: this is specifically for use with fastapi dependency injection, so maybe
# it belongs in `service.py`?
def get_session() -> Generator[Session, None, None]:
    with Session(ENGINE) as session:
        yield session


def get_user(*, session: Session, name: str) -> User | None:
    results = session.exec(select(User).where(User.name == name)).one_or_none()

    return results


def hash_password(password: str) -> str:
    # TODO: actually hash the password!!
    return password


def create_admin_user() -> None:
    with Session(ENGINE) as session:
        user = get_user(session=session, name="admin")
        if user:
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
                password_hash=hash_password(admin_password),
            )
        )
        session.commit()

    logger.info("OGDC admin user created.")


def init_db() -> None:
    logger.info("Ensuring database is ready on app startup...")
    SQLModel.metadata.create_all(ENGINE)
    create_admin_user()
    logger.info("Database tables are ready.")
