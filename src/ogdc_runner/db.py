from __future__ import annotations

from collections.abc import Generator

from loguru import logger
from sqlmodel import Field, Session, SQLModel, create_engine


class User(SQLModel, table=True):
    __tablename__ = "users"
    id: int | None = Field(default=None, primary_key=True)
    username: str = Field(index=True)
    password_hash: str


# TODO: inject user/pass and service URL via envvars
ENGINE = create_engine(
    "postgresql://admin:password@postgres-service/ogdc",
)


def create_db_and_tables() -> None:
    logger.info("Ensuring database is ready on app startup...")
    SQLModel.metadata.create_all(ENGINE)
    logger.info("Database tables are ready.")


def get_session() -> Generator[Session, None, None]:
    with Session(ENGINE) as session:
        yield session
