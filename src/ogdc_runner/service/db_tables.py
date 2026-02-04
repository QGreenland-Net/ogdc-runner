from __future__ import annotations

from typing import Literal

from sqlmodel import Field, SQLModel


class User(SQLModel, table=True):
    """Model representing the `users` table in the OGDC database."""

    __tablename__ = "users"
    # `id` field is auto-created
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    password_hash: str


class Recipe(SQLModel, table=True):
    """Model representing the `recipes` table in the OGDC database."""

    __tablename__ = "recipes"

    id: int | None = Field(default=None, primary_key=True)
    # Name of the recipe
    name: str
    user_id: int = Field(default=None, foreign_key="users.id")
    # TODO: think more about how we should uniquely identify recipes. With
    # `index=True` we only allow e.g.,
    # `github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags` once.
    # We could allow overwrite, but could any user do this if the submitter
    # requesting overwrite isn't the same user who originally submitted? We
    # could make this behavior contengint on output type (temporary could be
    # overwritten by any user), but then we wouldn't be able to track temp
    # outputs because we'd need to overwrite `user_id` here, and then the
    # origianl user wouldn't be tied with the original execution...
    # We could make the combination of `user_id` and `source` unique, but that
    # only really works for temp outputs. For other types of output (e.g.,
    # dataone dataset, tilestore) we mgiht want to enforce uniqueness on source
    # because we wouldn't want someone to publish some data, and then ahve
    # someone else claim "credit" for it.
    # Also, `source` is unreliable bc refs can be manipulated (main changes, and
    # version tags _can_ shift, even if they shouldn't...)
    # If we need to base behavior off of Output type then should output type be
    # tracked in the db? Should we track the whole recipe config in the db?
    source: str = Field(index=True)
    status: Literal["INIT", "PROCESSING", "SUCCEEDED", "FAILED"] = "INIT"
    # TODO:
    # last_updated:
