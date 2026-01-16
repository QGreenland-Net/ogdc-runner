from __future__ import annotations

from functools import cache
from pathlib import Path

import pytest
from sqlmodel import StaticPool, create_engine

from ogdc_runner.service import db

SHELL_RECIPE_TEST_PATH = Path(__file__).parent / "test_shell_workflow_recipe_dir"
VIZ_RECIPE_TEST_PATH = Path(__file__).parent / "test_viz_workflow_recipe_dir"


@pytest.fixture
def test_shell_workflow_recipe_directory() -> Path:
    return SHELL_RECIPE_TEST_PATH


@pytest.fixture
def test_viz_workflow_recipe_directory() -> Path:
    return VIZ_RECIPE_TEST_PATH


@pytest.fixture
def test_temp_output_recipe_directory() -> Path:
    return Path(__file__).parent / "test_temp_output_recipe_dir"


@pytest.fixture
def mock_db(monkeypatch):
    """Fixture to mock out the OGDC API database.

    Uses an in-memory sqlite database instead of the live postgresql database.

    Ensures required envvars are set:
        * `OGDC_JWT_SECRET_KEY`
        * `OGDC_ADMIN_PASSWORD` (set to `password`)
    """

    @cache
    def mock_get_engine():
        return create_engine(
            "sqlite:///:memory:",
            # Needed for pytest running tests in different threads.
            connect_args={"check_same_thread": False},
            # Use one connection for all requests during tests.
            # https://docs.sqlalchemy.org/en/13/core/pooling.html#sqlalchemy.pool.StaticPool
            poolclass=StaticPool,
        )

    monkeypatch.setattr(db, "get_engine", mock_get_engine)
    monkeypatch.setenv("OGDC_ADMIN_PASSWORD", "password")
    monkeypatch.setenv(
        "OGDC_JWT_SECRET_KEY",
        "2ae25b5398824129235724f243811d7a335a98339abe4630e4e27d25e4f144a2",
    )
