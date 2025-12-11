from __future__ import annotations

from functools import cache
from pathlib import Path

import pytest
from sqlmodel import create_engine

from ogdc_runner import db

SHELL_RECIPE_TEST_PATH = Path(__file__).parent / "test_shell_workflow_recipe_dir"
VIZ_RECIPE_TEST_PATH = Path(__file__).parent / "test_viz_workflow_recipe_dir"


@pytest.fixture
def test_shell_workflow_recipe_directory() -> Path:
    return SHELL_RECIPE_TEST_PATH


@pytest.fixture
def test_viz_workflow_recipe_directory() -> Path:
    return VIZ_RECIPE_TEST_PATH


@pytest.fixture
def mock_db(monkeypatch):
    @cache
    def mock_get_engine():
        return create_engine("sqlite:///:memory:")

    monkeypatch.setattr(db, "_get_engine", mock_get_engine)
    monkeypatch.setenv("OGDC_ADMIN_PASSWORD", "password")
