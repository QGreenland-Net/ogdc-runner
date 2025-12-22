from __future__ import annotations

from pathlib import Path

import pytest

SHELL_RECIPE_TEST_PATH = Path(__file__).parent / "test_shell_workflow_recipe_dir"
VIZ_RECIPE_TEST_PATH = Path(__file__).parent / "test_viz_workflow_recipe_dir"
PARALLEL_SHELL_RECIPE_TEST_PATH = (
    Path(__file__).parent / "test_parallel_shell_recipe_dir"
)


@pytest.fixture
def test_shell_workflow_recipe_directory() -> Path:
    return SHELL_RECIPE_TEST_PATH


@pytest.fixture
def test_viz_workflow_recipe_directory() -> Path:
    return VIZ_RECIPE_TEST_PATH


@pytest.fixture
def test_parallel_shell_workflow_recipe_directory() -> Path:
    return PARALLEL_SHELL_RECIPE_TEST_PATH
