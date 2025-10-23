from __future__ import annotations

from pathlib import Path

import pytest

SHELL_RECIPE_TEST_PATH = Path(__file__).parent / "test_recipe_dir"


@pytest.fixture
def test_recipe_directory() -> Path:
    return SHELL_RECIPE_TEST_PATH
