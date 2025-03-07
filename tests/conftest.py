from __future__ import annotations

from pathlib import Path

import pytest

SIMPLE_RECIPE_TEST_PATH = str(Path(__file__).parent / "test_recipe_dir")


@pytest.fixture
def test_recipe_directory() -> str:
    return SIMPLE_RECIPE_TEST_PATH
