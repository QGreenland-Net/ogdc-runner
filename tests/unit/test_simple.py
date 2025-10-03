from __future__ import annotations

from ogdc_runner.recipe import get_recipe_config


def test_get_recipe_config(test_recipe_directory):
    config = get_recipe_config(
        recipe_directory=test_recipe_directory,
    )

    assert config.recipe_directory == test_recipe_directory
    assert config.id == "test-ogdc-workflow"
