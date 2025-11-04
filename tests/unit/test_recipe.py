from __future__ import annotations

from ogdc_runner.recipe import get_recipe_config


def test_get_recipe_config(test_shell_workflow_recipe_directory):
    config = get_recipe_config(
        recipe_directory=test_shell_workflow_recipe_directory,
    )

    assert config.id == "test-ogdc-workflow"
