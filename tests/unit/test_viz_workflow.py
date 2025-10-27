from __future__ import annotations

import json
from pathlib import Path

from ogdc_runner.models.recipe_config import (
    InputParam,
    RecipeConfig,
    RecipeInput,
    VizWorkflow,
)
from ogdc_runner.recipe import get_recipe_config
from ogdc_runner.workflow.viz_workflow import get_viz_config_json


def test_get_viz_config_json(test_viz_workflow_recipe_directory):
    config = get_recipe_config(
        recipe_directory=test_viz_workflow_recipe_directory,
    )

    json_config = get_viz_config_json(recipe_config=config)

    data = json.loads(json_config)

    assert "deduplicate_clip_to_footprint" in data
    assert data["deduplicate_clip_to_footprint"] is False


def test_get_viz_config_json_defaults():
    config = RecipeConfig(
        name="test viz workflow with default config",
        workflow=VizWorkflow(
            config_file=None,
        ),
        input=RecipeInput(
            params=[
                InputParam(
                    value="https://example.com/path/to/data.gpkg",
                    type="url",
                ),
            ],
        ),
        recipe_directory=Path("/foo/"),
    )

    json_config = get_viz_config_json(recipe_config=config)

    assert json_config == "{}"
