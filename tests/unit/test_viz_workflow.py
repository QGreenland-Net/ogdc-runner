from __future__ import annotations

import json
from pathlib import Path

import pytest

from ogdc_runner.exceptions import OgdcInvalidRecipeConfig
from ogdc_runner.models.recipe_config import (
    InputParam,
    RecipeConfig,
    RecipeInput,
    VizWorkflow,
)
from ogdc_runner.recipe import get_recipe_config


def test_get_viz_config_json(test_viz_workflow_recipe_directory):
    config = get_recipe_config(
        recipe_directory=test_viz_workflow_recipe_directory,
    )

    assert config.workflow.type == "visualization"
    json_config = config.workflow.get_config_file_json()

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

    assert config.workflow.type == "visualization"
    json_config = config.workflow.get_config_file_json()

    assert json_config == "{}"


def test_get_viz_config_json_invalid_json(tmp_path):
    bad_conf_file = tmp_path / "bad.json"
    with bad_conf_file.open("w") as f:
        f.write("{not valid json!")

    with pytest.raises(OgdcInvalidRecipeConfig):
        RecipeConfig(
            name="test viz workflow with default config",
            workflow=VizWorkflow.model_validate(
                {"config_file": str(bad_conf_file)},
                context={"recipe_directory": tmp_path},
            ),
            input=RecipeInput(
                params=[
                    InputParam(
                        value="https://example.com/path/to/data.gpkg",
                        type="url",
                    ),
                ],
            ),
            recipe_directory=tmp_path,
        )
