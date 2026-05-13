from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest

from ogdc_runner.constants import MAX_PARALLEL_LIMIT
from ogdc_runner.exceptions import OgdcInvalidRecipeConfig
from ogdc_runner.models.recipe_config import (
    ParallelConfig,
    RecipeConfig,
    RecipeInput,
    UrlInput,
    VizWorkflow,
)
from ogdc_runner.recipe import get_recipe_config
from ogdc_runner.workflow.viz_workflow import make_and_submit_viz_workflow


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
                UrlInput(
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
                    UrlInput(
                        value="https://example.com/path/to/data.gpkg",
                        type="url",
                    ),
                ],
            ),
            recipe_directory=tmp_path,
        )


def _render_viz_workflow(config: RecipeConfig) -> dict[str, Any]:
    rendered_workflows: list[dict[str, Any]] = []

    def fake_submit(workflow: Any, wait: bool = False) -> str:
        del wait
        rendered_workflows.append(workflow.to_dict())
        return "test-workflow"

    with patch("ogdc_runner.workflow.viz_workflow.submit_workflow", fake_submit):
        assert make_and_submit_viz_workflow(config, wait=False) == "test-workflow"

    return rendered_workflows[0]


def _main_dag_tasks(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    main_template = next(
        template
        for template in workflow["spec"]["templates"]
        if template["name"] == "main"
    )
    return cast("list[dict[str, Any]]", main_template["dag"]["tasks"])


def test_viz_workflow_renders_serial_dag_when_parallel_disabled(
    test_viz_workflow_recipe_directory,
):
    config = get_recipe_config(
        recipe_directory=test_viz_workflow_recipe_directory,
    )
    workflow = _render_viz_workflow(config)
    tasks = _main_dag_tasks(workflow)

    assert "parallelism" not in workflow["spec"]
    assert [task["name"] for task in tasks] == ["setup-config", "run-viz-serial"]
    assert all("withParam" not in task for task in tasks)


def test_viz_workflow_renders_parallel_dag_with_default_cap(
    test_viz_workflow_recipe_directory,
):
    config = get_recipe_config(
        recipe_directory=test_viz_workflow_recipe_directory,
    )
    config.workflow.parallel = ParallelConfig(enabled=True)
    workflow = _render_viz_workflow(config)
    tasks = _main_dag_tasks(workflow)

    assert workflow["spec"]["parallelism"] == MAX_PARALLEL_LIMIT
    assert any("withParam" in task for task in tasks)


def test_viz_workflow_honors_max_parallelism(
    test_viz_workflow_recipe_directory,
):
    config = get_recipe_config(
        recipe_directory=test_viz_workflow_recipe_directory,
    )
    config.workflow.parallel = ParallelConfig(enabled=True, max_parallelism=17)
    workflow = _render_viz_workflow(config)

    assert workflow["spec"]["parallelism"] == 17
