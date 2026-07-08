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
    PvcMountInput,
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


def _template(workflow: dict[str, Any], name: str) -> dict[str, Any]:
    return next(
        template
        for template in workflow["spec"]["templates"]
        if template["name"] == name
    )


def _assert_references_existing_claims_only(workflow: dict[str, Any]) -> None:
    spec = workflow["spec"]
    assert "volumeClaimTemplates" not in spec
    for volume in spec["volumes"]:
        assert set(volume) <= {"name", "persistentVolumeClaim"}
        assert "claimName" in volume["persistentVolumeClaim"]


def _make_direct_pvc_viz_config(
    tmp_path: Path,
    workflow_config: dict[str, Any],
) -> RecipeConfig:
    (tmp_path / "config.json").write_text(json.dumps(workflow_config))
    return RecipeConfig(
        name="direct pvc viz",
        workflow=VizWorkflow.model_validate(
            {
                "config_file": "config.json",
                "parallel": {
                    "enabled": True,
                    "partition_size": 5,
                },
            },
            context={"recipe_directory": tmp_path},
        ),
        input=RecipeInput(
            params=[
                PvcMountInput(
                    claim_name="ogdc-test-pvc",
                    path="/tiles/",
                    glob="*.gpkg",
                )
            ]
        ),
        recipe_directory=tmp_path,
    )


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


def test_viz_workflow_renders_pvc_mount_input(
    monkeypatch,
    test_viz_workflow_recipe_directory,
):
    monkeypatch.setenv(
        "OGDC_ALLOWED_INPUT_PVCS",
        '[{"claimName": "ogdc-test-pvc", "description": "test data"}]',
    )
    config = get_recipe_config(
        recipe_directory=test_viz_workflow_recipe_directory,
    )
    config.workflow.parallel = ParallelConfig(enabled=True, partition_size=10)
    config.input = RecipeInput(
        params=[
            PvcMountInput(
                claim_name="ogdc-test-pvc",
                path="/tiles/OGDC/QGnet/viz-workflow/",
                glob="ice_basins.gpkg",
            )
        ]
    )

    workflow = _render_viz_workflow(config)
    tasks = _main_dag_tasks(workflow)

    _assert_references_existing_claims_only(workflow)
    assert {volume["name"] for volume in workflow["spec"]["volumes"]} == {
        "workflow-volume",
        "input-pvc-ogdc-test-pvc",
    }
    assert "list-pvc-files" in [task["name"] for task in tasks]
    stage_task = next(task for task in tasks if task["name"] == "stage-files")
    assert (
        stage_task["withParam"]
        == "{{tasks.list-pvc-files.outputs.parameters.partitions}}"
    )

    listing_template = _template(workflow, "list-pvc-files")
    listing_mounts = {
        mount["name"] for mount in listing_template["container"]["volumeMounts"]
    }
    assert "input-pvc-ogdc-test-pvc" in listing_mounts

    stage_template = _template(workflow, "stage-files")
    stage_mounts = {mount["name"] for mount in stage_template["script"]["volumeMounts"]}
    assert "input-pvc-ogdc-test-pvc" in stage_mounts


def test_serial_viz_workflow_renders_pvc_mount_input(
    monkeypatch,
    test_viz_workflow_recipe_directory,
):
    monkeypatch.setenv("OGDC_ALLOWED_INPUT_PVCS", '["ogdc-test-pvc"]')
    config = get_recipe_config(
        recipe_directory=test_viz_workflow_recipe_directory,
    )
    config.input = RecipeInput(
        params=[
            PvcMountInput(
                claim_name="ogdc-test-pvc",
                path="/tiles/OGDC/QGnet/viz-workflow/",
                glob="ice_basins.gpkg",
            )
        ]
    )

    workflow = _render_viz_workflow(config)
    tasks = _main_dag_tasks(workflow)

    _assert_references_existing_claims_only(workflow)
    assert [task["name"] for task in tasks] == [
        "setup-config",
        "list-pvc-files",
        "run-viz-serial",
    ]
    serial_task = next(task for task in tasks if task["name"] == "run-viz-serial")
    assert (
        serial_task["arguments"]["parameters"][1]["value"]
        == "{{tasks.list-pvc-files.outputs.parameters.files}}"
    )

    listing_template = _template(workflow, "list-pvc-files")
    listing_mounts = {
        mount["name"] for mount in listing_template["container"]["volumeMounts"]
    }
    assert "input-pvc-ogdc-test-pvc" in listing_mounts

    serial_template = _template(workflow, "run-viz-serial")
    serial_mounts = {
        mount["name"] for mount in serial_template["script"]["volumeMounts"]
    }
    assert "input-pvc-ogdc-test-pvc" in serial_mounts
    assert "workflow.stage(local_path)" in serial_template["script"]["source"]
    assert "symlink_to" not in serial_template["script"]["source"]


def test_viz_pvc_mounts_do_not_leak_to_later_non_pvc_render(
    monkeypatch,
    test_viz_workflow_recipe_directory,
):
    monkeypatch.setenv("OGDC_ALLOWED_INPUT_PVCS", '["ogdc-test-pvc"]')
    pvc_config = get_recipe_config(
        recipe_directory=test_viz_workflow_recipe_directory,
    )
    pvc_config.workflow.parallel = ParallelConfig(enabled=True, partition_size=10)
    pvc_config.input = RecipeInput(
        params=[
            PvcMountInput(
                claim_name="ogdc-test-pvc",
                path="/tiles/OGDC/QGnet/viz-workflow/",
                glob="ice_basins.gpkg",
            )
        ]
    )

    pvc_workflow = _render_viz_workflow(pvc_config)
    pvc_stage_template = _template(pvc_workflow, "stage-files")
    pvc_stage_mounts = {
        mount["name"] for mount in pvc_stage_template["script"]["volumeMounts"]
    }
    assert "input-pvc-ogdc-test-pvc" in pvc_stage_mounts

    plain_config = get_recipe_config(
        recipe_directory=test_viz_workflow_recipe_directory,
    )
    plain_config.workflow.parallel = ParallelConfig(enabled=True, partition_size=10)
    plain_workflow = _render_viz_workflow(plain_config)
    plain_stage_template = _template(plain_workflow, "stage-files")
    plain_stage_mounts = {
        mount["name"] for mount in plain_stage_template["script"]["volumeMounts"]
    }

    assert plain_stage_mounts == {"workflow-volume"}


def test_parallel_viz_pvc_can_start_at_raster_and_3dtiles(monkeypatch, tmp_path):
    monkeypatch.setenv("OGDC_ALLOWED_INPUT_PVCS", '["ogdc-test-pvc"]')
    config = _make_direct_pvc_viz_config(
        tmp_path,
        {
            "dir_staged": "/mnt/data/ogdc-test-pvc/data/10.18739/A26Q1SK18/staged",
            "dir_3dtiles": "output/3dtiles",
            "enable_stager": False,
            "enable_raster": True,
            "enable_raster_parents": False,
            "enable_web_tiles": False,
            "enable_3dtiles": True,
        },
    )

    workflow = _render_viz_workflow(config)
    tasks = _main_dag_tasks(workflow)
    task_names = [task["name"] for task in tasks]

    assert "list-pvc-files" in task_names
    assert "stage-files" not in task_names
    assert "discover-staged-tiles" not in task_names

    raster_task = next(task for task in tasks if task["name"] == "rasterize-max-z")
    threed_task = next(task for task in tasks if task["name"] == "create-3dtiles")
    assert (
        raster_task["withParam"]
        == "{{tasks.list-pvc-files.outputs.parameters.partitions}}"
    )
    assert raster_task["arguments"]["parameters"][1]["value"] == "{{item.files}}"
    assert (
        threed_task["withParam"]
        == "{{tasks.list-pvc-files.outputs.parameters.partitions}}"
    )
    assert threed_task["arguments"]["parameters"][1]["value"] == "{{item.files}}"

    setup_template = _template(workflow, "stage-viz-config")
    setup_script = setup_template["container"]["args"][0]
    assert "output/3dtiles" in setup_script
    assert "mkdir -p /mnt/data" not in setup_script
    assert "/mnt/workflow/direct-pvc-viz//mnt/data" not in setup_script

    threed_template = _template(workflow, "create-3dtiles")
    threed_mounts = {
        mount["name"] for mount in threed_template["script"]["volumeMounts"]
    }
    assert "input-pvc-ogdc-test-pvc" in threed_mounts


def test_parallel_viz_pvc_can_start_at_webtiles(monkeypatch, tmp_path):
    monkeypatch.setenv("OGDC_ALLOWED_INPUT_PVCS", '["ogdc-test-pvc"]')
    config = _make_direct_pvc_viz_config(
        tmp_path,
        {
            "enable_stager": False,
            "enable_raster": False,
            "enable_raster_parents": True,
            "enable_web_tiles": True,
            "enable_3dtiles": False,
        },
    )

    workflow = _render_viz_workflow(config)
    tasks = _main_dag_tasks(workflow)
    task_names = [task["name"] for task in tasks]

    assert "list-pvc-files" in task_names
    assert "discover-all-geotiffs" not in task_names
    assert not any(task_name.startswith("discover-parents") for task_name in task_names)

    web_tile_task = next(task for task in tasks if task["name"] == "create-web-tiles")
    assert (
        web_tile_task["withParam"]
        == "{{tasks.list-pvc-files.outputs.parameters.partitions}}"
    )
    assert web_tile_task["arguments"]["parameters"][1]["value"] == "{{item.files}}"

    web_tile_template = _template(workflow, "create-web-tiles")
    assert "isinstance(item, dict)" in web_tile_template["script"]["source"]
    web_tile_mounts = {
        mount["name"] for mount in web_tile_template["script"]["volumeMounts"]
    }
    assert "input-pvc-ogdc-test-pvc" in web_tile_mounts
