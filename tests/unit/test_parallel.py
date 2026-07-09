from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from ogdc_runner.models.parallel_config import ExecutionFunction
from ogdc_runner.models.recipe_config import (
    DataOneRecipeOutput,
    RecipeConfig,
    RecipeInput,
    ShellWorkflow,
    UrlInput,
)
from ogdc_runner.workflow.shell import (
    ShellParallelExecutionOrchestrator,
    make_and_submit_shell_workflow,
)


@pytest.fixture
def sample_recipe_config(tmpdir):
    recipe_dir = Path(str(tmpdir))
    fake_sh_file = tmpdir / "recipe.sh"
    fake_sh_file.write('echo "Processing"')

    return RecipeConfig(
        name="Test Parallel Recipe",
        input=RecipeInput(
            params=[
                UrlInput(value=f"https://example.com/file{i}.txt", type="url")
                for i in range(1, 6)
            ]
        ),
        output=DataOneRecipeOutput(dataone_id="12345"),
        workflow=ShellWorkflow.model_validate(
            {
                "sh_file": fake_sh_file.basename,
                "parallel": {
                    "enabled": True,
                    "partition_strategy": "files",
                    "partition_size": 2,
                },
            },
            context={"recipe_directory": recipe_dir},
        ),
        recipe_directory=recipe_dir,
    )


def test_orchestrator_creates_correct_partitions(sample_recipe_config):
    orchestrator = ShellParallelExecutionOrchestrator(
        recipe_config=sample_recipe_config,
        execution_function=ExecutionFunction(name="cmd-0", command="bash recipe.sh"),
    )
    partitions = orchestrator._create_partitions()

    assert len(partitions) == 3


def test_parallel_shell_workflow_uses_retained_partition_manifests(
    sample_recipe_config,
):
    rendered_workflows: list[dict[str, Any]] = []

    def fake_submit(workflow: Any, wait: bool = False) -> str:
        del wait
        rendered_workflows.append(workflow.to_dict())
        return "test-workflow"

    with patch("ogdc_runner.workflow.shell.submit_workflow", fake_submit):
        assert (
            make_and_submit_shell_workflow(sample_recipe_config, wait=False)
            == "test-workflow"
        )

    workflow = rendered_workflows[0]
    main_template = next(
        template
        for template in workflow["spec"]["templates"]
        if template["name"] == "main"
    )
    tasks = main_template["dag"]["tasks"]
    task_names = [task["name"] for task in tasks]

    assert "write-partition-manifests" in task_names
    cmd_task = next(task for task in tasks if task["name"] == "cmd-0")
    assert (
        cmd_task["withParam"]
        == "{{tasks.write-partition-manifests.outputs.parameters.partitions}}"
    )
    assert cmd_task["arguments"]["parameters"][0] == {
        "name": "partition-manifest-path",
        "value": (
            f"/mnt/workflow/{sample_recipe_config.id}/partition-manifests/"
            "shell-inputs/partition-{{item.partition_id}}.json"
        ),
    }


def test_execution_function_validation():
    with pytest.raises(Exception, match="Must specify exactly one of"):
        ExecutionFunction(name="test")
