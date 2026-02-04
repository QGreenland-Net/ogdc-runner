from __future__ import annotations

from pathlib import Path

import pytest

from ogdc_runner.models.parallel_config import ExecutionFunction
from ogdc_runner.models.recipe_config import (
    InputParam,
    RecipeConfig,
    RecipeInput,
    RecipeOutput,
    ShellWorkflow,
)
from ogdc_runner.workflow.shell import ShellParallelExecutionOrchestrator


@pytest.fixture
def sample_recipe_config(tmpdir):
    recipe_dir = Path(str(tmpdir))
    fake_sh_file = tmpdir / "recipe.sh"
    fake_sh_file.write('echo "Processing"')

    return RecipeConfig(
        name="Test Parallel Recipe",
        input=RecipeInput(
            params=[
                InputParam(value=f"https://example.com/file{i}.txt", type="url")
                for i in range(1, 6)
            ]
        ),
        output=RecipeOutput(dataone_id="12345"),
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


def test_execution_function_validation():
    with pytest.raises(Exception, match="Must specify exactly one of"):
        ExecutionFunction(name="test")
