from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import AnyUrl, ValidationError

from ogdc_runner.models.recipe_config import (
    InputParam,
    ParallelConfig,
    RecipeConfig,
    RecipeInput,
    RecipeOutput,
    ShellWorkflow,
)


def test_recipe_meta(tmpdir):
    fake_sh_file = tmpdir / "foo.sh"
    fake_sh_file.write('echo "FAKE PROCESSING"')
    recipe_input = RecipeInput(
        params=[InputParam(value=AnyUrl("http://www.example.com"), type="url")]
    )
    recipe_output = RecipeOutput(dataone_id="12345")
    name = "Test Recipe"
    recipe_id = "test-recipe"

    recipe_meta = RecipeConfig(
        name=name,
        input=recipe_input,
        output=recipe_output,
        workflow=ShellWorkflow.model_validate(
            {"sh_file": Path(fake_sh_file).name},
            context={"recipe_directory": Path(tmpdir)},
        ),
        recipe_directory=Path(tmpdir),
    )

    assert recipe_meta.name == name
    assert recipe_meta.id == recipe_id
    assert recipe_meta.input == recipe_input
    assert recipe_meta.output == recipe_output


def test_recipe_meta_failure_bad_id():
    recipe_input = RecipeInput(
        params=[InputParam(value=AnyUrl("http://www.example.com"), type="url")]
    )
    recipe_output = RecipeOutput(dataone_id="12345")

    # This name should raise a validation error, as `*` is not allowed.
    name = "Test Recipe*"

    with pytest.raises(ValidationError):
        RecipeConfig(
            name=name,
            input=recipe_input,
            output=recipe_output,
            workflow=ShellWorkflow(),
            recipe_directory=Path("/foo/"),
        )


def test_parallel_config_default():
    """Test default ParallelConfig values."""
    config = ParallelConfig()

    assert config.enabled is False
    assert config.partition_strategy == "files"
    assert config.partition_size is None


def test_parallel_config_custom():
    """Test ParallelConfig with custom values."""
    config = ParallelConfig(
        enabled=True,
        partition_strategy="files",
        partition_size=5,
    )

    assert config.enabled is True
    assert config.partition_strategy == "files"
    assert config.partition_size == 5


def test_shell_workflow_with_parallel_config(tmpdir):
    """Test ShellWorkflow with parallel configuration."""
    fake_sh_file = tmpdir / "recipe.sh"
    fake_sh_file.write('echo "Processing"')

    workflow = ShellWorkflow.model_validate(
        {
            "sh_file": fake_sh_file.basename,
            "parallel": {
                "enabled": True,
                "partition_strategy": "files",
                "partition_size": 3,
            },
        },
        context={"recipe_directory": tmpdir},
    )

    assert workflow.parallel.enabled is True
    assert workflow.parallel.partition_strategy == "files"
    assert workflow.parallel.partition_size == 3


def test_shell_workflow_without_parallel_config(tmpdir):
    """Test ShellWorkflow without parallel configuration uses defaults."""
    fake_sh_file = tmpdir / "recipe.sh"
    fake_sh_file.write('echo "Processing"')

    workflow = ShellWorkflow.model_validate(
        {"sh_file": fake_sh_file.basename},
        context={"recipe_directory": tmpdir},
    )

    # Should have default parallel config
    assert workflow.parallel.enabled is False
    assert workflow.parallel.partition_strategy == "files"
    assert workflow.parallel.partition_size is None


def test_recipe_config_with_parallel_workflow(tmpdir):
    """Test RecipeConfig with parallel workflow configuration."""
    fake_sh_file = tmpdir / "recipe.sh"
    fake_sh_file.write('echo "Processing"')

    recipe_input = RecipeInput(
        params=[
            InputParam(value=AnyUrl("http://www.example.com/file1.txt"), type="url"),
            InputParam(value=AnyUrl("http://www.example.com/file2.txt"), type="url"),
            InputParam(value=AnyUrl("http://www.example.com/file3.txt"), type="url"),
        ]
    )
    recipe_output = RecipeOutput(dataone_id="12345")

    recipe_config = RecipeConfig(
        name="Parallel Test Recipe",
        input=recipe_input,
        output=recipe_output,
        workflow=ShellWorkflow.model_validate(
            {
                "sh_file": fake_sh_file.basename,
                "parallel": {
                    "enabled": True,
                    "partition_strategy": "files",
                    "partition_size": 2,
                },
            },
            context={"recipe_directory": tmpdir},
        ),
        recipe_directory=tmpdir,
    )

    assert recipe_config.workflow.parallel.enabled is True
    assert recipe_config.workflow.parallel.partition_size == 2
    assert len(recipe_config.input.params) == 3
