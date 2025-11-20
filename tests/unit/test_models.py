from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import AnyUrl, ValidationError

from ogdc_runner.models.recipe_config import (
    InputParam,
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
