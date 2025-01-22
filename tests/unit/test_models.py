from __future__ import annotations

import pytest
from pydantic import AnyUrl, ValidationError

from ogdc_runner.models.recipe_config import RecipeConfig, RecipeInput, RecipeOutput


def test_recipe_meta():
    recipe_input = RecipeInput(url=AnyUrl("http://www.example.com"))
    recipe_output = RecipeOutput(dataone_id="12345")
    name = "Test Recipe"
    recipe_id = "test-recipe"

    recipe_meta = RecipeConfig(
        name=name,
        id=recipe_id,
        input=recipe_input,
        output=recipe_output,
        recipe_directory="/foo/",
    )

    assert recipe_meta.name == name
    assert recipe_meta.id == recipe_id
    assert recipe_meta.input == recipe_input
    assert recipe_meta.output == recipe_output


def test_recipe_meta_failure_bad_id():
    recipe_input = RecipeInput(url=AnyUrl("http://www.example.com"))
    recipe_output = RecipeOutput(dataone_id="12345")
    name = "Test Recipe"

    recipe_id = "test_recipe"
    with pytest.raises(ValidationError):
        # Underscores are not allowed, this should trigger the validation error.
        RecipeConfig(
            name=name,
            id=recipe_id,
            input=recipe_input,
            output=recipe_output,
            recipe_directory="/foo/",
        )
