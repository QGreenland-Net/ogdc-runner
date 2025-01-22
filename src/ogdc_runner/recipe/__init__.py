from __future__ import annotations

import fsspec
import yaml

from ogdc_runner.constants import RECIPE_CONFIG_FILENAME
from ogdc_runner.models.recipe_config import RecipeConfig


def get_recipe_config(recipe_directory: str) -> RecipeConfig:
    """Extract config from a recipe configuration file (meta.yml)."""
    recipe_path = f"{recipe_directory}/{RECIPE_CONFIG_FILENAME}"
    with fsspec.open(recipe_path, "rt") as config_file:
        config_dict = yaml.safe_load(config_file)

    config = RecipeConfig(**config_dict, recipe_directory=recipe_directory)

    return config
