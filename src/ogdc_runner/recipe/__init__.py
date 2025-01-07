from __future__ import annotations

from pathlib import Path
import fsspec

import yaml

from ogdc_runner.constants import RECIPE_CONFIG_FILENAME
from ogdc_runner.models.recipe_config import RecipeConfig


def get_recipe_config(recipe_directory: str) -> RecipeConfig:
    """Extract config from a recipe configuration file (meta.yml)."""
    if 'github' in recipe_directory:
        fs = fsspec.filesystem("github", org='QGreenland-Net', repo='ogdc-recipes')
        recipe_path = recipe_directory + '/' + RECIPE_CONFIG_FILENAME
        with fs.open(recipe_path, "rt") as config_file:
            config_dict = yaml.safe_load(config_file)
    else:
        with (Path(recipe_directory) / RECIPE_CONFIG_FILENAME).open() as config_file:
            config_dict = yaml.safe_load(config_file)

    config = RecipeConfig(**config_dict)

    return config
