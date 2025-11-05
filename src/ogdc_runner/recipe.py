from __future__ import annotations

import tempfile
from contextlib import contextmanager
from pathlib import Path

import fsspec
import yaml
from loguru import logger

from ogdc_runner.constants import RECIPE_CONFIG_FILENAME
from ogdc_runner.exceptions import OgdcInvalidRecipeDir
from ogdc_runner.models.recipe_config import RecipeConfig


@contextmanager
def stage_ogdc_recipe(recipe_location: str):  # type: ignore[no-untyped-def]
    """Stages the recipe directory."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        recipe_fs, recipe_fs_path = fsspec.core.url_to_fs(recipe_location)

        recipe_fs.get(recipe_fs_path, temp_path, recursive=True)
        logger.success("staged recipe directory from {recipe_location} to {temp_dir}")

        yield temp_path


def get_recipe_config(recipe_directory: Path) -> RecipeConfig:
    """Extract config from a recipe configuration file (meta.yml)."""
    recipe_path = recipe_directory / RECIPE_CONFIG_FILENAME
    try:
        with recipe_path.open("r") as config_file:
            config_dict = yaml.safe_load(config_file)
    except (FileNotFoundError, OSError) as err:
        raise OgdcInvalidRecipeDir(
            f"Recipe directory not found: {recipe_directory}"
        ) from err

    config = RecipeConfig(**config_dict, recipe_directory=recipe_directory)

    return config
