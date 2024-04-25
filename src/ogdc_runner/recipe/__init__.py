from __future__ import annotations

from pathlib import Path

import yaml

from ogdc_runner.constants import RECIPE_CONFIG_FILENAME


def get_recipe_config(recipe_directory: Path) -> dict:
    """Extract config from a recipe configuration file (meta.yml)."""
    with open(recipe_directory / RECIPE_CONFIG_FILENAME) as config_file:
        return yaml.safe_load(config_file)
