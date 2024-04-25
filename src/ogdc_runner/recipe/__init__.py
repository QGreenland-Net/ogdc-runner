from __future__ import annotations
from typing import Any, cast
from pathlib import Path

import yaml

from ogdc_runner.constants import RECIPE_CONFIG_FILENAME


def get_recipe_config(recipe_directory: Path) -> dict[str, Any]:
    """Extract config from a recipe configuration file (meta.yml)."""
    with open(recipe_directory / RECIPE_CONFIG_FILENAME) as config_file:
        config = yaml.safe_load(config_file)

    return cast(dict[str, Any], config)
