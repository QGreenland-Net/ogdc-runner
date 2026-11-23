"""Tests for loading steps recipes from a recipe directory."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.models.steps import StepsRecipeConfig
from ogdc_runner.recipe import load_recipe_config

REPO = Path(__file__).parents[1] / "test_steps_recipe_repo"
RECIPE_DIR = REPO / "recipes/sea-ice-conc"


def _copy_recipe(tmp_path: Path, **meta_over: Any) -> Path:
    meta = yaml.safe_load((RECIPE_DIR / "meta.yml").read_text())
    meta.update(meta_over)
    for name in ("to-percent.sh", "reproject-3413.sh"):
        (tmp_path / name).write_text((RECIPE_DIR / name).read_text())
    (tmp_path / "meta.yml").write_text(yaml.safe_dump(meta))
    return tmp_path


def test_steps_recipe_dispatch():
    config = load_recipe_config(RECIPE_DIR)
    assert isinstance(config, StepsRecipeConfig)
    assert config.id == "september-2023-sea-ice-concentration-average"
    assert config.recipe_directory == RECIPE_DIR


def test_legacy_recipe_dispatch(test_shell_workflow_recipe_directory):
    config = load_recipe_config(test_shell_workflow_recipe_directory)
    assert isinstance(config, RecipeConfig)


def test_legacy_workflow_and_steps_cannot_be_mixed(tmp_path):
    recipe_dir = _copy_recipe(tmp_path, workflow={"type": "shell"})
    with pytest.raises(ValidationError, match="workflow"):
        load_recipe_config(recipe_dir)


def test_local_template_script_must_exist(tmp_path):
    recipe_dir = _copy_recipe(tmp_path)
    (recipe_dir / "to-percent.sh").unlink()
    with pytest.raises(ValidationError, match=r"to-percent\.sh"):
        load_recipe_config(recipe_dir)


def test_graph_is_checked_when_all_templates_are_local(tmp_path):
    steps = [
        {"name": "a", "template": "to-percent", "input": {"from": "b.output"}},
    ]
    recipe_dir = _copy_recipe(
        tmp_path,
        recipe={"type": "steps", "steps": steps},
        templates_from=[],
    )
    with pytest.raises(ValidationError, match="earlier-step"):
        load_recipe_config(recipe_dir)
