"""Tests for loading steps recipes from a recipe directory."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from ogdc_runner import api
from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.exceptions import OgdcInvalidRecipeConfig
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.models.steps import StepsRecipeConfig
from ogdc_runner.recipe import (
    check_steps_recipe,
    load_recipe_config,
    load_shared_templates,
)

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


def _load_steps(recipe_dir: Path) -> StepsRecipeConfig:
    config = load_recipe_config(recipe_dir)
    assert isinstance(config, StepsRecipeConfig)
    return config


def _repo_copy(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    shutil.copytree(REPO, repo)
    return repo


def test_shared_templates_resolve_and_graph_is_valid():
    config = load_recipe_config(RECIPE_DIR)
    assert isinstance(config, StepsRecipeConfig)
    shared = load_shared_templates(str(RECIPE_DIR), config.templates_from)
    assert [t.name for t in shared] == ["compress-overviews"]
    check_steps_recipe(config, str(RECIPE_DIR))


def test_two_recipes_reuse_one_shared_template(tmp_path):
    repo = _repo_copy(tmp_path)
    shutil.copytree(repo / "recipes/sea-ice-conc", repo / "recipes/other")
    for name in ("sea-ice-conc", "other"):
        recipe_dir = repo / "recipes" / name
        config = load_recipe_config(recipe_dir)
        assert isinstance(config, StepsRecipeConfig)
        check_steps_recipe(config, str(recipe_dir))


def test_missing_shared_template_file(tmp_path):
    repo = _repo_copy(tmp_path)
    (repo / "templates/raster/compress-overviews.yml").unlink()
    config = _load_steps(repo / "recipes/sea-ice-conc")
    with pytest.raises(OgdcInvalidRecipeConfig, match="not found"):
        check_steps_recipe(config, str(repo / "recipes/sea-ice-conc"))


def test_shared_template_script_must_exist(tmp_path):
    repo = _repo_copy(tmp_path)
    (repo / "templates/raster/compress-overviews.sh").unlink()
    config = _load_steps(repo / "recipes/sea-ice-conc")
    with pytest.raises(OgdcInvalidRecipeConfig, match=r"compress-overviews\.sh"):
        check_steps_recipe(config, str(repo / "recipes/sea-ice-conc"))


def test_nested_includes_are_rejected(tmp_path):
    repo = _repo_copy(tmp_path)
    path = repo / "templates/raster/compress-overviews.yml"
    path.write_text(path.read_text() + "templates_from: [templates/x.yml]\n")
    config = _load_steps(repo / "recipes/sea-ice-conc")
    with pytest.raises(OgdcInvalidRecipeConfig, match="Invalid shared template"):
        check_steps_recipe(config, str(repo / "recipes/sea-ice-conc"))


def test_shared_templates_need_repo_layout(tmp_path):
    with pytest.raises(OgdcInvalidRecipeConfig, match="<root>/recipes/<name>"):
        load_shared_templates(str(tmp_path), ["templates/a.yml"])


def test_shared_script_cannot_escape_templates_dir(tmp_path):
    repo = _repo_copy(tmp_path)
    path = repo / "templates/raster/compress-overviews.yml"
    path.write_text(
        path.read_text().replace(
            '"compress-overviews.sh"', '"../../recipes/sea-ice-conc/to-percent.sh"'
        )
    )
    with pytest.raises(OgdcInvalidRecipeConfig, match="Invalid shared template"):
        load_shared_templates(
            str(repo / "recipes/sea-ice-conc"),
            ["templates/raster/compress-overviews.yml"],
        )


@pytest.fixture
def no_execution(monkeypatch):
    """Fail the test if a recipe reaches publication checks or a builder."""

    def boom(**_kwargs: Any) -> None:
        msg = "steps recipe reached execution"
        raise AssertionError(msg)

    for name in (
        "data_already_published",
        "make_and_submit_shell_workflow",
        "make_and_submit_viz_workflow",
    ):
        monkeypatch.setattr(api, name, boom)


def test_submit_rejects_steps_recipe(no_execution):  # noqa: ARG001
    with pytest.raises(OgdcInvalidRecipeConfig, match="not executable yet"):
        submit_ogdc_recipe(recipe_dir=RECIPE_DIR, wait=False, overwrite=False)
