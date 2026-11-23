from __future__ import annotations

import posixpath
import subprocess
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import fsspec
import yaml
from loguru import logger
from pydantic import ValidationError

from ogdc_runner.constants import RECIPE_CONFIG_FILENAME
from ogdc_runner.exceptions import OgdcInvalidRecipeConfig, OgdcInvalidRecipeDir
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.models.steps import (
    SharedTemplates,
    ShellTemplate,
    StepsRecipeConfig,
    VizTemplate,
)


@contextmanager
def stage_ogdc_recipe(recipe_location: str):  # type: ignore[no-untyped-def]
    """Stages the recipe directory."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        recipe_fs, recipe_fs_path = fsspec.core.url_to_fs(recipe_location)

        recipe_fs.get([recipe_fs_path], str(temp_path), recursive=True)
        logger.success(f"staged recipe directory from {recipe_location} to {temp_dir}")

        yield temp_path


@contextmanager
def clone_recipes_repo(repo_url: str, ref: str = "main") -> Generator[Path]:
    """Clone a git repository at a specific ref.

    Args:
        repo_url: Git repository URL
        ref: Git ref like 'main' or 'develop'
    """
    # Validate repo_url format
    if not repo_url.startswith(("https://", "git://", "git@")):
        raise ValueError(
            f"Invalid repo_url '{repo_url}'. Must start with 'https://', 'git://', or 'git@'."
        )
    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", ref, repo_url, tmpdir],
            check=True,
            capture_output=True,
            text=True,
        )

        yield Path(tmpdir)


def get_recipe_config(
    recipe_directory: Path, *, check_urls: bool = False
) -> RecipeConfig:
    """Extract config from a recipe configuration file (meta.yml).

    Args:
        recipe_directory: Path to the recipe directory containing meta.yml
        check_urls: If True, validate that all URL-type input parameters are accessible
    """
    recipe_path = recipe_directory / RECIPE_CONFIG_FILENAME
    try:
        with recipe_path.open("r") as config_file:
            config_dict = yaml.safe_load(config_file)
    except (FileNotFoundError, OSError) as err:
        raise OgdcInvalidRecipeDir(
            f"Recipe directory not found: {recipe_directory}"
        ) from err

    config = RecipeConfig.model_validate(
        dict(**config_dict, recipe_directory=recipe_directory),
        context={"recipe_directory": recipe_directory, "check_urls": check_urls},
    )

    return config


def load_recipe_config(
    recipe_directory: Path, *, check_urls: bool = False
) -> RecipeConfig | StepsRecipeConfig:
    """Load a legacy or (experimental) steps recipe, chosen by its top-level keys.

    A `recipe` key selects the steps form. Anything else takes the unchanged
    legacy path, so a recipe that mixes `workflow` and `recipe` is rejected by
    the steps schema.
    """
    meta_path = recipe_directory / RECIPE_CONFIG_FILENAME
    try:
        config_dict = yaml.safe_load(meta_path.read_text())
    except OSError as err:
        raise OgdcInvalidRecipeDir(
            f"Recipe directory not found: {recipe_directory}"
        ) from err

    # `recipe` only exists in the steps form.
    if isinstance(config_dict, dict) and "recipe" in config_dict:
        return StepsRecipeConfig.model_validate(
            dict(**config_dict, recipe_directory=recipe_directory),
            context={"check_urls": check_urls},
        )

    # Legacy recipe: unchanged path.
    return get_recipe_config(recipe_directory, check_urls=check_urls)


def _templates_root(recipe_fs_path: str) -> str:
    """Return `<root>/templates` for a recipe path shaped `<root>/recipes/<name>`."""
    parent, name = posixpath.split(recipe_fs_path.rstrip("/"))
    root, recipes = posixpath.split(parent)
    if recipes != "recipes" or not name:
        msg = "shared templates need a recipe path shaped <root>/recipes/<name>"
        raise OgdcInvalidRecipeConfig(msg)
    return posixpath.join(root, "templates")


def load_shared_templates(
    recipe_location: str, refs: list[str]
) -> list[ShellTemplate | VizTemplate]:
    """Read the `templates_from` files that live next to the recipe's `recipes/`.

    Offline validation only: no commit pinning or snapshotting happens here.
    """
    fs, recipe_fs_path = fsspec.core.url_to_fs(recipe_location)
    templates_root = _templates_root(recipe_fs_path)
    loaded: list[ShellTemplate | VizTemplate] = []
    for ref in refs:
        # Resolve the ref against the repo root and keep it inside `templates/`.
        file_path = posixpath.normpath(
            posixpath.join(posixpath.dirname(templates_root), ref)
        )
        if not file_path.startswith(templates_root + "/"):
            raise OgdcInvalidRecipeConfig(f"Template escapes templates/: {ref!r}")
        try:
            doc = yaml.safe_load(fs.cat_file(file_path))
        except FileNotFoundError as err:
            raise OgdcInvalidRecipeConfig(f"Shared template not found: {ref}") from err
        try:
            templates = SharedTemplates.model_validate(doc).templates
        except ValidationError as err:
            raise OgdcInvalidRecipeConfig(
                f"Invalid shared template {ref}: {err}"
            ) from err
        # Each template's script/config file must sit next to its template file.
        for template in templates:
            script = (
                template.sh_file
                if isinstance(template, ShellTemplate)
                else template.config_file
            )
            if script is None:
                continue
            # The schema rejects absolute paths and `..`, so this stays below
            # the template file's directory (inside `templates/`).
            script_path = posixpath.join(posixpath.dirname(file_path), script)
            if not fs.isfile(script_path):
                raise OgdcInvalidRecipeConfig(
                    f"{template.name}: {script} not found next to {ref}"
                )
        loaded.extend(templates)
    return loaded


def check_steps_recipe(config: StepsRecipeConfig, recipe_location: str) -> None:
    """Resolve shared templates and check the full step graph."""
    shared = load_shared_templates(recipe_location, config.templates_from)
    try:
        config.check_graph(shared)
    except ValueError as err:
        raise OgdcInvalidRecipeConfig(str(err)) from err


def find_recipe_dirs(recipes_dir: Path) -> list[Path]:
    """Find all directories containing meta.yml in the recipes directory."""
    recipe_dirs = set()

    # Look for meta files at 1 level under recipes/
    for meta_file in recipes_dir.glob(f"*/{RECIPE_CONFIG_FILENAME}"):
        recipe_dirs.add(meta_file.parent)

    return sorted(recipe_dirs)


def validate_all_recipes_in_repo(
    repo_url: str, ref: str = "main", *, check_urls: bool = False
) -> None:
    """Validate all recipes in a git repository.

    Args:
        repo_url: Git repository URL
        ref: Git reference (branch, tag, or commit)
        check_urls: If True, validate that all URL-type input parameters are accessible
    """

    print(f"Cloning {repo_url}@{ref}...")

    with clone_recipes_repo(repo_url, ref) as repo_dir:
        recipes_dir = repo_dir / "recipes"

        if not recipes_dir.exists():
            print("No 'recipes' directory found in repository")
            sys.exit(1)

        # Find all recipe directories
        recipe_dirs = find_recipe_dirs(recipes_dir)

        if not recipe_dirs:
            msg = "No recipes found in recipes directory"
            raise OgdcInvalidRecipeDir(msg)

        print(f"Found {len(recipe_dirs)} recipes to validate")
        if check_urls:
            print("URL validation enabled")
        print()

        invalid_recipes: list[tuple[Path, str]] = []

        for recipe_dir in recipe_dirs:
            recipe_name = recipe_dir.relative_to(recipes_dir)
            try:
                get_recipe_config(recipe_dir, check_urls=check_urls)
                print(f"✓ {recipe_name}")

            except (ValidationError, OgdcInvalidRecipeConfig) as err:
                print(f"✗ {recipe_name}")
                print(f"  Error: {err}\n")
                invalid_recipes.append((recipe_name, str(err)))

        # output
        print("\nValidation Results:")
        print(f"  Valid: {len(recipe_dirs) - len(invalid_recipes)}")
        print(f"  Invalid: {len(invalid_recipes)}")

        if invalid_recipes:
            print("\nFailed recipes:")
            for name, _ in invalid_recipes:
                print(f"  - {name}")
            sys.exit(1)
