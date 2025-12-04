from __future__ import annotations

import sys
from pathlib import Path

import click
from pydantic import ValidationError

from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_workflow_status
from ogdc_runner.recipe import get_recipe_config, stage_ogdc_recipe


@click.group
def cli() -> None:
    """A tool for submitting data transformation recipes to OGDC for execution."""


@cli.command
@click.argument(
    "recipe_path",
    required=True,
    metavar="RECIPE-PATH",
    type=str,
)
@click.option(
    "--wait",
    is_flag=True,
    default=False,
    help="Wait for recipe execution to complete.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing outputs of the given recipe if it has already run before.",
)
def submit(recipe_path: str, wait: bool, overwrite: bool) -> None:
    """
    Submit a recipe to OGDC for execution.

    RECIPE-PATH: Path to the recipe file. Use either a local path (e.g., '/ogdc-recipes/recipes/seal-tags')
    or an fsspec-compatible GitHub string (e.g., 'github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags').
    """
    with stage_ogdc_recipe(recipe_path) as recipe_dir:
        submit_ogdc_recipe(
            recipe_dir=recipe_dir,
            wait=wait,
            overwrite=overwrite,
        )


@cli.command
@click.argument(
    "workflow_name",
    required=True,
    type=str,
)
def check_workflow_status(workflow_name: str) -> None:
    """Check an argo workflow's status."""
    status = get_workflow_status(workflow_name)
    print(f"Workflow {workflow_name} has status {status}.")


@cli.command
@click.argument(
    "recipe_path",
    required=True,
    metavar="RECIPE-PATH",
    type=str,
)
def validate_recipe(recipe_path: str) -> None:
    """Validate an OGDC recipe directory."""
    with stage_ogdc_recipe(recipe_path) as recipe_dir:
        try:
            get_recipe_config(recipe_dir)
            print(f"Recipe {recipe_path} is valid.")
        except ValidationError as err:
            print(f"Recipe {recipe_path} is invalid.")
            print(err)
            sys.exit(1)


@cli.command
@click.argument(
    "recipes_location",
    required=True,
    default="github://qgreenland-net:ogdc-recipes@main/recipes",
    metavar="RECIPES-LOCATION",
    type=str,
)
def validate_all_recipes(recipes_location: str) -> None:
    """Validate all OGDC recipes in a directory or git repository.

    # NOTE: may remove local due to conversations yesterday (12/03/2025)
    RECIPES-LOCATION can be:
    - A local directory path containing recipe subdirectories
    - A github:// URL like github://qgreenland-net:ogdc-recipes@main/recipes

    Looks for all directories containing meta.yml or .meta.yml files
    that are exactly 2 levels deep from the base path.
    """
    with stage_ogdc_recipe(recipes_location) as base_dir:
        # find recipes
        recipe_dirs = _find_recipe_dirs(base_dir)

        if not recipe_dirs:
            print(f"No recipes found in {recipes_location}")
            sys.exit(1)

        print(f"Found {len(recipe_dirs)} recipes to validate\n")

        invalid_recipes = []

        for recipe_dir in recipe_dirs:
            recipe_name = recipe_dir.relative_to(base_dir)
            try:
                get_recipe_config(recipe_dir)
                print(f"✓ {recipe_name}")
            except ValidationError as err:
                print(f"✗ {recipe_name}")
                print(f"  Error: {err}\n")
                invalid_recipes.append((recipe_name, str(err)))

        print("\nValidation Results:")
        print(f"  Valid: {len(recipe_dirs) - len(invalid_recipes)}")
        print(f"  Invalid: {len(invalid_recipes)}")

        if invalid_recipes:
            print("\nFailed recipes:")
            for name, _ in invalid_recipes:
                print(f"  - {name}")
            sys.exit(1)


def _find_recipe_dirs(base_path: Path) -> list[Path]:
    """Find all directories containing meta.yml or .meta.yml exactly 2 levels deep."""
    recipe_dirs = []

    # Look for directories exactly 2 levels deep with meta.yml or .meta.yml
    for child in base_path.iterdir():
        if not child.is_dir():
            continue
        for grandchild in child.iterdir():
            if not grandchild.is_dir():
                continue
            # Check if this directory has meta.yml or .meta.yml
            if (grandchild / "meta.yml").exists() or (
                grandchild / ".meta.yml"
            ).exists():
                recipe_dirs.append(grandchild)

    return sorted(recipe_dirs)
