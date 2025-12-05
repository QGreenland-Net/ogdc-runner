from __future__ import annotations

import subprocess
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import click
from pydantic import ValidationError

from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_workflow_status
from ogdc_runner.constants import RECIPE_CONFIG_FILENAME
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
    required=False,
    default="https://github.com/qgreenland-net/ogdc-recipes.git",
    metavar="RECIPES-LOCATION",
    type=str,
)
@click.option(
    "--ref",
    default="main",
    help="Git reference branch or tag to validate",
    type=str,
)
def validate_all_recipes(recipes_location: str, ref: str) -> None:
    """Validate all OGDC recipes in a git repository.

    RECIPES-LOCATION: Git repository URL (default: https://github.com/qgreenland-net/ogdc-recipes.git)

    Examples:
      ogdc-runner validate-all-recipes
      ogdc-runner validate-all-recipes --ref develop
      ogdc-runner validate-all-recipes https://github.com/myorg/ogdc-recipes.git --ref feature-branch
    """
    print(f"Cloning {recipes_location}@{ref}...")

    try:
        with clone_recipes_repo(recipes_location, ref) as repo_dir:
            recipes_dir = repo_dir / "recipes"

            if not recipes_dir.exists():
                print("No 'recipes' directory found in repository")
                sys.exit(1)

            # Find all recipe directories
            recipe_dirs = _find_recipe_dirs(recipes_dir)

            if not recipe_dirs:
                print("No recipes found in recipes directory")
                sys.exit(1)

            print(f"Found {len(recipe_dirs)} recipes to validate\n")

            invalid_recipes = []

            for recipe_dir in recipe_dirs:
                recipe_name = recipe_dir.relative_to(recipes_dir)
                try:
                    get_recipe_config(recipe_dir)
                    print(f"✓ {recipe_name}")
                except ValidationError as err:
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
    except subprocess.CalledProcessError as e:
        print(f"Failed to clone repository: {e}\n{e.stderr}")
        sys.exit(1)


@contextmanager
def clone_recipes_repo(repo_url: str, ref: str = "main") -> Generator[Path, None, None]:
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


def _find_recipe_dirs(recipes_dir: Path) -> list[Path]:
    """Find all directories containing meta.yml in the recipes directory."""
    recipe_dirs = set()

    # Look for meta files at 1 level under recipes/
    for meta_file in recipes_dir.glob(f"*/{RECIPE_CONFIG_FILENAME}"):
        recipe_dirs.add(meta_file.parent)

    return sorted(recipe_dirs)
