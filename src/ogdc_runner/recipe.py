from __future__ import annotations

import subprocess
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import fsspec
import requests
import yaml
from loguru import logger
from pydantic import ValidationError

from ogdc_runner.constants import RECIPE_CONFIG_FILENAME
from ogdc_runner.exceptions import OgdcInvalidRecipeConfig, OgdcInvalidRecipeDir
from ogdc_runner.models.recipe_config import RecipeConfig


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

    config = RecipeConfig.model_validate(
        dict(**config_dict, recipe_directory=recipe_directory),
        context={"recipe_directory": recipe_directory},
    )

    return config


def find_recipe_dirs(recipes_dir: Path) -> list[Path]:
    """Find all directories containing meta.yml in the recipes directory."""
    recipe_dirs = set()

    # Look for meta files at 1 level under recipes/
    for meta_file in recipes_dir.glob(f"*/{RECIPE_CONFIG_FILENAME}"):
        recipe_dirs.add(meta_file.parent)

    return sorted(recipe_dirs)


def validate_input_urls(
    config: RecipeConfig, timeout: int = 30
) -> list[tuple[str, str]]:
    """Validate that all URL-type input parameters are accessible.

    Uses HEAD requests to check URL accessibility without downloading content.

    Args:
        config: The recipe configuration to validate
        timeout: Request timeout in seconds

    Returns:
        List of (url, error_message) tuples for any URLs that failed validation.
        Empty list means all URLs are accessible.
    """
    errors: list[tuple[str, str]] = []

    for param in config.input.params:
        if param.type != "url":
            continue

        url = str(param.value)
        try:
            response = requests.head(url, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            errors.append((url, f"HTTP {e.response.status_code}"))
        except requests.exceptions.ConnectionError:
            errors.append((url, "Connection failed"))
        except requests.exceptions.Timeout:
            errors.append((url, f"Timeout after {timeout}s"))
        except requests.exceptions.RequestException as e:
            errors.append((url, str(e)))

    return errors


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
                config = get_recipe_config(recipe_dir)

                if check_urls:
                    url_errors = validate_input_urls(config)
                    if url_errors:
                        error_details = "; ".join(
                            f"{url}: {error}" for url, error in url_errors
                        )
                        raise OgdcInvalidRecipeConfig(
                            f"URL validation failed: {error_details}"
                        )

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
