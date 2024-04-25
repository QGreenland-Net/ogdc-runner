from __future__ import annotations

from pathlib import Path

import click

from ogdc_runner.recipe.simple import render_simple_recipe

# TODO: How do we handle e.g. GitHub URL to recipe?
recipe_path = click.argument(
    "recipe_path",
    required=True,
    metavar="PATH",
    type=click.Path(
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
)


@click.group
def cli() -> None:
    """A tool for submitting data transformation recipes to OGDC for execution."""
    pass


@cli.command
@recipe_path
def render(recipe_path: Path) -> None:
    """Render a recipe, but don't submit it.

    Useful for testing.
    """
    render_simple_recipe(recipe_path)


@cli.command
@recipe_path
def submit(recipe_path: Path) -> None:
    """Render and submit a recipe to OGDC for execution."""
    raise NotImplementedError("Not yet!")
