from __future__ import annotations

import click

from ogdc_runner.argo import get_workflow_status
from ogdc_runner.recipe.simple import submit_ogdc_recipe


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
def submit(recipe_path: str, wait: bool) -> None:
    """
    Submit a recipe to OGDC for execution.

    RECIPE-PATH: Path to the recipe file. Use either a local path (e.g., '/ogdc-recipes/recipes/seal-tags')
    or an fsspec-compatible GitHub string (e.g., 'github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags').
    """
    submit_ogdc_recipe(recipe_dir=recipe_path, wait=wait)


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
