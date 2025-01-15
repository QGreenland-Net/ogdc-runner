from __future__ import annotations

import click

from ogdc_runner.argo import get_workflow_status, submit_workflow
from ogdc_runner.recipe.simple import make_simple_workflow

recipe_path = click.argument(
    "recipe_path",
    required=True,
    metavar="RECIPE-PATH",
    type=str,
)


@click.group
def cli() -> None:
    """A tool for submitting data transformation recipes to OGDC for execution."""


@cli.command
@recipe_path
def submit(recipe_path: str) -> None:
    """
    Submit a recipe to OGDC for execution.

    RECIPE-PATH: Path to the recipe file. Use either a local path (e.g., '/ogdc-recipes/recipes/seal-tags')
    or an fsspec-compatible GitHub string (e.g., 'github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags').
    """
    workflow = make_simple_workflow(
        recipe_dir=recipe_path,
    )
    submit_workflow(workflow)


@cli.command
@click.argument(
    "workflow_name",
    required=True,
    type=str,
)
def check_workflow_status(workflow_name: str) -> None:
    """Render and submit a recipe to OGDC for execution."""
    status = get_workflow_status(workflow_name)
    print(f"Workflow {workflow_name} has status {status}.")


@cli.command
@recipe_path
def submit_and_wait(recipe_path: str) -> None:
    """
    Submit a recipe to OGDC for execution and wait until completion.

    RECIPE-PATH: Path to the recipe file. Use either a local path (e.g., '/ogdc-recipes/recipes/seal-tags')
    or an fsspec-compatible GitHub string (e.g., 'github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags').
    """
    workflow = make_simple_workflow(
        recipe_dir=recipe_path,
    )
    submit_workflow(workflow, wait=True)
