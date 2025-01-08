from __future__ import annotations

import time

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


def _submit_workflow(recipe_path: str) -> str:
    workflow = make_simple_workflow(
        recipe_dir=recipe_path,
    )
    workflow_name = submit_workflow(workflow)
    print(f"Successfully submitted recipe with workflow name {workflow_name}")
    return workflow_name


@cli.command
@recipe_path
def submit(recipe_path: str) -> None:
    """
    Submit a recipe to OGDC for execution.

    RECIPE-PATH: Path to the recipe file. Use either a local path (e.g., '/ogdc-recipes/recipes/seal-tags')
    or an fsspec-compatible GitHub string (e.g., 'github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags').
    """
    _submit_workflow(recipe_path)


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
    workflow_name = _submit_workflow(recipe_path)

    while True:
        status = get_workflow_status(workflow_name)
        if status:
            print(f"Workflow status: {status}")
            # Terminal states
            if status in ("Succeeded", "Failed"):
                break
        time.sleep(5)
