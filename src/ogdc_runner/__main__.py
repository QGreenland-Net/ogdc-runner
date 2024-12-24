from __future__ import annotations

import os
import tempfile
import subprocess
import time
from pathlib import Path

import click

from ogdc_runner.argo import get_workflow_status, submit_workflow
from ogdc_runner.recipe.simple import make_simple_workflow

# TODO: How do we handle e.g. GitHub URL to recipe?
# handling github by
# checking ig path is a URL
# fetch the file into a temporary directory
# use that as a local path for submission
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


def _submit_workflow(recipe_path: Path) -> str:
    workflow = make_simple_workflow(
        recipe_dir=recipe_path,
    )
    workflow_name = submit_workflow(workflow)
    print(f"Successfully submitted recipe with workflow name {workflow_name}")
    return workflow_name


@cli.command
@recipe_path
def submit(recipe_path: Path) -> None:
    """Submit a recipe to OGDC for execution."""
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
def submit_and_wait(recipe_path: Path) -> None:
    """Submit a recipe to OGDC for execution and wait until completion."""
    workflow_name = _submit_workflow(recipe_path)

    while True:
        status = get_workflow_status(workflow_name)
        if status:
            print(f"Workflow status: {status}")
            # Terminal states
            if status in ("Succeeded", "Failed"):
                break
        time.sleep(5)
