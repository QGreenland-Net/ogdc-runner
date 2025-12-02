from __future__ import annotations

import sys
import time

import click
import requests
from loguru import logger
from pydantic import ValidationError

from ogdc_runner.exceptions import OgdcWorkflowExecutionError
from ogdc_runner.recipe import get_recipe_config, stage_ogdc_recipe

# TODO: make this configurable/default to prod URL
OGDC_API_URL = "http://localhost:8000"


@click.group
def cli() -> None:
    """A tool for submitting data transformation recipes to OGDC for execution."""


def _get_workflow_status(workflow_name: str) -> str:
    response = requests.get(
        url=f"{OGDC_API_URL}/status/{workflow_name}",
    )
    response.raise_for_status()

    status = response.json()["status"]

    return str(status)


def _wait_for_workflow_completion(workflow_name: str) -> None:
    while True:
        status = _get_workflow_status(workflow_name)
        if status:
            logger.info(f"Workflow status: {status}")
            # Terminal states
            if status == "Failed":
                raise OgdcWorkflowExecutionError(
                    f"Workflow with name {workflow_name} failed."
                )
            if status == "Succeeded":
                return
        time.sleep(5)


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
    response = requests.post(
        url=f"{OGDC_API_URL}/submit",
        json={
            "recipe_path": recipe_path,
            "overwrite": overwrite,
        },
    )

    response.raise_for_status()

    if wait:
        workflow_name = response.json()["recipe_workflow_name"]
        logger.info(
            f"Workflow with name {workflow_name} submitted. Waiting for completion..."
        )
        _wait_for_workflow_completion(workflow_name)

    print(response.json()["message"])


@cli.command
@click.argument(
    "workflow_name",
    required=True,
    type=str,
)
def check_workflow_status(workflow_name: str) -> None:
    """Check an argo workflow's status."""
    status = _get_workflow_status(workflow_name)
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
