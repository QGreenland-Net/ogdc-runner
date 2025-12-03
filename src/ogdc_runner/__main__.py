from __future__ import annotations

import datetime as dt
import os
import sys
import time

import click
import requests
from pydantic import ValidationError

from ogdc_runner.exceptions import OgdcServiceApiError, OgdcWorkflowExecutionError
from ogdc_runner.recipe import get_recipe_config, stage_ogdc_recipe

# Default the OGDC API URL based on the environment, falling back to the prod
# URL.
env = os.environ.get("ENVIRONMENT")
if env == "local":
    default_url = "http://localhost:8000"
elif env == "dev":
    default_url = "http://api.test.dataone.org/ogdc"
else:
    default_url = "http://api.dataone.org/ogdc"
OGDC_API_URL = os.environ.get("OGDC_API_URL", default_url)


@click.group
def cli() -> None:
    """A tool for submitting data transformation recipes to OGDC for execution."""


def _check_ogdc_api_error(response: requests.Response) -> None:
    if not response.ok:
        try:
            detail = response.json()["detail"]
        except Exception:
            detail = "No error details."
        err_msg = (
            f"API Error with status code {response.status_code}: {response.reason}."
            f"\nAPI Error details: {detail}"
        )
        raise OgdcServiceApiError(err_msg)


def _get_workflow_status(workflow_name: str) -> str:
    response = requests.get(
        url=f"{OGDC_API_URL}/status/{workflow_name}",
    )

    _check_ogdc_api_error(response)

    status = response.json()["status"]

    return str(status)


def _wait_for_workflow_completion(workflow_name: str) -> None:
    while True:
        status = _get_workflow_status(workflow_name)
        if status:
            print(f"Workflow status ({dt.datetime.now():%Y-%m-%dT%H:%m:%S}): {status}")
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

    RECIPE-PATH: Path to the recipe directory. Use an fsspec-compatible string
    representing a remote and publicly accessible recipe directory (e.g., for
    GitHub, 'github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags').
    """
    response = requests.post(
        url=f"{OGDC_API_URL}/submit",
        json={
            "recipe_path": recipe_path,
            "overwrite": overwrite,
        },
    )

    _check_ogdc_api_error(response)
    print(response.json()["message"])

    if wait:
        workflow_name = response.json()["recipe_workflow_name"]
        print("Waiting for completion...")
        _wait_for_workflow_completion(workflow_name)


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
