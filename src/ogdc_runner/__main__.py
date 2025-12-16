from __future__ import annotations

import datetime as dt
import os
import subprocess
import sys
import time

import click
import requests
from pydantic import ValidationError

from ogdc_runner.exceptions import (
    OgdcMissingEnvvar,
    OgdcServiceApiError,
    OgdcWorkflowExecutionError,
)
from ogdc_runner.recipe import (
    get_recipe_config,
    stage_ogdc_recipe,
    validate_all_recipes_in_repo,
)

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


def get_api_token() -> str:
    """Get an OGDC API token using envvar-provided username/password.

    `OGDC_API_USERNAME` and `OGDC_API_PASSWORD` must be set or an
    `OgdcMissingEnvvar` exception will be raised.

    The resulting access token can be used to authenticate with OGDC API
    endpoitns.
    """
    username = os.environ.get("OGDC_API_USERNAME")
    password = os.environ.get("OGDC_API_PASSWORD")
    if not username or not password:
        err = "OGDC_API_USERNAME and OGDC_API_PASSWORD must be set."
        raise OgdcMissingEnvvar(err)

    response = requests.post(
        f"{OGDC_API_URL}/token",
        data={
            "username": username,
            "password": password,
        },
    )

    response.raise_for_status()

    token_data = response.json()
    access_token = token_data["access_token"]

    if not isinstance(access_token, str):
        err_msg = "Failed to get valid access token from OGDC API."
        raise OgdcServiceApiError(err_msg)

    return access_token


def _check_ogdc_api_error(response: requests.Response) -> None:
    """Raise an `OgdcServiceApiError` if the response is not OK."""
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
    """Get the given workflow's status as a string."""
    response = requests.get(
        url=f"{OGDC_API_URL}/status/{workflow_name}",
    )

    _check_ogdc_api_error(response)

    status = response.json()["status"]

    return str(status)


def _wait_for_workflow_completion(workflow_name: str) -> None:
    """Wait for the given workflow to complete."""
    while True:
        status = _get_workflow_status(workflow_name)
        if status:
            print(
                f"Workflow status for {workflow_name} ({dt.datetime.now():%Y-%m-%d@%H:%M:%S}): {status}"
            )
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
        headers={"Authorization": f"Bearer {get_api_token()}"},
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
    try:
        validate_all_recipes_in_repo(recipes_location, ref)
    except subprocess.CalledProcessError as e:
        print(f"Failed to clone repository: {e}\n{e.stderr}")
        sys.exit(1)
