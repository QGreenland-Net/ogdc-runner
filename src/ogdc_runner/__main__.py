from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import click
import requests
import urllib3
from dataone.auth import is_token_valid, parse_tokens_dict, refresh_tokens
from pydantic import ValidationError

from ogdc_runner.exceptions import (
    OgdcServiceApiError,
    OgdcWorkflowExecutionError,
)
from ogdc_runner.recipe import (
    get_recipe_config,
    stage_ogdc_recipe,
    validate_all_recipes_in_repo,
)


class Config:
    def __init__(self) -> None:
        # Default the OGDC API URL based on the environment, falling back to the prod
        # URL.
        self.env = os.environ.get("ENVIRONMENT")
        self.verify_ssl = True
        
        if self.env == "local":
            self.default_url = "https://localhost:7443/api"
            self.verify_ssl = False
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.access_mode = "open"
        elif self.env == "dev":
            self.default_url = "https://ogdc.test.dataone.org/api"
            self.access_mode = "authenticated"
        else:
            self.default_url = "https://ogdc.dataone.org/api"
            self.access_mode = "authenticated"

        self.api_url = os.environ.get("OGDC_API_URL", self.default_url)
        
        self.session = requests.Session()
        self.session.verify = self.verify_ssl
        env_path = os.environ.get("TOKEN_CACHE_FILE")
        self.token_cache_file = Path(env_path) if env_path else Path.home() / ".config/ogdc/tokens.json"


@click.group()
@click.pass_context
def cli(ctx: click.Context) -> None:
    """A tool for submitting data transformation recipes to OGDC for execution."""
    ctx.obj = Config()


def _get_api_token(config: Config) -> str:
    if not config.token_cache_file.exists():
        msg = "Run 'ogdc-runner set-token' first."
        raise RuntimeError(msg)

    with config.token_cache_file.open("r") as f:
        data = json.load(f)

    if is_token_valid(data.get("access_token")):
        return data["access_token"]

    if is_token_valid(data.get("refresh_token")):
        try:
            new_tokens = parse_tokens_dict(refresh_tokens(
                refresh_url=f"{config.api_url}/refresh", 
                refresh_token=data["refresh_token"]
            ))
            
            with config.token_cache_file.open("w") as f:
                json.dump(new_tokens, f, indent=2)
                
            return new_tokens["access_token"]
            
        except Exception as e:
            msg = f"Session refresh failed: {e}"
            raise RuntimeError(msg) from e

    msg = "Session expired. Please log back in and set new tokens."
    raise RuntimeError(msg)


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


def _get_workflow_status(config: Config, workflow_name: str) -> str:
    """Get the given workflow's status as a string."""
    headers = {}
    if config.access_mode != "open":
        headers["Authorization"] = f"Bearer {_get_api_token(config)}"

    response = config.session.get(
        url=f"{config.api_url}/status/{workflow_name}",
        headers=headers,
    )

    _check_ogdc_api_error(response) # And here!

    status = response.json()["status"]

    return str(status)


def _wait_for_workflow_completion(config: Config, workflow_name: str) -> None:
    """Wait for the given workflow to complete."""
    while True:
        status = _get_workflow_status(config, workflow_name)
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


@cli.command()
@click.option("--access", help="The OIDC Access Token string")
@click.option("--refresh", help="The OIDC Refresh Token string")
@click.option("--json-str", help="A raw JSON token string containing both keys")
@click.pass_context
def set_token(ctx: click.Context, access, refresh, json_str):
    """Save OIDC tokens to the local user configuration folder."""
    # initialize
    config = ctx.obj

    new_access = access
    new_refresh = refresh

    if json_str:
        parsed = parse_tokens_dict(json_str)
        # pick CLI over parsed if both are provided
        new_access = new_access or parsed.get("access_token")
        new_refresh = new_refresh or parsed.get("refresh_token")
    
    # load existing state
    existing_data = {}
    if config.token_cache_file.exists():
        try:
            with config.token_cache_file.open("r") as f:
                existing_data = json.load(f)
        except json.JSONDecodeError:
            # if the file is mangled, ignore it. overwrite below
            pass
    
    # merge states. prefer new tokens. keep old if they exist and are not expired
    final_access = new_access
    if (
        not final_access 
        and "access_token" in existing_data 
        and is_token_valid(existing_data["access_token"])
    ):
        final_access = existing_data["access_token"]
    
    final_refresh = new_refresh
    if (
        not final_refresh 
        and "refresh_token" in existing_data 
        and is_token_valid(existing_data["refresh_token"])
    ):
        final_refresh = existing_data["refresh_token"]

    # make sure we wind up with something to save
    if not final_access and not final_refresh:
        msg = (
            "At least one of 'access', 'refresh', or "
            "'json-str' must be provided to set a token."
        )
        raise click.UsageError(msg)

    config.token_cache_file.parent.mkdir(parents=True, exist_ok=True)
    
    data = {}
    if final_access:
        data["access_token"] = final_access
    if final_refresh:
        data["refresh_token"] = final_refresh

    with config.token_cache_file.open("w") as f:
        json.dump(data, f, indent=2)
        
    click.echo("OGDC tokens updated successfully.")

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
@click.pass_context
def submit(ctx: click.Context, recipe_path: str, wait: bool, overwrite: bool) -> None:
    """
    Submit a recipe to OGDC for execution.

    RECIPE-PATH: Path to the recipe directory. Use an fsspec-compatible string
    representing a remote and publicly accessible recipe directory (e.g., for
    GitHub, 'github://qgreenland-net:ogdc-recipes@main/recipes/seal-tags').
    """
    config = ctx.obj

    headers = {}
    if config.access_mode != "open":
        headers["Authorization"] = f"Bearer {_get_api_token(config)}"

    response = config.session.post(
        url=f"{config.api_url}/submit",
        json={
            "recipe_path": recipe_path,
            "overwrite": overwrite,
        },
        headers=headers
    )

    _check_ogdc_api_error(response)
    print(response.json()["message"])

    if wait:
        workflow_name = response.json()["recipe_workflow_name"]
        print("Waiting for completion...")
        _wait_for_workflow_completion(config, workflow_name)


@cli.command
@click.argument(
    "workflow_name",
    required=True,
    type=str,
)
@click.pass_context
def check_workflow_status(ctx: click.Context, workflow_name: str) -> None:
    """Check an argo workflow's status."""
    config = ctx.obj
    status = _get_workflow_status(config, workflow_name)
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


def _download_output_for_workflow(config: Config, workflow_name: str, output_dir: Path) -> None:

    headers = {}
    if config.access_mode != "open":
        headers["Authorization"] = f"Bearer {_get_api_token(config)}"

    response = config.session.get(
        url=f"{config.api_url}/output/{workflow_name}",
        headers=headers,
    )

    _check_ogdc_api_error(response)
    data_url = response.json()["data_url"]

    data_filename = Path(urlparse(data_url).path).name
    assert data_filename.endswith(".zip")

    # Download the data for the user to the given directory.
    output_filepath = output_dir / data_filename
    with config.session.get(data_url, stream=True) as response:
        response.raise_for_status()
        with output_filepath.open("wb") as f:
            for chunk in response.iter_content():
                f.write(chunk)

    print(f"Wrote {output_filepath}")


@cli.command
@click.argument(
    "workflow_name",
    required=True,
    type=str,
)
@click.option(
    "--output-dir",
    default=Path("./"),
    help="Output directory to place the workflow output.",
    type=click.Path(
        writable=True, file_okay=False, dir_okay=True, resolve_path=True, path_type=Path
    ),
)
@click.pass_context
def get_output(ctx: click.Context, workflow_name: str, output_dir: Path) -> None:
    """Get the temporary output for the given workflow."""
    config = ctx.obj
    _download_output_for_workflow(config, workflow_name, output_dir)
