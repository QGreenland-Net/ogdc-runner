from __future__ import annotations

import os
import tempfile
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse

import click

from ogdc_runner.argo import get_workflow_status, submit_workflow
from ogdc_runner.recipe.simple import make_simple_workflow


# TODO: How do we handle e.g. GitHub URL to recipe?
# handling github by
# checking if path is a URL
def is_url(recipe_path):
    """ Check if the path is a valid URL."""
    parsed = urlparse(recipe_path)
    return parsed.scheme in ('http', 'https') and parsed.netloc != ''

def convert_url_to_ssh(recipe_path):
    """ Convert https github url to ssh format."""
    parsed = urlparse(recipe_path)
    if parsed.scheme != "https" or "github.com" not in parsed.netloc:
        raise ValueError("Invalid GitHub HTTPS URL")
    # extract just the repo path
    repo_path = parsed.path.lstrip("/")  
    return f"git@github.com:{repo_path}.git"

def clone_repo(ssh_url):
    """Clone the repository into a temporary directory."""
    temp_dir = tempfile.mkdtemp()
    try:
        subprocess.run(['git', 'clone', ssh_url, temp_dir], check=True)
        return temp_dir
    except subprocess.CalledProcessError as e:
        print(f"Error cloning repository: {e}")

# use that as a local path for submission
# need to figure out how to best do this for if its a url - this is a temporary solution
recipe_path = click.argument(
    "recipe_path",
    required=True,
    # metavar="PATH",
    # type=click.Path(
    #     exists=True,
    #     file_okay=False,
    #     dir_okay=True,
    #     readable=True,
    #     resolve_path=True,
    #     path_type=Path,
    # ),
)
recipe_name = click.argument(
    "recipe_name",
    required=False,
    type = str)


@click.group
def cli() -> None:
    """A tool for submitting data transformation recipes to OGDC for execution."""


def _submit_workflow(recipe_path) -> str:
    if is_url(recipe_path):
        print(f"Detected URL: {recipe_path}. Cloning repository...")
        ssh_url = convert_url_to_ssh(recipe_path)
        clone_repo(ssh_url)
    else: 
        workflow = make_simple_workflow(
            recipe_dir=recipe_path,
        )
        workflow_name = submit_workflow(workflow)
        print(f"Successfully submitted recipe with workflow name {workflow_name}")
        return workflow_name


@cli.command
@recipe_path
@recipe_name
def submit(recipe_path, recipe_name=None) -> None:
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
