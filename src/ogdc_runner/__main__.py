from __future__ import annotations

import time
from urllib.parse import urlparse

import click

from ogdc_runner.argo import get_workflow_status, submit_workflow
from ogdc_runner.recipe.simple import make_simple_workflow


# checking if path is a URL
def is_url(recipe_path: str) -> bool:
    """Check if the path is a valid URL."""
    parsed = urlparse(recipe_path)
    return parsed.scheme in ("http", "https") and parsed.netloc != ""


def format_url(recipe_path: str) -> str:
    if "https://github.com/" not in recipe_path:
        return "Invalid GitHub URL"
    # remove first part of URL
    base_url = recipe_path.replace("https://github.com/", "")

    components = base_url.split("/")
    owner = f"{components[0]}:{components[1]}".lower()
    branch = components[3]
    file_path = "/".join(components[4:])

    fsspec_format = f"github://{owner}@{branch}/{file_path}"
    return fsspec_format


# use that as a local path for submission
# need to figure out how to best do this for if its a url - this is a temporary solution
# will likely change this to string
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


@click.group
def cli() -> None:
    """A tool for submitting data transformation recipes to OGDC for execution."""


def _submit_workflow(recipe_path: str) -> str:
    if is_url(recipe_path):
        recipe_path = format_url(recipe_path)
    workflow = make_simple_workflow(
        recipe_dir=recipe_path,
    )
    workflow_name = submit_workflow(workflow)
    print(f"Successfully submitted recipe with workflow name {workflow_name}")
    return workflow_name


@cli.command
@recipe_path
def submit(recipe_path: str) -> None:
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
def submit_and_wait(recipe_path: str) -> None:
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
