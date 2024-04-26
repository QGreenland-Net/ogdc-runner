from __future__ import annotations

from pathlib import Path

import click
import yaml
from kubernetes import config, dynamic
from kubernetes.client import api_client

from ogdc_runner.constants import RECIPE_CONFIG_FILENAME
from ogdc_runner.jinja import j2_environment
from ogdc_runner.recipe.simple import render_simple_recipe

# TODO: How do we handle e.g. GitHub URL to recipe?
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


# TODO: config_cluster() subcommand?


@cli.command
@recipe_path
def render(recipe_path: Path) -> None:
    """Render a recipe, but don't submit it.

    Useful for testing.
    """
    print(render_simple_recipe(recipe_path))  # noqa: T201


@cli.command
@recipe_path
def submit(recipe_path: Path) -> None:
    """Render and submit a recipe to OGDC for execution."""
    driver_script = render_simple_recipe(recipe_path)

    with (recipe_path / RECIPE_CONFIG_FILENAME).open() as config_file:
        recipe_config = yaml.safe_load(config_file)

    k8s_client = dynamic.DynamicClient(
        api_client.ApiClient(configuration=config.load_kube_config())
    )
    recipe_id = recipe_config["id"]

    # A ConfigMap provides the driver script to the cluster
    configmap_manifest = {
        "kind": "ConfigMap",
        "apiVersion": "v1",
        "metadata": {"name": f"ogdc-recipe.{recipe_id}"},
        "data": {"parsl_driver_script.py": driver_script},
    }

    # A Job executes the driver script on the cluster
    job_manifest_template = j2_environment.get_template("job.yml.j2")
    job_manifest = yaml.safe_load(job_manifest_template.render(recipe_id=recipe_id))

    # TODO: This feels like an unnecessary step...
    configmap_api = k8s_client.resources.get(
        api_version=configmap_manifest["apiVersion"],
        kind=configmap_manifest["kind"],
    )
    job_api = k8s_client.resources.get(
        api_version=job_manifest["apiVersion"],
        kind=job_manifest["kind"],
    )

    # TODO: Do we need to specify the namespace? Does it use the namespace from the
    # active context if not?
    configmap_api.apply(configmap_manifest, namespace="qgnet")
    job_api.apply(job_manifest, namespace="qgnet")
