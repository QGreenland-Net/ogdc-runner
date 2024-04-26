from __future__ import annotations

from pathlib import Path

import click
import yaml
from kubernetes import client, config, utils

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

    config.load_kube_config()
    k8s_client = client.ApiClient()
    recipe_id = recipe_config["id"]

    # A ConfigMap provides the driver script to the cluster
    # TODO: Move to YAML template for consistency with job manifest template
    configmap_manifest = {
        "kind": "ConfigMap",
        "apiVersion": "v1",
        "metadata": {
            "name": f"ogdc-recipe.{recipe_id}",
            "namespace": "qgnet",
        },
        "data": {"parsl_driver_script.py": driver_script},
    }

    # A Job executes the driver script on the cluster
    job_manifest_template = j2_environment.get_template("job.yml.j2")
    job_manifest = yaml.safe_load(job_manifest_template.render(recipe_id=recipe_id))

    # TODO: There is no `apply_from_yaml`, but we really want to apply (i.e. declarative
    # config instead of imperative; shouldn't have to delete a configmap and create it
    # if it already exists).
    utils.create_from_yaml(k8s_client, yaml_objects=[configmap_manifest])
    utils.create_from_yaml(k8s_client, yaml_objects=[job_manifest])
