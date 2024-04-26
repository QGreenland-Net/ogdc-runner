from __future__ import annotations

import yaml
from kubernetes import client, config, utils

from ogdc_runner.recipe.simple import render_simple_recipe
from ogdc_runner import get_recipe_config


def submit_recipe(recipe_path: Path) -> None:
    """Render the recipe and submit to kubernetes."""
    # TODO: Support other recipe API
    driver_script = render_simple_recipe(recipe_path)
    recipe_config = get_recipe_config(recipe_directory=recipe_path)

    config.load_kube_config()
    k8s_client = client.ApiClient()
    recipe_id = recipe_config.id

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
