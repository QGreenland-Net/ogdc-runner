from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from kubernetes import client, config, utils

from ogdc_runner.constants import K8S_NAMESPACE
from ogdc_runner.jinja import j2_environment
from ogdc_runner.recipe import get_recipe_config
from ogdc_runner.recipe.simple import render_simple_recipe
from ogdc_runner.models.recipe_config import RecipeConfig


def apply_configmap(*, recipe_config: RecipeConfig, k8s_client: client.ApiClient, configmap_manifest: dict[str, Any]) -> None:
    api = client.CoreV1Api(k8s_client)
    listing = api.list_namespaced_config_map(namespace=K8S_NAMESPACE)
    matching_configmaps = [
        configmap
        for configmap in listing.items
        if recipe_config.id in configmap.metadata.name
    ]
    if len(matching_configmaps) == 1:
        matching_configmap_name = matching_configmaps[0].metadata.name
        print(f"Deleting existing configmap {matching_configmap_name}")
        result = api.delete_namespaced_config_map(
            name=matching_configmap_name, namespace=K8S_NAMESPACE
        )
        if result.status != "Success":
            raise RuntimeError(
                f"Attempt to delete configmap {matching_configmap_name} failed: {result.reason}"
            )
    elif len(matching_configmaps) > 1:
        raise RuntimeError(
            f"Found more than the expected number of configmaps for {recipe_config.id}"
        )
    utils.create_from_yaml(k8s_client, yaml_objects=[configmap_manifest])


def submit_recipe(recipe_path: Path) -> None:
    """Render the recipe and submit to kubernetes."""
    # TODO: Support other recipe API
    driver_script = render_simple_recipe(recipe_path)
    recipe_config = get_recipe_config(recipe_directory=recipe_path)

    # A ConfigMap provides the driver script to the cluster
    configmap_manifest_template = j2_environment.get_template("configmap.yml.j2")
    configmap_manifest_yaml = configmap_manifest_template.render(
        recipe_id=recipe_config.id,
        driver_script=driver_script,
    )
    configmap_manifest = yaml.safe_load(configmap_manifest_yaml)

    # A Job executes the driver script on the cluster
    job_manifest_template = j2_environment.get_template("job.yml.j2")
    job_manifest = yaml.safe_load(
        job_manifest_template.render(recipe_id=recipe_config.id)
    )

    # Load k8s config and create an API client instance.
    config.load_kube_config()
    k8s_client = client.ApiClient()
    apply_configmap(recipe_config=recipe_config, k8s_client=k8s_client, configmap_manifest=configmap_manifest)

    utils.create_from_yaml(k8s_client, yaml_objects=[job_manifest])

    print("Successfully submitted job.")
