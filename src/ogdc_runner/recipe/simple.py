from __future__ import annotations

import fsspec
from hera.workflows import (
    Artifact,
    Container,
    Parameter,
    Steps,
    Workflow,
    models,
)

from ogdc_runner.argo import ARGO_WORKFLOW_SERVICE, submit_workflow
from ogdc_runner.constants import SIMPLE_RECIPE_FILENAME
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.recipe import get_recipe_config


def _make_cmd_template(
    name: str,
    command: str,
) -> Container:
    template = Container(
        name=name,
        command=["sh", "-c"],
        args=[
            f"mkdir -p /output_dir/ && {command}",
        ],
        inputs=[Artifact(name="input-dir", path="/input_dir/")],
        outputs=[Artifact(name="output-dir", path="/output_dir/")],
    )

    return template


def _make_fetch_url_template(recipe_config: RecipeConfig) -> Container:
    fetch_url = str(recipe_config.input.url)
    template = Container(
        name=f"{recipe_config.id}-fetch-template",
        command=["sh", "-c"],
        args=[
            f"mkdir -p /output_dir/ && wget --content-disposition -P /output_dir/ {fetch_url}",
        ],
        outputs=[Artifact(name="output-dir", path="/output_dir/")],
    )

    return template


def _make_publish_template(recipe_id: str) -> Container:
    """Creates a container template that will move final output data into the
    OGDC data storage volume under a subpath named for the recipe_id."""
    template = Container(
        name="publish-data",
        command=["sh", "-c"],
        args=[
            "rsync --progress /input_dir/* /output_dir/",
        ],
        inputs=[Artifact(name="input-dir", path="/input_dir/")],
        volume_mounts=[
            models.VolumeMount(
                name="workflow-volume",
                mount_path="/output_dir/",
                sub_path=recipe_id,
            )
        ],
    )

    return template


def _cmds_from_simple_recipe(recipe_dir: str) -> list[str]:
    """Read commands from a 'simple' OGDC recipe.

    'simple' OGDC recipes are `.sh` files containing bash commands. Commands can
    use `/input_dir/` and `/output_dir/` to indicate:
        * `/input_dir/`: input from the previous step's `/output_dir/`
        * `/output_dir/`: output written by each command. It is expected that
          each command in a simple recipe will place data in `/output_dir/`.
    """
    recipe_path = f"{recipe_dir}/{SIMPLE_RECIPE_FILENAME}"
    print(f"Reading recipe from {recipe_path}")

    with fsspec.open(recipe_path, "rt") as f:
        lines = f.read().split("\n")
    commands = [line for line in lines if line and not line.startswith("#")]

    return commands


def data_already_published(recipe_config: RecipeConfig) -> bool:
    """Workflow that checks for the existence of published data for the given
    recipe."""
    with Workflow(
        generate_name=f"{recipe_config.id}-check-published",
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
        volumes=[
            models.Volume(
                name="workflow-volume",
                # TODO: parameterize this!
                persistent_volume_claim={"claim_name": "qgnet-ogdc-workflow-pvc"},
            )
        ],
    ) as w:
        check_dir_template = Container(
            name="check-already-published",
            command=["sh", "-c"],
            # Check for the existence of the recipe-specific subpath. If it
            # exists, writ eout a file with "yes". Otherwise write out a file
            # with "no". This file becomes an argo parameter that we can check
            # later.
            args=[
                f'test -d /mnt/{recipe_config.id} && echo "yes" > /tmp/published.txt || echo "no" > /tmp/published.txt',
            ],
            outputs=[
                Parameter(
                    name="data-published",
                    value_from=models.ValueFrom(path="/tmp/published.txt"),
                ),
            ],
            volume_mounts=[
                models.VolumeMount(
                    name="workflow-volume",
                    mount_path="/mnt/",
                ),
            ],
        )

        with Steps(name="steps"):
            check_dir_template()

    # wait for the workflow to complete.
    workflow_name = submit_workflow(workflow=w, wait=True)

    # Check the result. Get an updated instance of the workflow, with the latest
    # states for all notdes. Then, iterate through the nodes and find the
    # template we define above ("check-already-published") and extract its
    # output parameter.
    completed_workflow = ARGO_WORKFLOW_SERVICE.get_workflow(name=workflow_name)
    result = None
    for node in completed_workflow.status.nodes.values():
        if node.template_name == "check-already-published":
            result = node.outputs.parameters[0].value
    if not result:
        err_msg = "Failed to check if data have been published"
        raise RuntimeError(err_msg)

    assert result in ("yes", "no")

    return result == "yes"


def make_simple_workflow(recipe_dir: str) -> Workflow:
    """Run the workflow and return its name as a str."""
    commands = _cmds_from_simple_recipe(recipe_dir)
    recipe_config = get_recipe_config(recipe_dir)

    # TODO: consider moving this into a higher-level function. This should just
    # make a simple workflow from a simple recipe, and not submit workflows of
    # its own.
    # First, check if the data are already published. If so, we should raise an
    # error or overwrite if requested.
    if data_already_published(recipe_config):
        # TODO: better error handling (raise `OGDCRecipeError` or something similar)
        err_msg = "Data for recipe {recipe_config.id} have already been published."
        raise RuntimeError(err_msg)

    with Workflow(
        generate_name=f"{recipe_config.id}-",
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
        volumes=[
            models.Volume(
                name="workflow-volume",
                # TODO: parameterize this!
                persistent_volume_claim={"claim_name": "qgnet-ogdc-workflow-pvc"},
            )
        ],
    ) as w:
        cmd_templates = []
        for idx, command in enumerate(commands):
            cmd_template = _make_cmd_template(name=f"run-cmd-{idx}", command=command)
            cmd_templates.append(cmd_template)
        fetch_template = _make_fetch_url_template(recipe_config)

        # create publication template
        publish_template = _make_publish_template(recipe_config.id)

        with Steps(name="steps"):
            step = fetch_template()
            for idx, cmd_template in enumerate(cmd_templates):
                step = cmd_template(
                    name=f"step-{idx}",
                    arguments=step.get_artifact("output-dir").with_name("input-dir"),  # type: ignore[union-attr]
                )
            # publish final data
            publish_template(
                name="publish-data",
                arguments=step.get_artifact("output-dir").with_name("input-dir"),  # type: ignore[union-attr]
            )

    return w
