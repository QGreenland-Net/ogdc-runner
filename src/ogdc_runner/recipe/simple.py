from __future__ import annotations

import copy
from typing import Any

import fsspec
from hera.workflows import (
    Container,
    Steps,
    Workflow,
    models,
)
from pydantic import BaseModel

from ogdc_runner.argo import ARGO_WORKFLOW_SERVICE, submit_and_wait
from ogdc_runner.constants import SIMPLE_RECIPE_FILENAME
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.recipe import get_recipe_config


def _make_cmd_template(
    name: str,
    command: str,
    input_volume_mount: models.VolumeMount,
    output_volume_mount: models.VolumeMount,
) -> Container:
    template = Container(
        name=name,
        command=["sh", "-c"],
        args=[command],
        volume_mounts=[input_volume_mount, output_volume_mount],
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


class OGDCWorkflow(BaseModel):
    # TODO: pydantic complains if I type this as taking a Workflow and a
    # VolumeMount...
    argo_workflow: Any
    output_volume_mount: Any


def make_fetch_workflow(recipe_config: RecipeConfig) -> OGDCWorkflow:
    output_volume_mount = models.VolumeMount(
        name="workflow-volume",
        mount_path="/output_dir/",
        sub_path=f"{recipe_config.id}/fetch/",
    )
    with Workflow(
        generate_name=f"{recipe_config.id}-fetch-",
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
        volumes=[
            models.Volume(
                name="workflow-volume",
                persistent_volume_claim={"claim_name": "qgnet-ogdc-workflow-pvc"},
            )
        ],
    ) as w:
        fetch_url = str(recipe_config.input.url)
        fetch_template = Container(
            name=f"{recipe_config.id}-fetch-template",
            command=["sh", "-c"],
            args=[
                f"wget --content-disposition -P /output_dir/ {fetch_url}",
            ],
            volume_mounts=[output_volume_mount],
        )

        with Steps(name="steps"):
            fetch_template()

    return OGDCWorkflow(
        argo_workflow=w,
        output_volume_mount=output_volume_mount,
    )


def make_simple_workflow(
    *, recipe_config: RecipeConfig, input_volume_mount: models.VolumeMount
) -> OGDCWorkflow:
    """Run the workflow and return its name as a str."""
    commands = _cmds_from_simple_recipe(recipe_config.recipe_dir)

    with Workflow(
        generate_name=f"{recipe_config.id}-command-script-",
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
        # TODO: is this worth moving into global config as a default volume made
        # available to all OGDC workflows?
        volumes=[
            models.Volume(
                name="workflow-volume",
                persistent_volume_claim={"claim_name": "qgnet-ogdc-workflow-pvc"},
            )
        ],
    ) as w:
        first_cmd = commands.pop(0)
        previous_cmd_output_vm = models.VolumeMount(
            name="workflow-volume",
            mount_path="/output_dir/",
            # TODO: at this point we do not have a name generated for the
            # workflow, so this will NOT BE UNIQUE. If another workflow is
            # submitted with the same ID or the same workflow is re-submitted
            # after tweaks, then the first step will still be placed here! This
            # is a problem for some steps like the fetch step where we might end
            # up with a bunch of duplicate data.
            # Either we need to generate a unique id, reference the workflow id
            # in a way that argo can inject it at wf runtime, or have something
            # that checks for the existance of the recipe subpath before
            # submitting the workflow. If it exists, we can raise an
            # error/expose an option to overwrite.
            sub_path=f"{recipe_config.id}/command-script-0/",
        )
        first_cmd_template = _make_cmd_template(
            name="run-cmd-0",
            command=first_cmd,
            input_volume_mount=input_volume_mount,
            output_volume_mount=previous_cmd_output_vm,
        )
        cmd_templates = [first_cmd_template]
        for idx, command in enumerate(commands):
            input_volume_mount = copy.deepcopy(previous_cmd_output_vm)
            input_volume_mount.mount_path = "/input_dir"
            previous_cmd_output_vm = models.VolumeMount(
                name="workflow-volume",
                mount_path="/output_dir/",
                sub_path=f"{recipe_config.id}/command-script-{idx}/",
            )
            cmd_template = _make_cmd_template(
                name=f"run-cmd-{idx}",
                command=command,
                input_volume_mount=input_volume_mount,
                output_volume_mount=previous_cmd_output_vm,
            )
            cmd_templates.append(cmd_template)
        with Steps(name="steps"):
            for idx, cmd_template in enumerate(cmd_templates):
                cmd_template(name=f"step-{idx}")

    return OGDCWorkflow(
        argo_workflow=w,
        output_volume_mount=previous_cmd_output_vm,
    )


def submit_simple_workflow(recipe_dir: str):
    recipe_config = get_recipe_config(recipe_dir)

    # Create and submit a workflow to fetch the resource.
    fetch_workflow = make_fetch_workflow(recipe_config)
    submit_and_wait(fetch_workflow.argo_workflow)

    # Next, create and submit a workflow to process the user's "simple" recipe.
    input_volume_mount = copy.deepcopy(fetch_workflow.output_volume_mount)
    input_volume_mount.mount_path = "/input_dir/"
    simple_workflow = make_simple_workflow(
        recipe_config=recipe_config,
        input_volume_mount=input_volume_mount,
    )
    submit_and_wait(simple_workflow.argo_workflow)
