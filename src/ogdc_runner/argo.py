from __future__ import annotations

from pathlib import Path

from hera.shared import global_config
from hera.workflows import (
    Artifact,
    Container,
    Steps,
    Workflow,
    WorkflowsService,
    models,
)

from ogdc_runner.recipe import get_recipe_config

# Argo-related constants.
# TODO: move these to `constants.py`? And/or allow override via envvars or some
# other config.
ARGO_NAMESPACE = "argo-helm"
ARGO_SERVICE_ACCOUNT_NAME = "argo-workflow"
ARGO_WORKFLOW_SERVICE_URL = "http://localhost:2746"

# https://hera.readthedocs.io/en/stable/examples/workflows/misc/global_config/
global_config.namespace = ARGO_NAMESPACE
global_config.service_account_name = ARGO_SERVICE_ACCOUNT_NAME

# TODO: this is dev-specific config.
global_config.set_class_defaults(
    Container,
    image_pull_policy="Never",
)
global_config.image = "ogdc-gdal-runner"

WORKFLOW_SERVICE = WorkflowsService(host=ARGO_WORKFLOW_SERVICE_URL)


def make_cmd_template(
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


def make_fetch_url_template(recipe_dir: Path):
    recipe_config = get_recipe_config(recipe_dir)
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


def cmds_from_simple_recipe(recipe_dir: Path) -> list[str]:
    """Read commands from a 'simple' OGDC recipe.

    'simple' OGDC recipes are `.sh` files containing bash commands. Commands can
    use `/input_dir/` and `/output_dir/` to indicate:
        * `/input_dir/`: input from the previous step's `/output_dir/`
        * `/output_dir/`: output written by each command. It is expected that
          each command in a simple recipe will place data in `/output_dir/`.
    """
    recipe_path = recipe_dir / "recipe.sh"
    # read the commands from the recipe
    lines = recipe_path.read_text().split("\n")
    # Filter out comments. We assume all other lines are bash commands.
    commands = [line for line in lines if line and not line.startswith("#")]

    return commands


def run_simple_workflow(workflow_name: str, recipe_dir: Path) -> str:
    """Run the workflow and return its name as a str."""
    commands = cmds_from_simple_recipe(recipe_dir)

    with Workflow(
        generate_name=f"{workflow_name}-",
        entrypoint="steps",
        workflows_service=WORKFLOW_SERVICE,
        volumes=[
            models.Volume(
                name="workflow-pvc",
                persistent_volume_claim={"claim_name": "workflow-pvc"},
            )
        ],
    ) as w:
        cmd_templates = []
        for idx, command in enumerate(commands):
            cmd_template = make_cmd_template(name=f"run-cmd-{idx}", command=command)
            cmd_templates.append(cmd_template)
        fetch_template = make_fetch_url_template(recipe_dir)
        with Steps(name="steps"):
            step = fetch_template()
            for idx, cmd_template in enumerate(cmd_templates):
                step = cmd_template(
                    name=f"step-{idx}",
                    arguments=step.get_artifact("output-dir").with_name("input-dir"),
                )

    w.create()

    return w.name


def get_workflow_status(workflow_name: str) -> str:
    """Return the given workflow's status (e.g., `'Succeeded'`)"""
    workflow = WORKFLOW_SERVICE.get_workflow(name=workflow_name)

    return workflow.status.phase
