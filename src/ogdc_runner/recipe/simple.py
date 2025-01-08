from __future__ import annotations

import fsspec
from hera.workflows import (
    Artifact,
    Container,
    Steps,
    Workflow,
)

from ogdc_runner.argo import ARGO_WORKFLOW_SERVICE
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

    try:
        with fsspec.open(recipe_path, "rt") as f:
            lines = f.read().split("\n")
    except FileNotFoundError as err:
        raise FileNotFoundError(f"File not found at: {recipe_path}") from err

    # Filter out comments. We assume all other lines are bash commands.
    commands = [line for line in lines if line and not line.startswith("#")]

    return commands


def make_simple_workflow(recipe_dir: str) -> Workflow:
    """Run the workflow and return its name as a str."""
    commands = _cmds_from_simple_recipe(recipe_dir)
    recipe_config = get_recipe_config(recipe_dir)

    with Workflow(
        generate_name=f"{recipe_config.id}-",
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
    ) as w:
        cmd_templates = []
        for idx, command in enumerate(commands):
            cmd_template = _make_cmd_template(name=f"run-cmd-{idx}", command=command)
            cmd_templates.append(cmd_template)
        fetch_template = _make_fetch_url_template(recipe_config)
        with Steps(name="steps"):
            step = fetch_template()
            for idx, cmd_template in enumerate(cmd_templates):
                step = cmd_template(
                    name=f"step-{idx}",
                    arguments=step.get_artifact("output-dir").with_name("input-dir"),  # type: ignore[union-attr]
                )

    return w
