from __future__ import annotations

from hera.workflows import (
    Artifact,
    Container,
    Steps,
)

from ogdc_runner.argo import (
    OgdcWorkflow,
    submit_workflow,
)
from ogdc_runner.exceptions import OgdcInvalidRecipeConfig
from ogdc_runner.inputs import make_fetch_input_template
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.publish import make_publish_template


def make_cmd_template(
    name: str,
    command: str,
) -> Container:
    """Creates a command template with an optional custom image."""
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


def make_and_submit_shell_workflow(
    recipe_config: RecipeConfig,
    wait: bool,
) -> str:
    """Create and submit an argo workflow based on a shell recipe.

    Args:
        recipe_config: The recipe configuration
        wait: Whether to wait for the workflow to complete

    Returns the name of the workflow as a str.
    """
    if recipe_config.workflow.type != "shell":
        err_msg = f"Expected recipe configuration with workflow type `shell`. Got: {recipe_config.workflow.type}"
        raise OgdcInvalidRecipeConfig(err_msg)

    # Parse commands from the recipe's shell file
    commands = recipe_config.workflow.get_commands_from_sh_file()

    with OgdcWorkflow(
        name="shell",
        recipe_config=recipe_config,
        archive_workflow=True,
        entrypoint="steps",
    ) as w:
        # Create command templates
        cmd_templates = []
        for idx, command in enumerate(commands):
            cmd_template = make_cmd_template(
                name=f"run-cmd-{idx}",
                command=command,
            )
            cmd_templates.append(cmd_template)

        # Use the multi-input fetch template
        fetch_template = make_fetch_input_template(
            recipe_config=recipe_config,
        )

        # Create publication template
        publish_template = make_publish_template(
            recipe_config=recipe_config,
        )

        # Create the workflow steps
        with Steps(name="steps"):
            step = fetch_template()
            for idx, cmd_template in enumerate(cmd_templates):
                step = cmd_template(
                    name=f"step-{idx}",
                    arguments=step.get_artifact("output-dir").with_name("input-dir"),  # type: ignore[union-attr]
                )
            # Publish final data
            publish_template(
                name="publish-data",
                arguments=step.get_artifact("output-dir").with_name("input-dir"),  # type: ignore[union-attr]
            )

    # Submit the workflow
    workflow_name = submit_workflow(w, wait=wait)

    return workflow_name
