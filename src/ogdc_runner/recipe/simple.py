from __future__ import annotations

from hera.workflows import (
    Steps,
    Workflow,
)

from ogdc_runner.argo import (
    ARGO_WORKFLOW_SERVICE,
    submit_workflow,
)

# Import common utilities
from ogdc_runner.common import (
    apply_custom_container_config,
    data_already_published,
    make_cmd_template,
    make_fetch_input_template,
    make_publish_template,
    parse_commands_from_recipe_file,
)
from ogdc_runner.constants import SIMPLE_RECIPE_FILENAME
from ogdc_runner.exceptions import OgdcDataAlreadyPublished
from ogdc_runner.models.recipe_config import RecipeConfig
from ogdc_runner.recipe import get_recipe_config


def make_and_submit_simple_workflow(
    recipe_config: RecipeConfig,
    wait: bool,
    custom_image: str | None = None,
    custom_tag: str | None = None,
    custom_namespace: str | None = None,
    update_global: bool = False,
) -> str:
    """Create and submit an argo workflow based on a simple recipe.

    Args:
        recipe_config: The recipe configuration
        wait: Whether to wait for the workflow to complete
        custom_image: Optional custom image to use for all containers
        custom_tag: Optional custom tag for the image
        custom_namespace: Optional custom namespace for the workflow
        update_global: If True, update the global image config; if False, only apply to this workflow

    Returns the name of the workflow as a str.
    """
    # Parse commands from the simple recipe file
    commands = parse_commands_from_recipe_file(
        recipe_config.recipe_directory,
        SIMPLE_RECIPE_FILENAME,
    )

    with Workflow(
        generate_name=f"{recipe_config.id}-",
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
    ) as w:
        # Apply custom configuration if provided
        apply_custom_container_config(
            workflow=w,
            custom_image=custom_image,
            custom_tag=custom_tag,
            custom_namespace=custom_namespace,
            update_global=update_global,
        )

        # Create command templates
        cmd_templates = []
        for idx, command in enumerate(commands):
            cmd_template = make_cmd_template(
                name=f"run-cmd-{idx}",
                command=command,
                custom_image=custom_image,
                custom_tag=custom_tag,
            )
            cmd_templates.append(cmd_template)

        # Use the multi-input fetch template
        fetch_template = make_fetch_input_template(
            recipe_config=recipe_config,
            custom_image=custom_image,
            custom_tag=custom_tag,
        )

        # Create publication template
        publish_template = make_publish_template(
            recipe_id=recipe_config.id,
            custom_image=custom_image,
            custom_tag=custom_tag,
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


def submit_ogdc_recipe(
    *,
    recipe_dir: str,
    wait: bool,
    overwrite: bool,
    custom_image: str | None = None,
    custom_tag: str | None = None,
    custom_namespace: str | None = None,
    update_global: bool = False,
) -> str:
    """Submit an OGDC recipe for processing via argo workflows.

    Args:
        recipe_dir: Path to the recipe directory
        wait: Whether to wait for the workflow to complete
        overwrite: Whether to overwrite existing published data
        custom_image: Optional custom image to use for all containers
        custom_tag: Optional custom tag for the image
        custom_namespace: Optional custom namespace for the workflow
        update_global: If True, update the global image config; if False, only apply to this workflow

    Returns the name of the OGDC simple recipe submitted to Argo.
    """
    # Get the recipe configuration
    recipe_config = get_recipe_config(recipe_dir)

    # Check if the user-submitted workflow has already been published
    if data_already_published(
        recipe_config=recipe_config,
        overwrite=overwrite,
        custom_image=custom_image,
        custom_tag=custom_tag,
    ):
        err_msg = f"Data for recipe {recipe_config.id} have already been published."
        raise OgdcDataAlreadyPublished(err_msg)

    # We currently expect all recipes to be "simple"
    simple_recipe_workflow_name = make_and_submit_simple_workflow(
        recipe_config=recipe_config,
        wait=wait,
        custom_image=custom_image,
        custom_tag=custom_tag,
        custom_namespace=custom_namespace,
        update_global=update_global,
    )

    return simple_recipe_workflow_name
