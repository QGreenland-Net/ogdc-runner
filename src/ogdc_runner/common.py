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
from loguru import logger

from ogdc_runner.argo import (
    ARGO_WORKFLOW_SERVICE,
    OGDC_WORKFLOW_PVC,
    submit_workflow,
    update_namespace,
    update_runner_image,
)
from ogdc_runner.exceptions import OgdcWorkflowExecutionError
from ogdc_runner.models.recipe_config import RecipeConfig


def make_cmd_template(
    name: str,
    command: str,
    custom_image: str | None = None,
    custom_tag: str | None = None,
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

    # Set custom image if provided
    if custom_image or custom_tag:
        # This will override the global image setting just for this container
        template.image = custom_image if custom_image else None
        template.image_tag = custom_tag if custom_tag else None

    return template


def make_fetch_input_template(
    recipe_config: RecipeConfig,
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> Container:
    """Creates a container template that fetches multiple inputs from URLs or file paths.

    Supports:
    - HTTP/HTTPS URLs
    - File paths (including PVC paths)
    """
    # Create commands to fetch each input
    fetch_commands = []

    for i, param in enumerate(recipe_config.input.params):
        param_str = str(param)
        # Check if the parameter is a URL
        if param_str.startswith(("http://", "https://")):
            # It's a URL, use wget
            fetch_commands.append(
                f"wget --content-disposition -P /output_dir/ {param_str}"
            )
        else:
            # It's a file path (including PVC paths), use cp
            # Get just the filename for the destination
            filename = param_str.split("/")[-1]
            fetch_commands.append(f"cp {param_str} /output_dir/{filename}")

    # Join all commands with && for sequential execution
    combined_command = " && ".join(fetch_commands)
    if not combined_command:
        combined_command = "echo 'No input files to fetch'"

    template = Container(
        name=f"{recipe_config.id}-fetch-template-",
        command=["sh", "-c"],
        args=[
            f"mkdir -p /output_dir/ && {combined_command}",
        ],
        outputs=[Artifact(name="output-dir", path="/output_dir/")],
    )

    # Set custom image if provided
    if custom_image or custom_tag:
        # This will override the global image setting just for this container
        template.image = custom_image if custom_image else None
        template.image_tag = custom_tag if custom_tag else None

    return template


def make_publish_template(
    recipe_id: str,
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> Container:
    """Creates a container template that will move final output data into the
    OGDC data storage volume under a subpath named for the recipe_id."""
    template = Container(
        name="publish-data-",
        command=["sh", "-c"],
        args=[
            "rsync --progress /input_dir/* /output_dir/",
        ],
        inputs=[Artifact(name="input-dir", path="/input_dir/")],
        volume_mounts=[
            models.VolumeMount(
                name=OGDC_WORKFLOW_PVC.name,
                mount_path="/output_dir/",
                sub_path=recipe_id,
            )
        ],
    )

    # Set custom image if provided
    if custom_image or custom_tag:
        # This will override the global image setting just for this container
        template.image = custom_image if custom_image else None
        template.image_tag = custom_tag if custom_tag else None

    return template


def remove_existing_published_data(
    *,
    recipe_config: RecipeConfig,
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> None:
    """Executes an argo workflow that removes published data for a recipe if it
    exists."""
    with Workflow(
        generate_name=f"{recipe_config.id}-remove-existing-data-",
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
    ) as w:
        overwrite_template = Container(
            name="overwrite-already-published-",
            command=["sh", "-c"],
            args=[
                f"rm -rf /mnt/{recipe_config.id}",
            ],
            volume_mounts=[
                models.VolumeMount(
                    name=OGDC_WORKFLOW_PVC.name,
                    mount_path="/mnt/",
                ),
            ],
        )

        # Set custom image if provided
        if custom_image or custom_tag:
            # This will override the global image setting just for this container
            overwrite_template.image = custom_image if custom_image else None
            overwrite_template.image_tag = custom_tag if custom_tag else None

        with Steps(name="steps"):
            overwrite_template()

    workflow_name = submit_workflow(workflow=w, wait=True)

    # Cleanup this workflow, it is no longer needed
    ARGO_WORKFLOW_SERVICE.delete_workflow(workflow_name)


def check_for_existing_published_data(
    *,
    recipe_config: RecipeConfig,
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> bool:
    """Execute argo workflow that checks if the given recipe has published data.

    Returns `True` if data have already been published for the given recipe,
    otherwise `False`.
    """
    with Workflow(
        generate_name=f"{recipe_config.id}-check-published-",
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
    ) as w:
        check_dir_template = Container(
            name="check-already-published-",
            command=["sh", "-c"],
            # Check for the existence of the recipe-specific subpath. If it
            # exists, write out a file with "yes". Otherwise write out a file
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
                    name=OGDC_WORKFLOW_PVC.name,
                    mount_path="/mnt/",
                ),
            ],
        )

        # Set custom image if provided
        if custom_image or custom_tag:
            # This will override the global image setting just for this container
            check_dir_template.image = custom_image if custom_image else None
            check_dir_template.image_tag = custom_tag if custom_tag else None

        with Steps(name="steps"):
            check_dir_template()

    # wait for the workflow to complete.
    workflow_name = submit_workflow(workflow=w, wait=True)

    # If overwrite is not True, we need to check the result of the
    # `check-already-published` step to see if the data have been published or
    # not.
    # Check the result. Get an updated instance of the workflow, with the latest
    # states for all notdes. Then, iterate through the nodes and find the
    # template we define above ("check-already-published") and extract its
    # output parameter.
    completed_workflow = ARGO_WORKFLOW_SERVICE.get_workflow(name=workflow_name)
    result = None
    for node in completed_workflow.status.nodes.values():  # type: ignore[union-attr]
        if node.template_name == "check-already-published":
            result = node.outputs.parameters[0].value  # type: ignore[union-attr, index]
    if not result:
        err_msg = "Failed to check if data have been published"
        raise OgdcWorkflowExecutionError(err_msg)

    assert result in ("yes", "no")

    # Cleanup this workflow, it is no longer needed
    ARGO_WORKFLOW_SERVICE.delete_workflow(workflow_name)

    return result == "yes"


def data_already_published(
    *,
    recipe_config: RecipeConfig,
    overwrite: bool,
    custom_image: str | None = None,
    custom_tag: str | None = None,
) -> bool:
    """Check for the existence of published data for the given
    recipe and optionally remove it.

    If `overwrite=True`, this function will remove any existing published data
    for the provided recipe.

    Returns `True` if data have already been published for the given recipe,
    otherwise `False`.
    """
    if overwrite:
        # If `overwrite` is True, remove the existing data and return `False`.
        remove_existing_published_data(
            recipe_config=recipe_config,
            custom_image=custom_image,
            custom_tag=custom_tag,
        )
        return False

    return check_for_existing_published_data(
        recipe_config=recipe_config,
        custom_image=custom_image,
        custom_tag=custom_tag,
    )


def parse_commands_from_recipe_file(recipe_dir: str, filename: str) -> list[str]:
    """Read commands from a recipe file.

    Args:
        recipe_dir: The directory containing the recipe file
        filename: The name of the recipe file to parse

    Returns:
        A list of commands from the recipe file, with comments removed
    """
    recipe_path = f"{recipe_dir}/{filename}"
    logger.info(f"Reading recipe from {recipe_path}")

    with fsspec.open(recipe_path, "rt") as f:
        lines = f.read().split("\n")
    commands = [line for line in lines if line and not line.startswith("#")]

    return commands


def apply_custom_container_config(
    workflow: Workflow,
    custom_image: str | None = None,
    custom_tag: str | None = None,
    custom_namespace: str | None = None,
    update_global: bool = False,
) -> None:
    """Apply custom configuration to a workflow.

    Args:
        workflow: The workflow to configure
        custom_image: Optional custom image to use
        custom_tag: Optional custom tag for the image
        custom_namespace: Optional custom namespace
        update_global: If True, update the global config
    """
    if update_global:
        if custom_image or custom_tag:
            update_runner_image(image=custom_image, tag=custom_tag)
        if custom_namespace:
            update_namespace(namespace=custom_namespace)
