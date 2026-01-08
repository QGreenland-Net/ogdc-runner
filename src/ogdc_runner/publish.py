"""Code for interacting with and publishing outputs of OGDC recipes"""

from __future__ import annotations

from hera.workflows import (
    Artifact,
    Container,
    Parameter,
    Steps,
    Workflow,
    models,
)

from ogdc_runner.argo import (
    ARGO_WORKFLOW_SERVICE,
    OGDC_WORKFLOW_PVC,
    make_generate_name,
    submit_workflow,
)
from ogdc_runner.exceptions import OgdcWorkflowExecutionError
from ogdc_runner.models.recipe_config import (
    DataOneRecipeOutput,
    PvcRecipeOutput,
    RecipeConfig,
    TemporaryRecipeOutput,
)


def _publish_template_for_pvc(
    *,
    recipe_config: RecipeConfig,
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
                sub_path=recipe_config.id,
            )
        ],
    )

    return template


def _publish_template_for_temporary_output(
    *,
    recipe_config: RecipeConfig,
) -> Container:
    """Creates a container template that will zip final output data and store
    the output as an artifact in minio."""
    err = "TODO!"
    raise NotImplementedError(err)


def _publish_template_for_dataone(
    *,
    recipe_config: RecipeConfig,
) -> Container:
    """Creates a container template that will zip final output data and store
    the output as an artifact in minio."""
    err = "TODO!"
    raise NotImplementedError(err)


def make_publish_template(
    *,
    recipe_config: RecipeConfig,
) -> Container:
    """Creates a container template that will move final output data into the
    OGDC data storage volume under a subpath named for the recipe_id."""
    if isinstance(recipe_config.output, PvcRecipeOutput):
        return _publish_template_for_pvc(recipe_config=recipe_config)
    if isinstance(recipe_config.output, TemporaryRecipeOutput):
        return _publish_template_for_temporary_output(recipe_config=recipe_config)
    if isinstance(recipe_config.output, DataOneRecipeOutput):
        return _publish_template_for_dataone(recipe_config=recipe_config)
    err_msg = f"{type(recipe_config.output)} is not a recognized publication method."  # type: ignore[unreachable]
    raise NotImplementedError(err_msg)


def remove_existing_published_data(
    *,
    recipe_config: RecipeConfig,
) -> None:
    """Executes an argo workflow that removes published data for a recipe if it
    exists."""
    with Workflow(
        generate_name=make_generate_name(recipe_config.id, "-remove-existing-data-"),
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

        with Steps(name="steps"):
            overwrite_template()

    workflow_name = submit_workflow(workflow=w, wait=True)

    # Cleanup this workflow, it is no longer needed
    ARGO_WORKFLOW_SERVICE.delete_workflow(workflow_name)


def check_for_existing_published_data(
    *,
    recipe_config: RecipeConfig,
) -> bool:
    """Execute argo workflow that checks if the given recipe has published data.

    Returns `True` if data have already been published for the given recipe,
    otherwise `False`.
    """
    with Workflow(
        generate_name=make_generate_name(recipe_config.id, "-check-published-"),
        entrypoint="steps",
        workflows_service=ARGO_WORKFLOW_SERVICE,
    ) as w:
        check_dir_template = Container(
            name="check-already-published",
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
        )
        return False

    return check_for_existing_published_data(
        recipe_config=recipe_config,
    )
