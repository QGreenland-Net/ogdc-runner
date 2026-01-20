"""Code for interacting with and publishing outputs of OGDC recipes"""

from __future__ import annotations

import os
from functools import cache

import boto3
import botocore
from botocore.exceptions import ClientError
from hera.workflows import (
    Artifact,
    Container,
    NoneArchiveStrategy,
    Parameter,
    Steps,
    Workflow,
    models,
)
from loguru import logger

from ogdc_runner.argo import (
    ARGO_WORKFLOW_SERVICE,
    OGDC_WORKFLOW_PVC,
    make_generate_name,
    submit_workflow,
)
from ogdc_runner.exceptions import (
    OgdcMissingEnvvar,
    OgdcOutputDataRetrievalError,
    OgdcWorkflowExecutionError,
)
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
    output_filepath = f"/output_dir/{recipe_config.id}.zip"
    template = Container(
        name="publish-data-",
        command=["sh", "-c"],
        args=[
            f"mkdir -p /output_dir/ && cd /input_dir/ && zip -r {output_filepath} ./*",
        ],
        inputs=[Artifact(name="input-dir", path="/input_dir/")],
        outputs=[
            Artifact(
                name="published_zip",
                path=output_filepath,
                archive=NoneArchiveStrategy(),
            )
        ],
    )

    return template


def _publish_template_for_dataone(
    *,
    recipe_config: RecipeConfig,
) -> Container:
    """Creates a container template that will zip final output data and store
    the output as an artifact in dataone."""
    err = "TODO!"
    raise NotImplementedError(err)


def make_publish_template(
    *,
    recipe_config: RecipeConfig,
) -> Container:
    """Creates a container template that will publish final output data."""
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


def check_for_existing_pvc_published_data(
    *,
    recipe_config: RecipeConfig,
) -> bool:
    """Execute argo workflow that checks if the given recipe has published data to PVC.

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


def check_for_existing_published_data(
    *,
    recipe_config: RecipeConfig,
) -> bool:
    """Execute argo workflow that checks if the given recipe has published data.

    Returns `True` if data have already been published for the given recipe,
    otherwise `False`.
    """
    if recipe_config.output.type == "pvc":
        return check_for_existing_pvc_published_data(recipe_config=recipe_config)
    if recipe_config.output.type == "temporary":
        # TODO: implement check for temporary output status. This cannot be
        # derived from the recipe_config alone, because the temporary output
        # location uses a key that's based on the argo workflow name. We could
        # probably use `hera` to filter for a matching workflow based on the
        # `recipe_config.id`, and check its output, but this will be much easier
        # to implement once we track recipe executions in the database.
        logger.warning(
            f"Assuming temporary data are not published for '{recipe_config.name}'"
        )
        return False
    err_msg = "Checking publication status of {recipe_config.output.type} output type is not supported."
    raise NotImplementedError(err_msg)


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


@cache
def _get_s3_client() -> botocore.client.S3:
    s3_secret_access_key_id = os.environ.get("S3_SECRET_KEY_ID")
    s3_secret_access_key = os.environ.get("S3_SECRET_KEY_KEY")
    internal_s3_endpoint_url = os.environ.get("INTERNAL_S3_ENDPOINT_URL")
    if not all(
        (s3_secret_access_key_id, s3_secret_access_key, internal_s3_endpoint_url)
    ):
        err_msg = "S3_SECRET_KEY_ID, S3_SECRET_KEY_KEY, and INTERNAL_S3_ENDPOINT_URL must be set."
        raise OgdcMissingEnvvar(err_msg)

    s3_client = boto3.client(
        service_name="s3",
        endpoint_url=internal_s3_endpoint_url,
        aws_access_key_id=s3_secret_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )

    return s3_client


def _get_presigned_s3_url(s3_key: str) -> str:
    """Get a pre-signed s3 URL for the given s3 key.

    This returns a URL that is only valid for 2 hours, allowing the user to
    download the data before we clean it up.

    See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
    """
    s3_client = _get_s3_client()
    try:
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": "argo-workflows",
                "Key": s3_key,
            },
            # Time in seconds for the presigned URL to remain valid:
            # 2 hours.
            ExpiresIn=60 * 120,
        )
        presigned_url = str(presigned_url)
    except ClientError as e:
        err_msg = f"Error creating pre-signed url for {s3_key}: {e}"
        raise OgdcOutputDataRetrievalError(err_msg) from e

    public_s3_endpoint_url = os.environ.get("PUBLIC_S3_ENDPOINT_URL")
    internal_s3_endpoint_url = os.environ.get("INTERNAL_S3_ENDPOINT_URL")
    if not all((internal_s3_endpoint_url, public_s3_endpoint_url)):
        err_msg = "INTERNAL_S3_ENDPOINT_URL and PUBLIC_S3_ENDPOINT_URL must be set."
        raise OgdcMissingEnvvar(err_msg)

    public_presigned_url = presigned_url.replace(
        str(internal_s3_endpoint_url), str(public_s3_endpoint_url)
    )

    return str(public_presigned_url)


def _check_s3_key_object_exists(s3_key: str) -> bool:
    """Checks if the object given by the s3 key exists.

    Returns `True` if the object exists, otherwise raises an
    OgdcOutputDataRetrievalError.
    """
    s3_client = _get_s3_client()
    try:
        response = s3_client.head_object(Bucket="argo-workflows", Key=s3_key)
        logger.info(
            f"Found object with key {s3_key}. Size: {response.get('ContentLength')}."
        )
        return True
    except ClientError as e:
        err_msg = f"Failed to find s3 object {s3_key}: {e}"
        raise OgdcOutputDataRetrievalError(err_msg) from e


def get_temporary_output_data_url(*, workflow_name: str) -> str:
    """Return the s3 URL for the temporary published output.

    Raises an `OgdcOutputDataRetrievalError` if the published output is not found.
    """
    completed_workflow = ARGO_WORKFLOW_SERVICE.get_workflow(name=workflow_name)
    for node in completed_workflow.status.nodes.values():  # type: ignore[union-attr]
        if node.outputs and node.outputs.artifacts:
            for artifact in node.outputs.artifacts:
                # There is only one expected `published_zip`.
                if artifact.name == "published_zip":
                    s3_key = artifact.s3.key  # type: ignore[union-attr]
                    assert s3_key is not None
                    s3_key = str(s3_key)
                    _check_s3_key_object_exists(s3_key)
                    s3_url = _get_presigned_s3_url(s3_key)
                    return s3_url

    err_msg = "Failed to find an output s3 location from workflow."
    raise OgdcOutputDataRetrievalError(err_msg)
