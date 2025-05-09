from __future__ import annotations

import os
import time

from hera.shared import global_config
from hera.workflows import (
    Container,
    Workflow,
    WorkflowsService,
    models,
)
from loguru import logger

from ogdc_runner.exceptions import OgdcWorkflowExecutionError

OGDC_WORKFLOW_PVC = models.Volume(
    name="workflow-volume",
    persistent_volume_claim=models.PersistentVolumeClaimVolumeSource(
        claim_name="qgnet-ogdc-workflow-pvc",
    ),
)


def _configure_argo_settings() -> WorkflowsService:
    """Configure argo settings for the OGDC and return an argo WorkflowsService instance.

    Environment variables can be used to override common defaults:

    * ARGO_NAMESPACE: defines the kubernetes namespace the OGDC argo instance is deployed to.
    * ARGO_SERVICE_ACCOUNT_NAME: defines the Argo service account with permissions to execute workflows.
    * ARGO_WORKFLOWS_SERVICE_URL: the OGDC Argo workflows service URL.
    * OGDC_RUNNER_IMAGE_TAG: the docker image tag to use for `ogdc-runner` in
      non-development environments. Defaults to "latest".
    * ENVIRONMENT: if set to `dev`, a local `ogdc-runner` image will be used
      instead of pulling a version from GHCR. Build with `docker build -t
      ogdc-runner .` in the `rancher-desktop` docker context.

    Returns a `WorkflowsService` instance.

    Note that this function is expected to be run only once (automatically) at
    module import time.
    """

    # set argo constants from envvars, falling back on dev settings
    argo_namespace = os.environ.get("ARGO_NAMESPACE", "qgnet")
    argo_service_account_name = os.environ.get(
        "ARGO_SERVICE_ACCOUNT_NAME", "argo-workflow"
    )
    argo_workflows_service_url = os.environ.get(
        "ARGO_WORKFLOWS_SERVICE_URL", "http://localhost:2746"
    )
    ogdc_runner_image_tag = os.environ.get("OGDC_RUNNER_IMAGE_TAG", "latest")

    # Set global defaults for argo
    # https://hera.readthedocs.io/en/stable/examples/workflows/misc/global_config/
    global_config.namespace = argo_namespace
    global_config.service_account_name = argo_service_account_name

    # If the environment is explicitly set to "dev", then use a locally-built image.
    if os.environ.get("ENVIRONMENT") == "dev":
        global_config.image = "ogdc-runner"
        global_config.set_class_defaults(
            Container,
            image_pull_policy="Never",
        )
    else:
        global_config.image = (
            f"ghcr.io/qgreenland-net/ogdc-runner:{ogdc_runner_image_tag}"
        )

    global_config.set_class_defaults(
        Workflow,
        # Setup artifact garbage collection. This will tell argo to remove artifacts
        # on workflow deletion.
        artifact_gc=models.ArtifactGC(
            strategy="OnWorkflowDeletion",
            service_account_name=argo_service_account_name,
        ),
        # Setup default OGDC workflow pvc, which is where final outputs are
        # written.
        volumes=[OGDC_WORKFLOW_PVC],
    )
    workflows_service = WorkflowsService(host=argo_workflows_service_url)

    return workflows_service


ARGO_WORKFLOW_SERVICE = _configure_argo_settings()


def get_workflow_status(workflow_name: str) -> str | None:
    """Return the given workflow's status (e.g., `'Succeeded'`)"""
    workflow = ARGO_WORKFLOW_SERVICE.get_workflow(name=workflow_name)

    status: str | None = workflow.status.phase  # type: ignore[union-attr]

    return status


def wait_for_workflow_completion(workflow_name: str) -> None:
    while True:
        status = get_workflow_status(workflow_name)
        if status:
            logger.info(f"Workflow status: {status}")
            # Terminal states
            if status == "Failed":
                raise OgdcWorkflowExecutionError(
                    f"Workflow with name {workflow_name} failed."
                )
            if status == "Succeeded":
                return
        time.sleep(5)


def submit_workflow(workflow: Workflow, *, wait: bool = False) -> str:
    """Submit the given workflow and return its name as a str."""
    workflow.create()

    workflow_name = workflow.name

    # mypy seems to think that the workflow name might be `None`. I have not
    # encountered this case, but maybe it would indicate a problem we should be
    # aware of?
    if workflow_name is None:
        err_msg = "Problem with submitting workflow."
        raise OgdcWorkflowExecutionError(err_msg)

    logger.success(f"Successfully submitted workflow with name {workflow_name}")

    if wait:
        wait_for_workflow_completion(workflow_name)

    return workflow_name
