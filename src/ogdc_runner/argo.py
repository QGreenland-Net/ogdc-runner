from __future__ import annotations

import os

from hera.shared import global_config
from hera.workflows import (
    Container,
    Workflow,
    WorkflowsService,
)


def _configure_argo_settings() -> WorkflowsService:
    """Configure argo settings for the OGDC and return an argo WorkflowsService instance.

    Environment variables can be used to override common defaults in dev:

    * ARGO_NAMESPACE: defines the kubernetes namespace the OGDC argo instance is deployed to.
    * ARGO_SERVICE_ACCOUNT_NAME: defines the Argo service account with permissions to execute workflows.
    * ARGO_WORKFLOWS_SERVICE_URL: the OGDC Argo workflows service URL.

    Returns a `WorkflowsService` instance.

    Note that this function is expected to be run only once (automatically) at
    module import time.
    """

    # set argo constants from envvars, falling back on dev settings
    argo_namespace = (os.environ.get("ARGO_NAMESPACE", "argo-helm"),)
    argo_service_account_name = (
        os.environ.get("ARGO_SERVICE_ACCOUNT_NAME", "argo-workflow"),
    )
    argo_workflows_service_url = (
        os.environ.get("ARGO_WORKFLOWS_SERVICE_URL", "http://localhost:2746"),
    )

    # Set global defaults for argo
    # https://hera.readthedocs.io/en/stable/examples/workflows/misc/global_config/
    global_config.namespace = argo_namespace
    global_config.service_account_name = argo_service_account_name

    # TODO: this is dev-specific config!
    global_config.set_class_defaults(
        Container,
        image_pull_policy="Never",
    )
    global_config.image = "ogdc-runner"

    workflows_service = WorkflowsService(host=argo_workflows_service_url)

    return workflows_service


ARGO_WORKFLOW_SERVICE = _configure_argo_settings()


def get_workflow_status(workflow_name: str) -> str | None:
    """Return the given workflow's status (e.g., `'Succeeded'`)"""
    workflow = ARGO_WORKFLOW_SERVICE.get_workflow(name=workflow_name)

    status: str | None = workflow.status.phase  # type: ignore[union-attr]

    return status


def submit_workflow(workflow: Workflow) -> str:
    """Submit the given workflow and return its name as a str."""
    workflow.create()

    workflow_name = workflow.name

    # mypy seems to think that the workflow name might be `None`. I have not
    # encountered this case, but maybe it would indicate a problem we should be
    # aware of?
    if workflow_name is None:
        err_msg = "Problem with submitting workflow."
        raise RuntimeError(err_msg)

    return workflow_name
