from __future__ import annotations

from hera.shared import global_config
from hera.workflows import (
    Container,
    WorkflowsService,
)

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

ARGO_WORKFLOW_SERVICE = WorkflowsService(host=ARGO_WORKFLOW_SERVICE_URL)


def get_workflow_status(workflow_name: str) -> str | None:
    """Return the given workflow's status (e.g., `'Succeeded'`)"""
    workflow = ARGO_WORKFLOW_SERVICE.get_workflow(name=workflow_name)

    status: str | None = workflow.status.phase  # type: ignore[union-attr]

    return status
