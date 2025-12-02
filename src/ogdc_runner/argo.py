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
        claim_name="cephfs-qgnet-ogdc-workflow-pvc",
    ),
)


class ArgoConfig:
    """Configuration for Argo workflows."""

    def __init__(
        self,
        namespace: str,
        service_account_name: str,
        workflows_service_url: str,
        runner_image: str,
        runner_image_tag: str,
        image_pull_policy: str,
    ):
        self.namespace = namespace
        self.service_account_name = service_account_name
        self.workflows_service_url = workflows_service_url
        self.runner_image = runner_image
        self.runner_image_tag = runner_image_tag
        self.image_pull_policy = image_pull_policy

    @property
    def full_image_path(self) -> str:
        """Return the full image path with tag."""
        return f"{self.runner_image}:{self.runner_image_tag}"


class ArgoManager:
    """Manager for Argo workflow configurations and services."""

    def __init__(self) -> None:
        self._config = self._initialize_config()
        self._workflow_service = self._setup_workflow_service()
        self._apply_global_config()

    def _initialize_config(self) -> ArgoConfig:
        """Initialize Argo configuration from environment variables with defaults."""
        is_dev_environment = os.environ.get("ENVIRONMENT") == "dev"
        is_local_environment = os.environ.get("ENVIRONMENT") == "local"

        # Default runner image configuration
        runner_image = (
            "ogdc-runner"
            if is_local_environment
            else "ghcr.io/qgreenland-net/ogdc-runner"
        )
        runner_image_tag = os.environ.get("OGDC_RUNNER_IMAGE_TAG", "latest")

        image_pull_policy = "IfNotPresent"
        if is_dev_environment:
            image_pull_policy = "Always"
        elif is_local_environment:
            image_pull_policy = "Never"

        return ArgoConfig(
            namespace=os.environ.get("ARGO_NAMESPACE", "qgnet"),
            service_account_name=os.environ.get(
                "ARGO_SERVICE_ACCOUNT_NAME", "argo-workflow"
            ),
            # TODO: pick up service URL from envvar via helm?
            workflows_service_url=os.environ.get(
                "ARGO_WORKFLOWS_SERVICE_URL",
                "http://qgnet-ogdc-argo-workflows-server.qgnet.svc.cluster.local:2746",
            ),
            runner_image=runner_image,
            runner_image_tag=runner_image_tag,
            image_pull_policy=image_pull_policy,
        )

    def _setup_workflow_service(self) -> WorkflowsService:
        """Set up and return the Argo WorkflowsService with namespace set."""
        return WorkflowsService(
            host=self._config.workflows_service_url, namespace=self._config.namespace
        )

    def _apply_global_config(self) -> None:
        """Apply the current configuration to Hera's global config."""
        global_config.namespace = self._config.namespace
        global_config.service_account_name = self._config.service_account_name
        global_config.image = self._config.full_image_path

        global_config.set_class_defaults(
            Container,
            image_pull_policy=self._config.image_pull_policy,
        )

        global_config.set_class_defaults(
            Workflow,
            # Setup artifact garbage collection
            artifact_gc=models.ArtifactGC(
                strategy="OnWorkflowDeletion",
                service_account_name=self._config.service_account_name,
            ),
            # Setup default OGDC workflow pvc
            volumes=[OGDC_WORKFLOW_PVC],
        )

    @property
    def workflow_service(self) -> WorkflowsService:
        """Get the current workflow service."""
        return self._workflow_service

    @property
    def config(self) -> ArgoConfig:
        """Get the current configuration."""
        return self._config

    def update_image(
        self,
        image: str | None = None,
        tag: str | None = None,
        pull_policy: str | None = None,
    ) -> None:
        """
        Update the runner image configuration and re-apply global config.

        Args:
            image: New image path (without tag)
            tag: New image tag
            pull_policy: New image pull policy
        """
        if image is not None:
            self._config.runner_image = image

        if tag is not None:
            self._config.runner_image_tag = tag

        if pull_policy is not None:
            self._config.image_pull_policy = pull_policy

        # Re-apply global config with updated values
        self._apply_global_config()
        logger.info(
            f"Updated runner image to {self._config.full_image_path} with pull policy {self._config.image_pull_policy}"
        )


# Initialize the ArgoManager as a singleton
ARGO_MANAGER: ArgoManager = ArgoManager()
ARGO_WORKFLOW_SERVICE: WorkflowsService = ARGO_MANAGER.workflow_service


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
