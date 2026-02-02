from __future__ import annotations

import os
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from hera.shared import global_config
from hera.workflows import (
    Container,
    Workflow,
    WorkflowsService,
    models,
)
from loguru import logger

from ogdc_runner.exceptions import OgdcWorkflowExecutionError
from ogdc_runner.models.recipe_config import RecipeConfig

# Kubernetes names must be no more than 63 characters.
# Argo appends a 5-character random suffix to generate_name, so we reserve space for it.
KUBERNETES_NAME_MAX_LENGTH = 63
ARGO_GENERATED_SUFFIX_LENGTH = 5


def make_generate_name(*, recipe_id: str, suffix: str = "") -> str:
    """Create a workflow generate_name, truncating recipe_id if necessary.

    Kubernetes names must be no more than 63 characters. Argo appends a 5-character
    random suffix to generate_name to create the final workflow name. This function
    truncates the recipe_id as needed to ensure the final name stays within the limit.

    Args:
        recipe_id: The recipe identifier to use as the base name
        suffix: The suffix to append (e.g., "remove-existing-data")

    Returns:
        A generate_name string that will produce a valid Kubernetes name
    """
    # Reserve space for Argo's generated suffix
    max_generate_name_length = KUBERNETES_NAME_MAX_LENGTH - ARGO_GENERATED_SUFFIX_LENGTH
    max_id_length = max_generate_name_length - len(suffix)
    truncated_id = recipe_id[:max_id_length]
    if suffix:
        return f"{truncated_id}-{suffix}-"
    return truncated_id


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
        env = os.environ.get("ENVIRONMENT")
        is_dev_environment = env == "dev"
        is_local_environment = env == "local"
        logger.info(f"Using ENVIRONMENT={env}")

        # Default runner image configuration
        runner_image = (
            "ogdc-runner"
            if is_local_environment
            else "ghcr.io/qgreenland-net/ogdc-runner"
        )
        runner_image_tag = os.environ.get("OGDC_RUNNER_IMAGE_TAG", "latest")

        image_pull_policy = "IfNotPresent"
        if is_dev_environment:
            # In dev, we expect the `latest` image to be used, so we always want
            # the latest pulled and updated.
            image_pull_policy = "Always"

        # Argo workflows service URL
        workflows_service_url = os.environ.get(
            "ARGO_WORKFLOWS_SERVICE_URL",
            # Default to locally-hosted argo workflows.
            "http://localhost:2746",
        )
        logger.info(f"Using ARGO_WORKFLOWS_SERVICE_URL={workflows_service_url}")

        return ArgoConfig(
            namespace=os.environ.get("ARGO_NAMESPACE", "qgnet"),
            service_account_name=os.environ.get(
                "ARGO_SERVICE_ACCOUNT_NAME", "argo-workflow"
            ),
            workflows_service_url=workflows_service_url,
            runner_image=runner_image,
            runner_image_tag=runner_image_tag,
            image_pull_policy=image_pull_policy,
        )

    def _setup_workflow_service(self) -> WorkflowsService:
        """Set up and return the Argo WorkflowsService with namespace set."""
        return WorkflowsService(
            host=self._config.workflows_service_url, namespace=self._config.namespace
        )

    def _workflow_archival_and_deletion_config(self) -> dict[str, Any]:
        """Setup workflow TTL and artifact garbage collection.

        These two settings work together.

        TTLStrategy sets the number of seconds until a workflow is deleted when
        the workflow is successful. On deletion, artifacts are cleaned up, per
        the ArtifactGC configuration.

        TTL is set for 7 days to provide sufficient time for inspection of
        intermediate outputs and retrieval of temporary final outputs. Temporary
        final outputs are stored as artifacts associated with the workflow, so
        when the workflow is cleaned up so will the output associatged with it.

        Only successful workflows are automatically cleaned up. Failed workflows
        are not automatically archived or cleaned up to provide unlimited time
        for debugging/inspection of outputs.
        """
        # Setup default TTL strategy
        ttl_strategy = models.TTLStrategy(
            # Only cleanup successful workflows.
            # Sufficient time should be provided to allow users to download
            # outputs if they are of the "temporary" type (7 days)
            # TODO: consider delegating that responsibility to the workflows
            # themselves _if_ the output type is temporary. If not, the
            # output is persisted someplace else, and we don't need to keep
            # the workflow (and thus the artifacts) around for very
            # long. This could default to a day or something like that.
            seconds_after_success=60 * 24 * 7,
        )

        # Setup artifact garbage collection
        artifact_gc = models.ArtifactGC(
            strategy="OnWorkflowDeletion",
            service_account_name=self._config.service_account_name,
        )

        return {
            "artifact_gc": artifact_gc,
            "ttl_strategy": ttl_strategy,
        }

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
            # Setup default OGDC workflow pvc
            volumes=[OGDC_WORKFLOW_PVC],
            # Setup workflow archival and deletion
            **self._workflow_archival_and_deletion_config(),
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


@contextmanager
def OgdcWorkflow(
    *,
    recipe_config: RecipeConfig,
    name: str,
    archive_workflow: bool,
    **kwargs: Any,
) -> Generator[Workflow, None, None]:
    """Contexts manager that yields an argo workflow with configuration driven by `recipe_config`.

    kwargs:

        - `recipe_config`: Recipe configuration that is driving this workflow.
        - `name`: name of this workflow. Used with the recipe ID to generate a
          name for the argo workflow.
        - `archive_workflow`: Set to `True` to archive this workflow on workflow
          success. It is recommended to archive workflows that are used to
          transform data for provenance and metrics reasons. Workflows that do
          some task not related to data transformation can set this to `False`.

    This context manager sets the following on argo.workflows.Workflow:

    * `generate_name`: based on recipe ID and the `name`.
    * `workflows_service`: uses the ogdc's configured Argo workflows service
    * `labels`: Adds `ogdc/persist-workflow-in-archive` label based on
      `archive_workflow` kwarg. Other passed `labels` are preserved.

    All other kwargs are passed directly to `argo.workflows.Workflow.
    """

    # Merge labels provided by user with `ogdc/persist-workflow-in-archive`.
    labels = {
        **kwargs.pop("labels", {}),
        "ogdc/persist-workflow-in-archive": "true" if archive_workflow else "false",
    }

    workflow_kwargs = {
        # user kwargs first. This ensures that configurations set by this
        # function get priority (everything after this).
        **kwargs,
        # OGDC-specific behavior that we want to be consistent about.
        "generate_name": make_generate_name(
            recipe_id=recipe_config.id,
            suffix=name.replace(" ", "-"),
        ),
        "workflows_service": ARGO_WORKFLOW_SERVICE,
        "labels": labels,
    }

    with Workflow(
        **workflow_kwargs,
    ) as w:
        yield w
