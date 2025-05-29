from __future__ import annotations

import importlib
import sys
from typing import Any

import pytest
from hera.workflows import Container


# Patch sys.modules to allow re-importing the argo module after env changes
def reload_argo_module(_: object) -> Any:
    # Remove ogdc_runner.argo from sys.modules so it reloads with new env vars
    sys.modules.pop("ogdc_runner.argo", None)
    sys.modules.pop("src.ogdc_runner.argo", None)
    # Remove ARGO_WORKFLOW_SERVICE and argo_manager from global namespace if present
    globals().pop("ARGO_WORKFLOW_SERVICE", None)
    globals().pop("argo_manager", None)
    # Import fresh
    import ogdc_runner.argo

    importlib.reload(ogdc_runner.argo)
    return ogdc_runner.argo


env_test_settings = [
    # when ENVIRONMENT=dev, the `ogdc-runner` image should be used
    ("dev", "ogdc-runner:ogdc_runner_image_tag_test", "Never"),
    # when ENVIRONMENT=production, the `ogdc-runner` image hosted on ghcr should be used
    (
        "production",
        "ghcr.io/qgreenland-net/ogdc-runner:ogdc_runner_image_tag_test",
        "IfNotPresent",
    ),
]


@pytest.mark.parametrize(
    ("env", "expected_image", "expected_pull_policy"), env_test_settings
)
def test__configure_argo_settings_envvar_override(
    env, expected_image, expected_pull_policy, monkeypatch
):
    """Test ArgoManager config picks up envvar overrides and ENVIRONMENT."""
    for envvar in (
        "ARGO_NAMESPACE",
        "ARGO_SERVICE_ACCOUNT_NAME",
        "ARGO_WORKFLOWS_SERVICE_URL",
        "OGDC_RUNNER_IMAGE_TAG",
    ):
        monkeypatch.setenv(envvar, f"{envvar.lower()}_test")
    monkeypatch.setenv("ENVIRONMENT", env)

    argo = reload_argo_module(monkeypatch)
    workflow_service = argo.ARGO_WORKFLOW_SERVICE

    assert workflow_service.host == "argo_workflows_service_url_test"
    assert argo.global_config.namespace == "argo_namespace_test"
    assert argo.global_config.service_account_name == "argo_service_account_name_test"
    assert argo.global_config.image == expected_image
    assert Container().image_pull_policy == expected_pull_policy


def test__configure_argo_settings_dev(monkeypatch):
    """Test ArgoManager config for dev environment defaults."""
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.delenv("OGDC_RUNNER_IMAGE_TAG", raising=False)
    monkeypatch.delenv("ARGO_NAMESPACE", raising=False)
    monkeypatch.delenv("ARGO_SERVICE_ACCOUNT_NAME", raising=False)
    monkeypatch.delenv("ARGO_WORKFLOWS_SERVICE_URL", raising=False)

    argo = reload_argo_module(monkeypatch)
    assert argo.global_config.image == "ogdc-runner:latest"
    assert Container().image_pull_policy == "Never"
    assert argo.global_config.namespace == "qgnet"
    assert argo.global_config.service_account_name == "argo-workflow"


def test__configure_argo_settings_prod(monkeypatch):
    """Test ArgoManager config for production environment defaults."""
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("OGDC_RUNNER_IMAGE_TAG", raising=False)
    monkeypatch.delenv("ARGO_NAMESPACE", raising=False)
    monkeypatch.delenv("ARGO_SERVICE_ACCOUNT_NAME", raising=False)
    monkeypatch.delenv("ARGO_WORKFLOWS_SERVICE_URL", raising=False)

    argo = reload_argo_module(monkeypatch)
    assert argo.global_config.image == "ghcr.io/qgreenland-net/ogdc-runner:latest"
    assert Container().image_pull_policy == "IfNotPresent"
    assert argo.global_config.namespace == "qgnet"
    assert argo.global_config.service_account_name == "argo-workflow"


def test_update_runner_image(monkeypatch):
    """Test update_runner_image updates global_config.image and pull policy."""
    monkeypatch.setenv("ENVIRONMENT", "production")
    argo = reload_argo_module(monkeypatch)
    argo.update_runner_image(image="foo/bar", tag="baz", pull_policy="Always")
    assert argo.global_config.image == "foo/bar:baz"
    assert Container().image_pull_policy == "Always"


def test_update_namespace(monkeypatch):
    """Test update_namespace updates global_config.namespace and workflow_service."""
    monkeypatch.setenv("ENVIRONMENT", "dev")
    argo = reload_argo_module(monkeypatch)
    argo.update_namespace("new-namespace")
    assert argo.global_config.namespace == "new-namespace"
    # Fetch the updated workflow service from the manager
    workflow_service = argo.argo_manager.workflow_service
    assert workflow_service.namespace == "new-namespace"


def test_update_service_account(monkeypatch):
    """Test update_service_account updates global_config.service_account_name."""
    monkeypatch.setenv("ENVIRONMENT", "dev")
    argo = reload_argo_module(monkeypatch)
    argo.update_service_account("new-sa")
    assert argo.global_config.service_account_name == "new-sa"


def test_update_workflow_service_url(monkeypatch):
    """Test update_workflow_service_url updates workflow_service.host."""
    monkeypatch.setenv("ENVIRONMENT", "dev")
    argo = reload_argo_module(monkeypatch)
    argo.update_workflow_service_url("http://new-url")
    # Fetch the updated workflow service from the manager
    workflow_service = argo.argo_manager.workflow_service
    assert workflow_service.host == "http://new-url"
