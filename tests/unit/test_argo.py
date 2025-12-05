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
    # Remove ARGO_WORKFLOW_SERVICE and ARGO_MANAGER from global namespace if present
    globals().pop("ARGO_WORKFLOW_SERVICE", None)
    globals().pop("ARGO_MANAGER", None)
    # Import fresh
    import ogdc_runner.argo  # noqa: PLC0415

    importlib.reload(ogdc_runner.argo)
    return ogdc_runner.argo


env_test_settings = [
    # when ENVIRONMENT=local, the `ogdc-runner` image should be used
    ("local", "ogdc-runner:ogdc_runner_image_tag_test", "Never"),
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
    monkeypatch.setenv("ENVIRONMENT", "local")
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


def test_ARGO_MANAGER_config_access(monkeypatch):
    """Test that ArgoManager config can be accessed and has correct properties."""
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setenv(
        "ARGO_WORKFLOWS_SERVICE_URL",
        "http://qgnet-ogdc-argo-workflows-server.qgnet.svc.cluster.local:2746",
    )
    argo = reload_argo_module(monkeypatch)

    # Test that we can access the manager and its config
    manager = argo.ARGO_MANAGER
    config = manager.config

    assert config.namespace == "qgnet"
    assert config.service_account_name == "argo-workflow"
    assert (
        config.workflows_service_url
        == "http://qgnet-ogdc-argo-workflows-server.qgnet.svc.cluster.local:2746"
    )
    assert config.runner_image == "ogdc-runner"
    assert config.runner_image_tag == "latest"
    assert config.image_pull_policy == "Never"


def test_ARGO_MANAGER_update_image(monkeypatch):
    """Test ArgoManager.update_image method directly."""
    monkeypatch.setenv("ENVIRONMENT", "local")
    argo = reload_argo_module(monkeypatch)

    # Test direct method call on manager
    argo.ARGO_MANAGER.update_image(
        image="test-image", tag="test-tag", pull_policy="Always"
    )

    assert argo.global_config.image == "test-image:test-tag"
    assert Container().image_pull_policy == "Always"
