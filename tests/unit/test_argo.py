from __future__ import annotations

import pytest
from hera.shared import global_config
from hera.workflows import Container

from ogdc_runner.argo import _configure_argo_settings

env_test_settings = [
    # when ENVIRONMENT=dev, the `ogdc-runner` image should be used
    ("dev", "ogdc-runner"),
    # when ENVIRONMENT=prod, the `ogdc-runner` image hosted on ghcr should be used
    ("production", "ghcr.io/qgreenland-net/ogdc-runner:ogdc_runner_image_tag_test"),
]


@pytest.mark.parametrize(("env", "expected_image"), env_test_settings)
def test__configure_argo_settings_envvar_override(env, expected_image, monkeypatch):
    """Test `_configure_argo_settings` envvar overrides in a dev environment"""
    for envvar in (
        "ARGO_NAMESPACE",
        "ARGO_SERVICE_ACCOUNT_NAME",
        "ARGO_WORKFLOWS_SERVICE_URL",
        "OGDC_RUNNER_IMAGE_TAG",
    ):
        monkeypatch.setenv(envvar, f"{envvar.lower()}_test")

    # Set environment
    monkeypatch.setenv("ENVIRONMENT", env)

    workflow_service = _configure_argo_settings()

    assert workflow_service.host == "argo_workflows_service_url_test"
    assert global_config.namespace == "argo_namespace_test"
    assert global_config.service_account_name == "argo_service_account_name_test"
    assert global_config.image == expected_image


def test__configure_argo_settings_dev(monkeypatch):
    """Test `_configure_argo_settings` dev environment"""
    monkeypatch.setenv("ENVIRONMENT", "dev")
    _configure_argo_settings()
    assert global_config.image == "ogdc-runner"
    assert Container().image_pull_policy == "Never"
