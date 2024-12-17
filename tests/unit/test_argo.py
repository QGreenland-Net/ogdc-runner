from __future__ import annotations

from hera.shared import global_config

from ogdc_runner.argo import _configure_argo_settings


def test__configure_argo_settings_envvar_override(monkeypatch):
    for envvar in (
        "ARGO_NAMESPACE",
        "ARGO_SERVICE_ACCOUNT_NAME",
        "ARGO_WORKFLOWS_SERVICE_URL",
    ):
        monkeypatch.setenv(envvar, f"{envvar.lower()}_test")

    workflow_service = _configure_argo_settings()

    assert workflow_service.host[0] == "argo_workflows_service_url_test"
    assert global_config.namespace[0] == "argo_namespace_test"
    assert global_config.service_account_name[0] == "argo_service_account_name_test"
