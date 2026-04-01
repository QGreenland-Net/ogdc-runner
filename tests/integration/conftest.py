from __future__ import annotations

from contextlib import contextmanager

import pytest
from hera.workflows import Workflow, models

from ogdc_runner import argo


@pytest.fixture(autouse=True)
def delete_and_do_not_archive_workflows(monkeypatch):
    """Cleanup successful argo workflows after completion and ensure they are not
    archived.

    This fixture auto-runs for all tests in this directory.
    """

    @contextmanager
    def wrapper(*args, **kwargs):
        with Workflow(*args, **kwargs) as w:
            assert w.labels
            # label this workflow so that it is not archived.
            w.labels["ogdc/persist-workflow-in-archive"] = "false"
            # Successful workflows will be deleted after 1 second.
            w.ttl_strategy = models.TTLStrategy(
                seconds_after_success=1,
            )
            yield w

    monkeypatch.setattr(argo, "Workflow", wrapper)
