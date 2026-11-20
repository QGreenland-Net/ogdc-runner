from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

import pytest
from hera.workflows import Workflow, models

from ogdc_runner import argo
from ogdc_runner.service.db import get_db_session
from ogdc_runner.service.orders import Order, OrderId, create_order


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
            # Successful workflows will be deleted after 1 second unless the
            # workflow already set a longer TTL, such as temporary-output
            # workflows whose artifacts need to remain available for download.
            if w.ttl_strategy is None:
                w.ttl_strategy = models.TTLStrategy(
                    seconds_after_success=1,
                )
            yield w

    monkeypatch.setattr(argo, "Workflow", wrapper)


@pytest.fixture(autouse=True)
def set_envvars(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "local")
    # Required for integration tests to interact with the database
    # TODO: these values assume the default local deployment config. Document
    # this? Or is this a code-smell? The credentials are baked into the helm
    # deployment, but the integration tests assume being able to interact with
    # components that are deployed to the cluster.
    monkeypatch.setenv("OGDC_DB_HOST", "localhost")
    monkeypatch.setenv("OGDC_DB_USERNAME", "admin")
    monkeypatch.setenv("OGDC_DB_PASSWORD", "password")


@pytest.fixture
def test_order_id(
    set_envvars,  # noqa: ARG001
) -> Generator[OrderId]:
    with get_db_session() as session:
        order_id = create_order(session=session)

    yield order_id

    # Delete the associated order after the test completes.
    with get_db_session() as session:
        order = session.get(Order, order_id)
        if order:
            session.delete(order)
            session.commit()
