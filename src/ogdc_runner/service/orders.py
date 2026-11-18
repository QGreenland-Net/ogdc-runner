from __future__ import annotations

import datetime as dt
import enum
from typing import Self

from loguru import logger
from sqlmodel import Field, Session, SQLModel


class OrderStatus(enum.StrEnum):
    SUBMITTED = "Submitted"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"

    @classmethod
    def _missing_(cls, value: object) -> Self | None:
        """Return `FAILED` if an argo workflow reports status `Error`.

        According to
        `https://argo-workflows.readthedocs.io/en/latest/lifecyclehook/`, a
        workflow status can have `Failed` or `Error`.

        TODO: consider handling both cases separately. They mean different things!
        """
        if isinstance(value, str) and value.lower() == "error":
            return cls.FAILED
        return None


# TODO: is "order" the right word here? Use of "Workflow" maybe should be
# constrained to argo specifically? And a recipe submitted to the OGDC would be
# an "order" (or some other phrase) that executes 1 or more argo workflows.
# This would require an update to our API routes (instead of POST/GET from
# `/workflows/`, it would be e.g., `/orders/`. We could still expose endpoints
# for getting status of individudal argo workflows, but that becomes less
# important under this change.
class Order(SQLModel, table=True):
    id: str = Field(primary_key=True)
    timestamp: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.UTC))
    status: OrderStatus
    # TODO: figure out how to get username from signed-in user.
    # username: str


def _get_order(*, session: Session, order_id: str) -> Order:
    order_record = session.get(Order, order_id)
    if not order_record:
        # TODO: better error.
        raise RuntimeError(f"Order with {order_id=} not found.")

    return order_record


def update_order_status(
    *, session: Session, order_id: str, order_status: OrderStatus
) -> None:
    try:
        order_record = _get_order(session=session, order_id=order_id)
        # TODO: check state transition? E.g., ensure that a workflow can't
        # go from `Succeeded` to `Running`? It might be possible that the
        # "Running" lifecycle hook runs after the "on-exit" handler?
        order_record.status = order_status
        session.commit()
    except Exception as e:
        logger.error(f"Failed to update {order_id} with status {order_status}: {e}")
        raise e

    logger.info(f"Order {order_id} updated with status {order_status}")


def get_order_status(*, session: Session, order_id: str) -> OrderStatus:
    order_record = _get_order(session=session, order_id=order_id)

    return order_record.status
