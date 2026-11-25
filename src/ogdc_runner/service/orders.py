from __future__ import annotations

import datetime as dt
import enum
import uuid
from typing import NewType, Self, cast

from loguru import logger
from sqlmodel import Field, Session, SQLModel, col, select, update


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


ALLOWED_STATUS_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.SUBMITTED: frozenset(
        {OrderStatus.RUNNING, OrderStatus.SUCCEEDED, OrderStatus.FAILED}
    ),
    OrderStatus.RUNNING: frozenset(
        {OrderStatus.SUCCEEDED, OrderStatus.FAILED, OrderStatus.UNKNOWN}
    ),
    # Terminal conditions
    OrderStatus.SUCCEEDED: frozenset(),
    OrderStatus.FAILED: frozenset(),
    # UNknown is unlikely to be encountered, and would occur if the exit handler
    # is given an unexpected status message. This could be a failure.
    OrderStatus.UNKNOWN: frozenset(),
}


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


OrderId = NewType("OrderId", str)


def create_order(*, session: Session) -> OrderId:
    """Create a new Order with the "SUBMITTED" status and return it's ID."""
    order_id = str(uuid.uuid4())

    new_order = Order(
        id=order_id,
        status=OrderStatus.SUBMITTED,
    )
    # Register the submission
    session.add(new_order)
    session.commit()

    order_id = cast(OrderId, order_id)

    return order_id


def update_order_status(
    *,
    session: Session,
    order_id: str,
    order_status: OrderStatus,
) -> bool:
    """Update the order with `order_status` if the status transition is allowed.

    Returns `True` if the update requested by this function succeeded or was
    already applied, otherwise `False`. A `False` return indicates that an
    invalid status transition was requested (likely due to a race condition).
    """
    try:
        valid_from = [
            current
            for current, targets in ALLOWED_STATUS_TRANSITIONS.items()
            if order_status in targets
        ]

        result = session.exec(
            update(Order)
            .where(
                col(Order.id) == order_id,
                col(Order.status).in_(valid_from),
            )
            .values(status=order_status)
        )
        session.commit()

        if result.rowcount == 1:
            logger.info(f"Order {order_id} updated with status {order_status}")
            return True

        # Nothing updated: either the order is missing, already in this status,
        # or the transition is stale.
        current_status = session.exec(
            select(Order.status).where(Order.id == order_id)
        ).one_or_none()
        if current_status is None:
            err_msg = f"Order with {order_id=} not found."
            raise RuntimeError(err_msg)
        if current_status == order_status:
            logger.debug(f"Order {order_id} already has status {order_status}")
            return True

        logger.warning(
            f"Ignoring stale status transition for order {order_id}: {current_status} -> {order_status}"
        )
        return False
    except Exception as e:
        logger.error(f"Failed to update {order_id} with status {order_status}: {e}")
        raise e


def get_order_status(*, session: Session, order_id: str) -> OrderStatus:
    order_record = _get_order(session=session, order_id=order_id)

    return order_record.status
