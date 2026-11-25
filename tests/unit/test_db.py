from __future__ import annotations

from typing import cast

import pytest
from sqlmodel import Session, SQLModel, create_engine

from ogdc_runner.service.orders import (
    ALLOWED_STATUS_TRANSITIONS,
    Order,
    OrderId,
    OrderStatus,
    create_order,
    get_order_status,
    update_order_status,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

    engine.dispose()


def test_create_order_and_update_order_status(session):
    order_id = create_order(session=session)

    assert (
        update_order_status(
            session=session, order_id=order_id, order_status=OrderStatus.RUNNING
        )
        is True
    )

    assert get_order_status(session=session, order_id=order_id) == OrderStatus.RUNNING


def _create_order_with_status(session: Session, order_status: OrderStatus) -> OrderId:
    order_id = "test-order-id"
    session.add(Order(id=order_id, status=order_status))
    session.commit()
    order_id = cast(OrderId, order_id)
    return order_id


@pytest.mark.parametrize(
    ("start_status", "target_status"),
    [
        (start_status, target_status)
        for start_status, targets in ALLOWED_STATUS_TRANSITIONS.items()
        for target_status in targets
    ],
)
def test_update_order_status_allowed_transition_is_applied(
    session, start_status, target_status
):
    order_id = _create_order_with_status(session=session, order_status=start_status)

    assert (
        update_order_status(
            session=session,
            order_id=order_id,
            order_status=target_status,
        )
        is True
    )

    assert get_order_status(session=session, order_id=order_id) == target_status


@pytest.mark.parametrize(
    ("start_status", "target_status"),
    [
        (OrderStatus.SUCCEEDED, OrderStatus.RUNNING),  # late "Running" hook
        (OrderStatus.FAILED, OrderStatus.RUNNING),
        (OrderStatus.SUCCEEDED, OrderStatus.FAILED),
        (OrderStatus.RUNNING, OrderStatus.SUBMITTED),  # backwards
    ],
)
def test_update_order_status_disallowed_transition_is_ignored(
    session, start_status, target_status
):
    order_id = _create_order_with_status(session=session, order_status=start_status)

    assert (
        update_order_status(
            session=session,
            order_id=order_id,
            order_status=target_status,
        )
        is False
    )

    assert get_order_status(session=session, order_id=order_id) == start_status


def test_update_order_status_same_status_is_idempotent(session):
    shared_status = OrderStatus.RUNNING
    order_id = _create_order_with_status(session=session, order_status=shared_status)

    assert (
        update_order_status(
            session=session,
            order_id=order_id,
            order_status=shared_status,
        )
        is True
    )

    assert get_order_status(session=session, order_id=order_id) == shared_status
