from __future__ import annotations

import click

from ogdc_runner.service.db import get_db_session
from ogdc_runner.service.orders import OrderStatus, update_order_status

if __name__ == "__main__":

    @click.command()
    @click.option("--order-id", required=True, type=str)
    @click.option("--workflow-status", required=True, type=str)
    def update_order_cli(
        order_id: str,
        workflow_status: str,
    ) -> None:

        try:
            order_status = OrderStatus(workflow_status)
        except ValueError:
            order_status = OrderStatus.UNKNOWN

        with get_db_session() as session:
            update_order_status(
                order_id=order_id,
                order_status=order_status,
                session=session,
            )

    update_order_cli()
