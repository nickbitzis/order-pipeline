"""Inventory worker: reserves stock, or starts compensation when it cannot."""

from __future__ import annotations

from typing import Any

from orderpipeline import saga
from orderpipeline.events import add_to_outbox, new_event
from orderpipeline.logs import log
from orderpipeline.queue.base import Delivery
from orderpipeline.services.worker import PoisonMessage, Worker, mark_processed


class InventoryWorker(Worker):
    service = "inventory"
    group = "inventory"
    topics = ("payment.captured",)

    def handle(self, event: dict[str, Any], d: Delivery) -> None:
        if event["type"] != "PaymentCaptured":
            raise PoisonMessage(f"inventory cannot handle {event['type']}")
        order_id = event["order_id"]
        sku, qty = event["data"]["sku"], event["data"]["qty"]
        with self.tx() as c:
            mark_processed(c, self.group, event["event_id"])
            # Conditional decrement: the check and the write are one statement,
            # so two workers can never both take the last unit.
            cur = c.execute(
                "UPDATE inventory SET stock = stock - ? WHERE sku = ? AND stock >= ?",
                (qty, sku, qty),
            )
            if cur.rowcount == 1:
                c.execute(
                    "INSERT INTO reservations(order_id, sku, qty, at)"
                    " VALUES (?, ?, ?, strftime('%s','now'))",
                    (order_id, sku, qty),
                )
                saga.transition(c, order_id, saga.COMPLETED, f"reserved {qty} x {sku}")
                add_to_outbox(c, new_event("OrderCompleted", order_id, event["correlation_id"],
                                           {"sku": sku, "qty": qty}))
                outcome = "reserved"
            else:
                # Compensating action: undo the earlier step (the charge).
                saga.transition(c, order_id, saga.REFUND_PENDING, f"out of stock: {sku}")
                add_to_outbox(c, new_event("RefundRequested", order_id, event["correlation_id"],
                                           {"reason": f"out of stock: {sku}"}))
                outcome = "out_of_stock"
        log(self.logger, "inventory decision", order_id=order_id, sku=sku, qty=qty,
            outcome=outcome)
