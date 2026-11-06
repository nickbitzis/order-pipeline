"""Payments worker: charges on OrderPlaced, refunds on RefundRequested."""

from __future__ import annotations

import logging
from typing import Any

from orderpipeline import saga
from orderpipeline.events import add_to_outbox, new_event
from orderpipeline.logs import log
from orderpipeline.provider import FakePaymentProvider, ProviderError
from orderpipeline.queue.base import Delivery
from orderpipeline.services.worker import PoisonMessage, RetryableError, Worker, mark_processed


class PaymentsWorker(Worker):
    service = "payments"
    group = "payments"
    topics = ("order.placed", "refund.requested")

    def __init__(self, *args: Any, provider: FakePaymentProvider | None = None, **kw: Any):
        super().__init__(*args, **kw)
        s = self.settings
        self.provider = provider or FakePaymentProvider(
            self.conn, failure_rate=s.provider_failure_rate, latency_ms=s.provider_latency_ms,
            seed=s.seed,
        )

    def handle(self, event: dict[str, Any], delivery: Delivery) -> None:
        if event["type"] == "OrderPlaced":
            self.charge(event, delivery)
        elif event["type"] == "RefundRequested":
            self.refund(event, delivery)
        else:
            raise PoisonMessage(f"payments cannot handle {event['type']}")

    def _order(self, order_id: str):
        row = self.conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            raise PoisonMessage(f"unknown order {order_id}")
        return row

    def charge(self, event: dict[str, Any], d: Delivery) -> None:
        order = self._order(event["order_id"])
        if order["status"] not in (saga.PENDING, saga.PAYMENT_FAILED):
            with self.tx() as c:
                mark_processed(c, self.group, event["event_id"])
            return
        try:
            # Keyed on the order, so even two different events for one order
            # cannot charge twice at a provider that honors idempotency keys.
            charge_id = self.provider.charge(
                order["id"], order["amount_cents"], idempotency_key=f"charge:{order['id']}"
            )
        except ProviderError as e:
            raise RetryableError(str(e)) from e
        # The dangerous window: money moved, our database does not know yet.
        self.chaos.maybe_crash("payments.after_charge", d.message.id, d.attempts)
        with self.tx() as c:
            mark_processed(c, self.group, event["event_id"])
            c.execute(
                "INSERT INTO payments(order_id, charge_id, amount_cents, status, updated_at)"
                " VALUES (?, ?, ?, 'charged', strftime('%s','now'))",
                (order["id"], charge_id, order["amount_cents"]),
            )
            saga.transition(c, order["id"], saga.PAID, f"charged {charge_id}")
            add_to_outbox(c, new_event(
                "PaymentCaptured", order["id"], event["correlation_id"],
                {"charge_id": charge_id, "sku": order["sku"], "qty": order["qty"]},
            ))
        log(self.logger, "payment captured", order_id=order["id"], charge_id=charge_id,
            attempt=d.attempts)

    def on_exhausted(self, event: dict[str, Any], error: str) -> None:
        if event.get("type") != "OrderPlaced":
            return
        with self.tx() as c:
            row = c.execute("SELECT status FROM orders WHERE id = ?",
                            (event["order_id"],)).fetchone()
            if row and row["status"] == saga.PENDING:
                saga.transition(c, event["order_id"], saga.PAYMENT_FAILED,
                                f"retries exhausted: {error}")

    def refund(self, event: dict[str, Any], d: Delivery) -> None:
        pay = self.conn.execute(
            "SELECT * FROM payments WHERE order_id = ?", (event["order_id"],)
        ).fetchone()
        if pay is None:
            raise PoisonMessage(f"no payment to refund for {event['order_id']}")
        if pay["status"] == "refunded":
            with self.tx() as c:
                mark_processed(c, self.group, event["event_id"])
            return
        try:
            refund_id = self.provider.refund(
                pay["order_id"], pay["amount_cents"], idempotency_key=f"refund:{pay['order_id']}"
            )
        except ProviderError as e:
            raise RetryableError(str(e)) from e
        self.chaos.maybe_crash("payments.after_refund", d.message.id, d.attempts)
        with self.tx() as c:
            mark_processed(c, self.group, event["event_id"])
            c.execute(
                "UPDATE payments SET status = 'refunded', refund_id = ?,"
                " updated_at = strftime('%s','now') WHERE order_id = ?",
                (refund_id, pay["order_id"]),
            )
            saga.transition(c, pay["order_id"], saga.REFUNDED, f"refunded {refund_id}")
            add_to_outbox(c, new_event(
                "OrderCancelled", pay["order_id"], event["correlation_id"],
                {"reason": event["data"].get("reason", ""), "refund_id": refund_id},
            ))
        log(self.logger, "payment refunded", logging.INFO, order_id=pay["order_id"],
            refund_id=refund_id)
