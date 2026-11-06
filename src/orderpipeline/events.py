"""Event envelopes, topics, and the transactional outbox write."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any

TOPICS = {
    "OrderPlaced": "order.placed",
    "PaymentCaptured": "payment.captured",
    "RefundRequested": "refund.requested",
    "OrderCompleted": "order.completed",
    "OrderCancelled": "order.cancelled",
}

# topic -> consumer groups. order.completed and order.cancelled have no
# consumers in this demo; they are kept in the log for audit and replay.
SUBSCRIPTIONS = {
    "order.placed": ["payments"],
    "payment.captured": ["inventory"],
    "refund.requested": ["payments"],
}


def new_event(
    event_type: str, order_id: str, correlation_id: str, data: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "event_id": uuid.uuid4().hex,
        "type": event_type,
        "order_id": order_id,
        "correlation_id": correlation_id,
        "occurred_at": time.time(),
        "data": data or {},
    }


def add_to_outbox(conn: sqlite3.Connection, event: dict[str, Any]) -> None:
    """Write an event in the caller's open transaction.

    The event commits or rolls back together with the state change that caused
    it. A separate relay publishes it later. This is the transactional outbox.
    """
    headers = {"event_id": event["event_id"], "correlation_id": event["correlation_id"]}
    conn.execute(
        "INSERT INTO outbox(event_id, topic, payload, headers, created_at) VALUES (?, ?, ?, ?, ?)",
        (
            event["event_id"],
            TOPICS[event["type"]],
            json.dumps(event),
            json.dumps(headers),
            time.time(),
        ),
    )
