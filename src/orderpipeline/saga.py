"""Order saga states and the only transitions allowed between them.

    PENDING --charge ok--> PAID --stock reserved--> COMPLETED
       |                    |
       | retries exhausted  +--out of stock--> REFUND_PENDING --refund ok--> REFUNDED
       v
    PAYMENT_FAILED (message in DLQ) --replayed and charged--> PAID
"""

from __future__ import annotations

import sqlite3
import time

PENDING = "PENDING"
PAID = "PAID"
COMPLETED = "COMPLETED"
PAYMENT_FAILED = "PAYMENT_FAILED"
REFUND_PENDING = "REFUND_PENDING"
REFUNDED = "REFUNDED"

ALLOWED = {
    PENDING: {PAID, PAYMENT_FAILED},
    PAYMENT_FAILED: {PAID},
    PAID: {COMPLETED, REFUND_PENDING},
    REFUND_PENDING: {REFUNDED},
    COMPLETED: set(),
    REFUNDED: set(),
}
TERMINAL = {COMPLETED, REFUNDED}


class InvalidTransition(Exception):
    pass


def transition(conn: sqlite3.Connection, order_id: str, to_state: str, reason: str) -> str:
    """Move an order to `to_state` inside the caller's transaction. Returns the old state."""
    row = conn.execute("SELECT status FROM orders WHERE id = ?", (order_id,)).fetchone()
    if row is None:
        raise InvalidTransition(f"unknown order {order_id}")
    current = row["status"]
    if to_state not in ALLOWED[current]:
        raise InvalidTransition(f"{order_id}: {current} -> {to_state} not allowed")
    now = time.time()
    conn.execute(
        "UPDATE orders SET status = ?, updated_at = ? WHERE id = ?", (to_state, now, order_id)
    )
    conn.execute(
        "INSERT INTO saga_log(order_id, from_state, to_state, reason, at) VALUES (?, ?, ?, ?, ?)",
        (order_id, current, to_state, reason, now),
    )
    return current


def history(conn: sqlite3.Connection, order_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT from_state, to_state, reason, at FROM saga_log WHERE order_id = ? ORDER BY id",
        (order_id,),
    ).fetchall()
    return [dict(r) for r in rows]
