"""Invariant checks that must hold after any run, chaos or not."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from orderpipeline import saga
from orderpipeline.queue.base import MessageQueue


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


def check_all(conn: sqlite3.Connection, queue: MessageQueue) -> list[Check]:
    out: list[Check] = []
    dlq_orders = {
        dl.message.payload.get("order_id")
        for dl in queue.dead_letters(limit=100_000)
        if isinstance(dl.message.payload, dict)
    }
    orders = {r["id"]: r for r in conn.execute("SELECT * FROM orders")}

    # 1. charged exactly once
    charges: dict[str, int] = {}
    for r in conn.execute(
        "SELECT order_id, COUNT(*) AS n FROM provider_ledger WHERE kind = 'charge' GROUP BY 1"
    ):
        charges[r["order_id"]] = r["n"]
    paid = [r["order_id"] for r in conn.execute("SELECT order_id FROM payments")]
    multi = [o for o, n in charges.items() if n > 1]
    missing = [o for o in paid if charges.get(o, 0) != 1]
    orphans = [o for o in charges if o not in set(paid) and o not in dlq_orders]
    out.append(Check(
        "every paid order is charged exactly once",
        not multi and not missing and not orphans,
        f"{len(paid)} paid orders, {sum(charges.values())} provider charges, "
        f"{len(multi)} charged twice, {len(orphans)} charges with no payment record",
    ))

    # 2. stock never negative and reconciles with reservations
    neg = conn.execute("SELECT COUNT(*) FROM inventory WHERE stock < 0").fetchone()[0]
    drift = conn.execute(
        "SELECT i.sku, i.initial_stock - i.stock AS used,"
        " COALESCE((SELECT SUM(qty) FROM reservations r WHERE r.sku = i.sku), 0) AS reserved"
        " FROM inventory i"
    ).fetchall()
    bad_sku = [r["sku"] for r in drift if r["used"] != r["reserved"]]
    left = {r["sku"]: r["stock"] for r in conn.execute("SELECT sku, stock FROM inventory")}
    out.append(Check(
        "no stock goes negative",
        neg == 0 and not bad_sku,
        f"min stock {min(left.values()) if left else 0}, "
        f"{len(bad_sku)} SKUs where stock used != reserved",
    ))

    # 3. every order that did not complete is refunded or in the DLQ
    refunded = {r["order_id"] for r in conn.execute(
        "SELECT order_id FROM provider_ledger WHERE kind = 'refund'")}
    unresolved, parked = [], []
    for oid, o in orders.items():
        if o["status"] == saga.COMPLETED:
            continue
        if o["status"] == saga.REFUNDED and oid in refunded:
            continue
        (parked if oid in dlq_orders else unresolved).append(oid)
    n_failed = sum(1 for o in orders.values() if o["status"] != saga.COMPLETED)
    out.append(Check(
        "every failed order is refunded or in the DLQ",
        not unresolved,
        f"{n_failed} orders did not complete: "
        f"{sum(1 for o in orders.values() if o['status'] == saga.REFUNDED)} refunded, "
        f"{len(parked)} in DLQ, {len(unresolved)} unresolved",
    ))

    # 4. money reconciles: provider net == value of completed orders, plus money
    #    whose next step is parked in the DLQ (a charge with no payment record, or
    #    a refund that ran out of retries). Replaying the DLQ settles both.
    led = {r["kind"]: r["s"] for r in conn.execute(
        "SELECT kind, COALESCE(SUM(amount_cents), 0) AS s FROM provider_ledger GROUP BY kind")}
    net = led.get("charge", 0) - led.get("refund", 0)
    completed = conn.execute(
        "SELECT COALESCE(SUM(amount_cents), 0) FROM orders WHERE status = ?", (saga.COMPLETED,)
    ).fetchone()[0]
    stranded = conn.execute(
        "SELECT COALESCE(SUM(l.amount_cents), 0) FROM provider_ledger l"
        " LEFT JOIN payments p ON p.order_id = l.order_id"
        " WHERE l.kind = 'charge' AND p.order_id IS NULL"
    ).fetchone()[0]
    owed = conn.execute(
        "SELECT COALESCE(SUM(o.amount_cents), 0) FROM orders o WHERE o.status = ?"
        " AND NOT EXISTS (SELECT 1 FROM provider_ledger l"
        " WHERE l.kind = 'refund' AND l.order_id = o.id)",
        (saga.REFUND_PENDING,),
    ).fetchone()[0]
    out.append(Check(
        "totals reconcile",
        net == completed + stranded + owed,
        f"charged {led.get('charge', 0) / 100:.2f}, refunded {led.get('refund', 0) / 100:.2f}, "
        f"net {net / 100:.2f} = completed {completed / 100:.2f}"
        f" + parked in DLQ {(stranded + owed) / 100:.2f}",
    ))

    # 5. one OrderPlaced event per order (outbox atomicity + idempotency keys)
    placed = conn.execute(
        "SELECT COUNT(*) FROM outbox WHERE topic = 'order.placed'").fetchone()[0]
    out.append(Check(
        "one OrderPlaced event per order",
        placed == len(orders),
        f"{len(orders)} orders, {placed} OrderPlaced events",
    ))
    return out


def format_checks(checks: list[Check]) -> str:
    lines = [f"[{'PASS' if c.ok else 'FAIL'}] {c.name}: {c.detail}" for c in checks]
    return "\n".join(lines)
