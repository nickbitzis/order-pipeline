"""The invariant checker on a seeded chaos run, and on tampered data."""

from __future__ import annotations

import pytest

from orderpipeline.db import connect
from orderpipeline.invariants import check_all
from orderpipeline.queue.sqlite import SQLiteQueue
from orderpipeline.simulation import chaos_settings, replay_dlq, run


@pytest.fixture(scope="module")
def chaos_db(tmp_path_factory):
    db = str(tmp_path_factory.mktemp("chaos") / "chaos.db")
    s = chaos_settings(db, seed=11, crash_rate=0.05, duplicate_rate=0.1, drop_ack_rate=0.05)
    report = run(s, 120, timeout=60)
    return s, report


def _checks(db):
    conn, q = connect(db), SQLiteQueue(db)
    try:
        return {c.name: c for c in check_all(conn, q)}
    finally:
        conn.close()
        q.close()


def test_seeded_chaos_run_drains_and_holds_every_invariant(chaos_db):
    _, report = chaos_db
    assert report.idle
    failed = [f"{c.name}: {c.detail}" for c in report.checks if not c.ok]
    assert failed == []
    # chaos actually happened
    assert report.outcomes.get("retry", 0) > 0
    assert report.outcomes.get("duplicate", 0) > 0
    assert report.client_retries > 0


def test_replaying_the_dlq_settles_everything(chaos_db):
    s, _ = chaos_db
    r = replay_dlq(s)
    assert r.ok and r.dlq == 0
    assert set(r.statuses) <= {"COMPLETED", "REFUNDED"}


def test_checker_catches_a_double_charge(db_path, conn):
    conn.execute(
        "INSERT INTO orders VALUES ('o1','c','SKU-MUG',1,1200,'COMPLETED','x','k','h',0,0)")
    conn.execute("INSERT INTO payments VALUES ('o1','ch_1',1200,'charged',NULL,0)")
    for ref in ("ch_1", "ch_2"):
        conn.execute("INSERT INTO provider_ledger(kind, ref_id, order_id, idempotency_key,"
                     " amount_cents, at) VALUES ('charge', ?, 'o1', 'k', 1200, 0)", (ref,))
    checks = _checks(db_path)
    assert not checks["every paid order is charged exactly once"].ok
    assert not checks["totals reconcile"].ok


def test_checker_catches_an_order_stuck_without_refund_or_dlq(db_path, conn):
    conn.execute(
        "INSERT INTO orders VALUES ('o2','c','SKU-MUG',1,1200,'PENDING','x','k','h',0,0)")
    assert not _checks(db_path)["every failed order is refunded or in the DLQ"].ok


def test_checker_catches_stock_that_does_not_match_reservations(db_path, conn):
    conn.execute("UPDATE inventory SET stock = stock - 5 WHERE sku = 'SKU-TEE'")
    assert not _checks(db_path)["no stock goes negative"].ok


def test_database_refuses_negative_stock(conn):
    import sqlite3

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE inventory SET stock = -1 WHERE sku = 'SKU-MUG'")
