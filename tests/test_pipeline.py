"""Relay, idempotent consumers, retries, DLQ, saga compensation, crash recovery."""

from __future__ import annotations

import json
import logging

import pytest
from helpers import client_for, place

from orderpipeline import saga
from orderpipeline.chaos import Chaos, ChaosQueue, SimulatedCrash
from orderpipeline.logs import JsonFormatter, correlation_id
from orderpipeline.provider import FakePaymentProvider
from orderpipeline.queue.sqlite import SQLiteQueue
from orderpipeline.services.inventory import InventoryWorker
from orderpipeline.services.payments import PaymentsWorker
from orderpipeline.services.relay import OutboxRelay


@pytest.fixture
def q(db_path, clock):
    q = SQLiteQueue(db_path, max_attempts=3, clock=clock)
    yield q
    q.close()


def charges(conn, order_id=None):
    sql = "SELECT COUNT(*) FROM provider_ledger WHERE kind = 'charge'"
    if order_id:
        return conn.execute(sql + " AND order_id = ?", (order_id,)).fetchone()[0]
    return conn.execute(sql).fetchone()[0]


def status(conn, oid):
    return conn.execute("SELECT status FROM orders WHERE id = ?", (oid,)).fetchone()[0]


def drain(conn, q, settings, payments=None, rounds=20):
    relay = OutboxRelay(conn, q)
    pay = payments or PaymentsWorker(conn, q, settings)
    inv = InventoryWorker(conn, q, settings)
    for _ in range(rounds):
        n = relay.run_once() + pay.run_once() + inv.run_once()
        if n == 0:
            break


def test_relay_publishes_once_and_marks_rows(settings, conn, q):
    place(client_for(settings))
    relay = OutboxRelay(conn, q)
    assert relay.run_once() == 1
    assert relay.run_once() == 0
    assert conn.execute("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL").fetchone()[0] == 0
    assert len(q.receive("order.placed", "payments")) == 1


def test_relay_crash_after_publish_republishes_on_restart(settings, conn, q):
    place(client_for(settings))
    with pytest.raises(SimulatedCrash):
        OutboxRelay(conn, q, Chaos(crash_rate=1.0)).run_once()
    OutboxRelay(conn, q).run_once()
    # At-least-once: the event is on the queue twice. Consumers must dedupe.
    assert len(q.receive("order.placed", "payments", max_messages=10)) == 2


def test_happy_path_completes_order(settings, conn, q):
    oid = place(client_for(settings)).json()["order_id"]
    drain(conn, q, settings)
    assert status(conn, oid) == saga.COMPLETED
    assert charges(conn, oid) == 1
    assert conn.execute("SELECT stock FROM inventory WHERE sku='SKU-MUG'").fetchone()[0] == 199


def test_duplicate_delivery_never_double_charges(settings, conn, q):
    """Provider idempotency is OFF here, so only the consumer's dedupe table protects us."""
    place(client_for(settings))
    OutboxRelay(conn, q).run_once()
    event = json.loads(conn.execute("SELECT payload FROM outbox").fetchone()[0])
    for _ in range(3):  # the broker hands the same event over three more times
        q.publish("order.placed", event, headers={"correlation_id": "x"})
    provider = FakePaymentProvider(conn, honor_idempotency_key=False)
    worker = PaymentsWorker(conn, q, settings, provider=provider)
    while worker.run_once():
        pass
    assert charges(conn) == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM processed_events WHERE consumer='payments'").fetchone()[0] == 1


def test_without_dedupe_a_redelivery_would_double_charge(conn):
    """The control case: a provider with no idempotency, called twice, charges twice."""
    p = FakePaymentProvider(conn, honor_idempotency_key=False)
    p.charge("o1", 100, "charge:o1")
    p.charge("o1", 100, "charge:o1")
    assert charges(conn, "o1") == 2


def test_transient_failure_is_retried_with_backoff(settings, conn, q, clock):
    oid = place(client_for(settings)).json()["order_id"]
    OutboxRelay(conn, q).run_once()
    provider = FakePaymentProvider(conn, failure_rate=1.0)
    w = PaymentsWorker(conn, q, settings, provider=provider)
    w.run_once()
    assert status(conn, oid) == saga.PENDING
    row = conn.execute("SELECT state, attempts, visible_at FROM q_deliveries").fetchone()
    assert row["state"] == "ready" and row["attempts"] == 1 and row["visible_at"] >= clock()
    provider.failure_rate = 0.0
    clock.advance(1)
    w.run_once()
    assert status(conn, oid) == saga.PAID


def test_retries_exhausted_go_to_dlq_and_mark_payment_failed(settings, conn, q, clock):
    oid = place(client_for(settings)).json()["order_id"]
    OutboxRelay(conn, q).run_once()
    w = PaymentsWorker(conn, q, settings, provider=FakePaymentProvider(conn, failure_rate=1.0))
    for _ in range(3):
        w.run_once()
        clock.advance(10)
    assert status(conn, oid) == saga.PAYMENT_FAILED
    [dl] = q.dead_letters()
    assert dl.message.payload["order_id"] == oid and dl.attempts == 3
    assert charges(conn) == 0


def test_dlq_replay_after_recovery_completes_the_order(settings, conn, q, clock):
    oid = place(client_for(settings)).json()["order_id"]
    OutboxRelay(conn, q).run_once()
    provider = FakePaymentProvider(conn, failure_rate=1.0)
    w = PaymentsWorker(conn, q, settings, provider=provider)
    for _ in range(3):
        w.run_once()
        clock.advance(10)
    provider.failure_rate = 0.0
    assert q.replay() == 1
    drain(conn, q, settings, payments=w)
    assert status(conn, oid) == saga.COMPLETED
    states = [h["to_state"] for h in saga.history(conn, oid)]
    assert states == ["PENDING", "PAYMENT_FAILED", "PAID", "COMPLETED"]


def test_poison_message_goes_straight_to_dlq(settings, conn, q):
    q.publish("order.placed", {"not": "an event"})
    PaymentsWorker(conn, q, settings).run_once()
    [dl] = q.dead_letters()
    assert dl.attempts == 1 and dl.last_error.startswith("poison")


def test_crash_after_charge_does_not_double_charge_on_redelivery(settings, conn, q, clock):
    oid = place(client_for(settings)).json()["order_id"]
    OutboxRelay(conn, q).run_once()
    crashing = PaymentsWorker(conn, q, settings, chaos=Chaos(crash_rate=1.0))
    with pytest.raises(SimulatedCrash):
        crashing.run_once()
    assert charges(conn, oid) == 1 and status(conn, oid) == saga.PENDING
    clock.advance(settings.visibility_timeout + 1)  # lease expires, message comes back
    PaymentsWorker(conn, q, settings).run_once()
    assert status(conn, oid) == saga.PAID
    assert charges(conn, oid) == 1  # provider idempotency key caught the retry


def test_lost_ack_redelivery_is_skipped_by_dedupe(settings, conn, q, clock):
    oid = place(client_for(settings)).json()["order_id"]
    OutboxRelay(conn, q).run_once()
    lossy = ChaosQueue(q, Chaos(drop_ack_rate=1.0))
    PaymentsWorker(conn, lossy, settings).run_once()
    clock.advance(settings.visibility_timeout + 1)
    PaymentsWorker(conn, q, settings).run_once()
    outcomes = dict(conn.execute(
        "SELECT json_extract(labels, '$.outcome'), value FROM stats"
        " WHERE name = 'messages_total'").fetchall())
    assert outcomes == {"ok": 1, "duplicate": 1}
    assert charges(conn, oid) == 1


def test_out_of_stock_triggers_refund_saga(settings, conn, q):
    conn.execute("UPDATE inventory SET stock = 1 WHERE sku = 'SKU-BAG'")
    c = client_for(settings)
    a = place(c, key="a", sku="SKU-BAG").json()["order_id"]
    b = place(c, key="b", sku="SKU-BAG").json()["order_id"]
    drain(conn, q, settings)
    assert status(conn, a) == saga.COMPLETED
    assert status(conn, b) == saga.REFUNDED
    states = [h["to_state"] for h in saga.history(conn, b)]
    assert states == ["PENDING", "PAID", "REFUND_PENDING", "REFUNDED"]
    refunds = conn.execute(
        "SELECT COUNT(*) FROM provider_ledger WHERE kind='refund' AND order_id=?", (b,)
    ).fetchone()[0]
    assert refunds == 1
    assert conn.execute("SELECT stock FROM inventory WHERE sku='SKU-BAG'").fetchone()[0] == 0


def test_inventory_never_oversells(settings, conn, q):
    conn.execute("UPDATE inventory SET stock = 3 WHERE sku = 'SKU-CAP'")
    c = client_for(settings)
    ids = [place(c, key=f"k{i}", sku="SKU-CAP", qty=2).json()["order_id"] for i in range(4)]
    drain(conn, q, settings)
    done = [i for i in ids if status(conn, i) == saga.COMPLETED]
    assert len(done) == 1
    assert conn.execute("SELECT stock FROM inventory WHERE sku='SKU-CAP'").fetchone()[0] == 1


def test_invalid_saga_transition_is_refused(settings, conn):
    oid = place(client_for(settings)).json()["order_id"]
    with pytest.raises(saga.InvalidTransition):
        saga.transition(conn, oid, saga.COMPLETED, "skip payment")


def test_json_log_carries_correlation_id():
    rec = logging.LogRecord("orderpipeline.x", logging.INFO, "f", 1, "hello", None, None)
    rec.fields = {"order_id": "o1"}
    token = correlation_id.set("cid-1")
    try:
        out = json.loads(JsonFormatter().format(rec))
    finally:
        correlation_id.reset(token)
    assert out["correlation_id"] == "cid-1" and out["order_id"] == "o1" and out["msg"] == "hello"
