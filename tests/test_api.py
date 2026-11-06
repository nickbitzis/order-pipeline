"""Orders API: idempotency keys, outbox atomicity, health, metrics."""

from __future__ import annotations

import json

import pytest
from helpers import client_for, place

from orderpipeline import api as api_module


def count(conn, sql):
    return conn.execute(sql).fetchone()[0]


def test_create_order_writes_order_and_outbox_event(settings, conn):
    c = client_for(settings)
    r = place(c)
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "PENDING" and body["amount_cents"] == 1200
    row = conn.execute("SELECT topic, payload FROM outbox").fetchone()
    assert row["topic"] == "order.placed"
    assert json.loads(row["payload"])["order_id"] == body["order_id"]


def test_same_idempotency_key_returns_same_order_without_duplicate(settings, conn):
    c = client_for(settings)
    r1 = place(c, key="same")
    r2 = place(c, key="same")
    assert r2.status_code == 200
    assert r2.headers["Idempotent-Replayed"] == "true"
    assert r1.json()["order_id"] == r2.json()["order_id"]
    assert count(conn, "SELECT COUNT(*) FROM orders") == 1
    assert count(conn, "SELECT COUNT(*) FROM outbox") == 1


def test_reused_key_with_different_body_is_rejected(settings, conn):
    c = client_for(settings)
    place(c, key="k", qty=1)
    r = place(c, key="k", qty=2)
    assert r.status_code == 422
    assert count(conn, "SELECT COUNT(*) FROM orders") == 1


def test_missing_idempotency_key_is_rejected(settings):
    c = client_for(settings)
    r = c.post("/orders", json={"customer_id": "c", "sku": "SKU-MUG", "qty": 1})
    assert r.status_code == 400


def test_unknown_sku_is_rejected_and_writes_nothing(settings, conn):
    r = place(client_for(settings), sku="SKU-NOPE")
    assert r.status_code == 422
    assert count(conn, "SELECT COUNT(*) FROM orders") == 0


def test_outbox_write_failure_rolls_back_the_order(settings, conn, monkeypatch):
    def broken(*_a, **_k):
        raise RuntimeError("disk full")

    monkeypatch.setattr(api_module, "add_to_outbox", broken)
    c = client_for(settings, raise_server_exceptions=False)
    r = place(c)
    assert r.status_code == 500
    assert count(conn, "SELECT COUNT(*) FROM orders") == 0
    assert count(conn, "SELECT COUNT(*) FROM outbox") == 0
    assert count(conn, "SELECT COUNT(*) FROM saga_log") == 0


def test_correlation_id_flows_from_request_into_event_headers(settings, conn):
    c = client_for(settings)
    r = place(c, cid="trace-abc")
    assert r.headers["x-correlation-id"] == "trace-abc"
    headers = json.loads(conn.execute("SELECT headers FROM outbox").fetchone()[0])
    assert headers["correlation_id"] == "trace-abc"


def test_get_order_includes_saga_history(settings):
    c = client_for(settings)
    oid = place(c).json()["order_id"]
    o = c.get(f"/orders/{oid}").json()
    assert [h["to_state"] for h in o["saga"]] == ["PENDING"]
    assert c.get("/orders/ord_missing").status_code == 404


def test_healthz(settings):
    r = client_for(settings).get("/healthz")
    assert r.status_code == 200 and r.json() == {"status": "ok"}


@pytest.mark.parametrize("metric", [
    "orderpipeline_queue_depth", "orderpipeline_dlq_size", "orderpipeline_outbox_pending 1",
    'orderpipeline_orders{status="PENDING"} 1',
])
def test_metrics_exposes_pipeline_state(settings, metric):
    c = client_for(settings)
    place(c)
    text = c.get("/metrics").text
    assert metric in text
