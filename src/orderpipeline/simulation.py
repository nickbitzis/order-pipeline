"""Seeded load and chaos run used by scripts/chaos_demo.py, the demo command, and tests."""

from __future__ import annotations

import json
import random
import time
import uuid
from dataclasses import dataclass, field

from fastapi.testclient import TestClient

from orderpipeline.api import create_app
from orderpipeline.config import Settings
from orderpipeline.db import CATALOG, connect
from orderpipeline.invariants import Check, check_all
from orderpipeline.queue.sqlite import SQLiteQueue
from orderpipeline.runner import Pipeline


def chaos_settings(db_path: str, seed: int = 42, **overrides: object) -> Settings:
    base: dict[str, object] = dict(
        db_path=db_path, seed=seed,
        provider_failure_rate=0.3, provider_latency_ms=2.0,
        max_attempts=5, visibility_timeout=1.0, backoff_base=0.05, backoff_max=0.5,
        poll_interval=0.02,
        crash_rate=0.03, duplicate_rate=0.05, drop_ack_rate=0.03,
    )
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


@dataclass
class RunReport:
    orders: int
    client_retries: int
    seconds: float
    idle: bool
    statuses: dict[str, int]
    outcomes: dict[str, int]
    dlq: int
    checks: list[Check] = field(default_factory=list)
    replayed: int = 0

    @property
    def ok(self) -> bool:
        return self.idle and all(c.ok for c in self.checks)


def place_orders(client: TestClient, n: int, seed: int, retry_rate: float = 0.1) -> int:
    """Post n orders. Some clients 'time out' and retry with the same key."""
    rng = random.Random(seed)
    skus = list(CATALOG)
    retries = 0
    for i in range(n):
        body = {
            "customer_id": f"cust-{rng.randint(1, 120):03d}",
            "sku": rng.choice(skus),
            "qty": rng.choices([1, 2, 3], weights=[70, 20, 10])[0],
        }
        headers = {"Idempotency-Key": f"key-{seed}-{i}",
                   "X-Correlation-ID": uuid.UUID(int=rng.getrandbits(128)).hex}
        r = client.post("/orders", json=body, headers=headers)
        r.raise_for_status()
        if rng.random() < retry_rate:
            r2 = client.post("/orders", json=body, headers=headers)
            assert r2.json()["order_id"] == r.json()["order_id"]
            retries += 1
    return retries


def run(s: Settings, n_orders: int, payments: int = 2, inventory: int = 2,
        timeout: float = 180.0) -> RunReport:
    t0 = time.time()
    app = create_app(s)
    pipe = Pipeline(s, payments=payments, inventory=inventory).start()
    try:
        with TestClient(app) as client:
            retries = place_orders(client, n_orders, s.seed)
        idle = pipe.wait_idle(timeout)
    finally:
        pipe.stop()
    statuses, outcomes, dlq, checks = snapshot(s)
    return RunReport(n_orders, retries, round(time.time() - t0, 1), idle, statuses,
                     outcomes, dlq, checks)


def snapshot(s: Settings) -> tuple[dict[str, int], dict[str, int], int, list[Check]]:
    conn = connect(s.db_path)
    q = SQLiteQueue(s.db_path, max_attempts=s.max_attempts)
    try:
        statuses = {r[0]: r[1] for r in conn.execute(
            "SELECT status, COUNT(*) FROM orders GROUP BY 1 ORDER BY 1")}
        outcomes: dict[str, int] = {}
        for r in conn.execute("SELECT labels, value FROM stats WHERE name = 'messages_total'"):
            lab = json.loads(r["labels"])
            outcomes[lab["outcome"]] = outcomes.get(lab["outcome"], 0) + int(r["value"])
        return statuses, dict(sorted(outcomes.items())), len(q.dead_letters(limit=100_000)), \
            check_all(conn, q)
    finally:
        conn.close()
        q.close()


def replay_dlq(s: Settings, timeout: float = 60.0) -> RunReport:
    """Operator fix: provider is healthy again, replay every dead letter."""
    t0 = time.time()
    healthy = Settings(**{**s.__dict__, "provider_failure_rate": 0.0, "crash_rate": 0.0,
                          "duplicate_rate": 0.0, "drop_ack_rate": 0.0})
    q = SQLiteQueue(s.db_path, max_attempts=s.max_attempts)
    pipe = Pipeline(healthy).start()
    try:
        n = q.replay()
        idle = pipe.wait_idle(timeout)
    finally:
        pipe.stop()
        q.close()
    statuses, outcomes, dlq, checks = snapshot(s)
    total = sum(statuses.values())
    return RunReport(total, 0, round(time.time() - t0, 1), idle, statuses, outcomes, dlq,
                     checks, replayed=n)
