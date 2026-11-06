"""A fake payment provider that fails at random.

It keeps its own ledger table so the invariant checker can count real charges.
Like most real providers it honors an idempotency key: a second charge with the
same key returns the first charge instead of taking money twice. That can be
switched off to show what the consumer's own dedupe table protects against.
"""

from __future__ import annotations

import random
import sqlite3
import threading
import time
import uuid
from collections import Counter

from orderpipeline.db import transaction


class ProviderError(Exception):
    """A transient failure (timeout, 503). Safe to retry."""


class FakePaymentProvider:
    # Shared by every instance in the process, like one remote service seen by
    # many workers. The n-th call for a key gets the same outcome on every run.
    _calls: Counter[str] = Counter()
    _lock = threading.Lock()

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        failure_rate: float = 0.0,
        latency_ms: float = 0.0,
        seed: int = 7,
        honor_idempotency_key: bool = True,
    ) -> None:
        self.conn = conn
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        self.seed = seed
        self.honor_idempotency_key = honor_idempotency_key

    def _call(self, op: str, key: str) -> None:
        with self._lock:
            self._calls[f"{self.seed}:{op}:{key}"] += 1
            n = self._calls[f"{self.seed}:{op}:{key}"]
        rng = random.Random(f"{self.seed}:{op}:{key}:{n}")
        if self.latency_ms:
            time.sleep(rng.uniform(0.5, 1.5) * self.latency_ms / 1000)
        if rng.random() < self.failure_rate:
            raise ProviderError(f"{op} failed: provider returned 503")

    def _write(self, kind: str, order_id: str, amount: int, key: str) -> str:
        with transaction(self.conn) as c:
            if self.honor_idempotency_key:
                row = c.execute(
                    "SELECT ref_id FROM provider_ledger WHERE kind = ? AND idempotency_key = ?",
                    (kind, key),
                ).fetchone()
                if row:
                    return row["ref_id"]
            ref = f"{kind[:2]}_{uuid.uuid4().hex[:12]}"
            c.execute(
                "INSERT INTO provider_ledger(kind, ref_id, order_id, idempotency_key,"
                " amount_cents, at) VALUES (?, ?, ?, ?, ?, ?)",
                (kind, ref, order_id, key, amount, time.time()),
            )
            return ref

    def charge(self, order_id: str, amount_cents: int, idempotency_key: str) -> str:
        self._call("charge", idempotency_key)
        return self._write("charge", order_id, amount_cents, idempotency_key)

    def refund(self, order_id: str, amount_cents: int, idempotency_key: str) -> str:
        self._call("refund", idempotency_key)
        return self._write("refund", order_id, amount_cents, idempotency_key)
