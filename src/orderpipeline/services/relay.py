"""Outbox relay: publishes committed outbox rows to the queue.

It publishes first and marks second. A crash between the two publishes the
row again on restart, so delivery is at-least-once and consumers dedupe.
Run exactly one relay per outbox table (replicas: 1).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time

from orderpipeline.chaos import Chaos
from orderpipeline.logs import correlation_id, log
from orderpipeline.queue.base import MessageQueue

logger = logging.getLogger("orderpipeline.relay")


class OutboxRelay:
    def __init__(
        self,
        conn: sqlite3.Connection,
        queue: MessageQueue,
        chaos: Chaos | None = None,
        batch_size: int = 100,
        poll_interval: float = 0.1,
    ) -> None:
        self.conn = conn
        self.queue = queue
        self.chaos = chaos or Chaos.off()
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.last_beat = time.time()

    def run_once(self) -> int:
        rows = self.conn.execute(
            "SELECT id, event_id, topic, payload, headers FROM outbox"
            " WHERE published_at IS NULL ORDER BY id LIMIT ?",
            (self.batch_size,),
        ).fetchall()
        for r in rows:
            payload = json.loads(r["payload"])
            headers = json.loads(r["headers"])
            token = correlation_id.set(headers.get("correlation_id", "-"))
            try:
                self.queue.publish(r["topic"], payload, key=payload.get("order_id"),
                                   headers=headers)
                self.chaos.maybe_crash("relay.after_publish", r["event_id"])
                self.conn.execute(
                    "UPDATE outbox SET published_at = ? WHERE id = ?", (time.time(), r["id"])
                )
                log(logger, "event published", topic=r["topic"], event_id=r["event_id"],
                    type=payload.get("type"))
            finally:
                correlation_id.reset(token)
        self.last_beat = time.time()
        return len(rows)

    def run_forever(self, stop: threading.Event) -> None:
        while not stop.is_set():
            if self.run_once() == 0:
                stop.wait(self.poll_interval)
