"""Shared consumer loop: dedupe, retry with backoff, dead-lettering, metrics."""

from __future__ import annotations

import logging
import random
import sqlite3
import threading
import time
from typing import Any

from orderpipeline.chaos import Chaos, SimulatedCrash
from orderpipeline.config import Settings
from orderpipeline.db import transaction
from orderpipeline.logs import correlation_id, log
from orderpipeline.metrics import Stats
from orderpipeline.queue.base import Delivery, MessageQueue


class RetryableError(Exception):
    """Try again later with backoff."""


class PoisonMessage(Exception):
    """Can never succeed. Send straight to the DLQ."""


class AlreadyProcessed(Exception):
    """Raised inside a handler transaction when another delivery won the race."""


def already_processed(conn: sqlite3.Connection, consumer: str, event_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM processed_events WHERE consumer = ? AND event_id = ?", (consumer, event_id)
    ).fetchone()
    return row is not None


def mark_processed(conn: sqlite3.Connection, consumer: str, event_id: str) -> None:
    """Insert into the dedupe table inside the handler's transaction.

    The primary key makes this fail if the event was already processed, which
    rolls back the whole unit of work. So side effects and the dedupe record
    commit together or not at all.
    """
    try:
        conn.execute(
            "INSERT INTO processed_events(consumer, event_id, processed_at) VALUES (?, ?, ?)",
            (consumer, event_id, time.time()),
        )
    except sqlite3.IntegrityError as e:
        raise AlreadyProcessed(event_id) from e


def backoff_delay(attempts: int, base: float, cap: float, rng: random.Random) -> float:
    """Exponential backoff with full jitter."""
    return rng.uniform(0, min(cap, base * 2 ** (attempts - 1)))


class Worker:
    service = "worker"
    group = "worker"
    topics: tuple[str, ...] = ()

    def __init__(
        self,
        conn: sqlite3.Connection,
        queue: MessageQueue,
        settings: Settings,
        chaos: Chaos | None = None,
    ) -> None:
        self.conn = conn
        self.queue = queue
        self.settings = settings
        self.chaos = chaos or Chaos.off()
        self.stats = Stats(conn)
        self.logger = logging.getLogger(f"orderpipeline.{self.service}")
        self.rng = random.Random(settings.seed)
        self.last_beat = time.time()

    # subclasses implement this; it must call mark_processed in its transaction
    def handle(self, event: dict[str, Any], delivery: Delivery) -> None:
        raise NotImplementedError

    def on_exhausted(self, event: dict[str, Any], error: str) -> None:
        """Called on the final failed attempt, before the message is dead-lettered."""

    def run_once(self) -> int:
        n = 0
        for topic in self.topics:
            for d in self.queue.receive(
                topic, self.group, max_messages=1,
                visibility_timeout=self.settings.visibility_timeout,
            ):
                self.process(d)
                n += 1
        self.last_beat = time.time()
        return n

    def run_forever(self, stop: threading.Event) -> None:
        while not stop.is_set():
            if self.run_once() == 0:
                stop.wait(self.settings.poll_interval)

    def process(self, d: Delivery) -> None:
        cid = d.message.headers.get("correlation_id", "-")
        token = correlation_id.set(cid)
        t0 = time.perf_counter()
        topic = d.message.topic
        outcome = "ok"
        try:
            event = d.message.payload
            if not isinstance(event, dict) or "event_id" not in event or "type" not in event:
                raise PoisonMessage("payload is not a valid event envelope")
            if already_processed(self.conn, self.group, event["event_id"]):
                outcome = "duplicate"
                log(self.logger, "duplicate delivery skipped", event_id=event["event_id"],
                    message_id=d.message.id, attempt=d.attempts)
            else:
                try:
                    self.handle(event, d)
                except AlreadyProcessed:
                    outcome = "duplicate"
            self.chaos.maybe_crash(f"{self.service}.before_ack", d.message.id, d.attempts)
            self.queue.ack(d)
        except SimulatedCrash:
            outcome = "crashed"
            log(self.logger, "worker crashed mid-message", logging.WARNING,
                message_id=d.message.id, attempt=d.attempts)
            raise
        except PoisonMessage as e:
            outcome = "dead_lettered"
            self.queue.nack(d, error=f"poison: {e}", dead_letter=True)
            log(self.logger, "poison message dead-lettered", logging.ERROR,
                message_id=d.message.id, error=str(e))
        except RetryableError as e:
            final = d.attempts >= self.queue.max_attempts
            if final:
                outcome = "dead_lettered"
                self.on_exhausted(d.message.payload, str(e))
                self.queue.nack(d, error=str(e))
                log(self.logger, "retries exhausted, dead-lettered", logging.ERROR,
                    message_id=d.message.id, attempt=d.attempts, error=str(e))
            else:
                outcome = "retry"
                delay = backoff_delay(
                    d.attempts, self.settings.backoff_base, self.settings.backoff_max, self.rng
                )
                self.queue.nack(d, delay=delay, error=str(e))
                log(self.logger, "handler failed, will retry", logging.WARNING,
                    message_id=d.message.id, attempt=d.attempts, delay_s=round(delay, 3),
                    error=str(e))
        finally:
            self.stats.record(self.service, topic, outcome, time.perf_counter() - t0)
            correlation_id.reset(token)

    def tx(self):
        return transaction(self.conn)
