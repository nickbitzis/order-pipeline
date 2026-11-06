"""Fault injection: worker crashes, duplicate publishes, and lost acks.

Every decision is a hash of (seed, point, key), not a shared random stream, so
the same seed makes the same decisions no matter how threads interleave.
"""

from __future__ import annotations

import random
import threading
from collections import Counter
from typing import Any

from orderpipeline.queue.base import Delivery, MessageQueue


class SimulatedCrash(Exception):
    """Raised at a crash point. The message is left unacked, like a killed process."""


class Chaos:
    def __init__(
        self,
        seed: int = 7,
        crash_rate: float = 0.0,
        duplicate_rate: float = 0.0,
        drop_ack_rate: float = 0.0,
    ) -> None:
        self.seed = seed
        self.crash_rate = crash_rate
        self.duplicate_rate = duplicate_rate
        self.drop_ack_rate = drop_ack_rate
        self._seen: Counter[tuple] = Counter()
        self._lock = threading.Lock()

    def roll(self, *parts: object) -> float:
        return random.Random(f"{self.seed}:" + ":".join(map(str, parts))).random()

    def maybe_crash(self, point: str, *key: object) -> None:
        if not self.crash_rate:
            return
        # Count visits so a restarted service does not crash on the same spot forever.
        with self._lock:
            self._seen[(point, *key)] += 1
            n = self._seen[(point, *key)]
        if self.roll("crash", point, *key, n) < self.crash_rate:
            raise SimulatedCrash(point)

    @classmethod
    def off(cls) -> Chaos:
        return cls()


class ChaosQueue:
    """Wraps a queue. Publishes some messages twice and drops some acks.

    A dropped ack leaves the message in flight, so it is redelivered after the
    visibility timeout even though the work was done: a duplicate delivery.
    """

    def __init__(self, inner: MessageQueue, chaos: Chaos) -> None:
        self.inner = inner
        self.chaos = chaos
        self.duplicates_published = 0
        self.acks_dropped = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def publish(self, topic: str, payload: dict[str, Any], **kw: Any) -> int:
        msg_id = self.inner.publish(topic, payload, **kw)
        key = payload.get("event_id", msg_id)
        if self.chaos.duplicate_rate and self.chaos.roll("dup", key) < self.chaos.duplicate_rate:
            self.inner.publish(topic, payload, **kw)
            self.duplicates_published += 1
        return msg_id

    def ack(self, delivery: Delivery) -> bool:
        if self.chaos.drop_ack_rate and (
            self.chaos.roll("dropack", delivery.message.id, delivery.group, delivery.attempts)
            < self.chaos.drop_ack_rate
        ):
            self.acks_dropped += 1
            return False
        return self.inner.ack(delivery)
