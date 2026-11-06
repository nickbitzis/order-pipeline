"""The queue interface every service codes against.

Swapping SQLite for Kafka, SQS, or RabbitMQ means writing another class with
these methods. See the README section "Mapping to real brokers".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class Message:
    id: int
    topic: str
    key: str | None
    payload: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)
    created_at: float = 0.0


@dataclass(frozen=True)
class Delivery:
    """One lease on one message for one consumer group.

    `attempts` counts this delivery too, so the first receive has attempts == 1.
    `receipt` must be passed back to ack or nack. A receipt goes stale when the
    visibility timeout expires and someone else receives the message.
    """

    message: Message
    group: str
    attempts: int
    receipt: str


@dataclass(frozen=True)
class DeadLetter:
    message: Message
    group: str
    attempts: int
    last_error: str


class MessageQueue(Protocol):
    max_attempts: int

    def subscribe(self, topic: str, group: str) -> None: ...

    def publish(
        self,
        topic: str,
        payload: dict[str, Any],
        *,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> int: ...

    def receive(
        self,
        topic: str,
        group: str,
        *,
        max_messages: int = 1,
        visibility_timeout: float = 30.0,
    ) -> list[Delivery]: ...

    def ack(self, delivery: Delivery) -> bool: ...

    def nack(
        self,
        delivery: Delivery,
        *,
        delay: float = 0.0,
        error: str = "",
        dead_letter: bool = False,
    ) -> bool: ...

    def depth(self) -> list[dict[str, Any]]: ...

    def dead_letters(
        self, topic: str | None = None, group: str | None = None, limit: int = 100
    ) -> list[DeadLetter]: ...

    def replay(
        self,
        topic: str | None = None,
        group: str | None = None,
        message_ids: list[int] | None = None,
    ) -> int: ...

    def close(self) -> None: ...
