"""A durable message queue on SQLite.

Model:
- `messages` is an append-only log per topic (like a Kafka topic).
- `subscriptions` lists the consumer groups for each topic. Publishing creates
  one `deliveries` row per group, so every group sees every message (fan-out),
  and workers inside a group compete for rows (like SQS consumers on a queue).
- A receive leases a row for `visibility_timeout` seconds. If the worker dies
  and never acks, the lease expires and the row is delivered again. That is
  at-least-once delivery.
- After `max_attempts` deliveries the row moves to state `dead`: the DLQ.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from orderpipeline.db import connect, transaction
from orderpipeline.queue.base import DeadLetter, Delivery, Message

QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS q_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    key TEXT,
    payload TEXT NOT NULL,
    headers TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS q_subscriptions (
    topic TEXT NOT NULL,
    group_name TEXT NOT NULL,
    PRIMARY KEY (topic, group_name)
);
CREATE TABLE IF NOT EXISTS q_deliveries (
    message_id INTEGER NOT NULL,
    topic TEXT NOT NULL,
    group_name TEXT NOT NULL,
    state TEXT NOT NULL,            -- ready | inflight | acked | dead
    attempts INTEGER NOT NULL DEFAULT 0,
    visible_at REAL NOT NULL,
    receipt TEXT,
    last_error TEXT,
    PRIMARY KEY (message_id, group_name)
);
CREATE INDEX IF NOT EXISTS ix_q_ready ON q_deliveries(topic, group_name, state, visible_at);
"""


class SQLiteQueue:
    def __init__(
        self,
        path: str | Path,
        *,
        max_attempts: int = 5,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.path = str(path)
        self.max_attempts = max_attempts
        self.clock = clock
        self.conn: sqlite3.Connection = connect(path)
        self.conn.executescript(QUEUE_SCHEMA)

    def close(self) -> None:
        self.conn.close()

    # producer side -----------------------------------------------------
    def subscribe(self, topic: str, group: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO q_subscriptions(topic, group_name) VALUES (?, ?)",
            (topic, group),
        )

    def publish(
        self,
        topic: str,
        payload: dict[str, Any],
        *,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> int:
        now = self.clock()
        with transaction(self.conn) as c:
            cur = c.execute(
                "INSERT INTO q_messages(topic, key, payload, headers, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (topic, key, json.dumps(payload), json.dumps(headers or {}), now),
            )
            msg_id = cur.lastrowid
            c.execute(
                "INSERT INTO q_deliveries(message_id, topic, group_name, state, visible_at)"
                " SELECT ?, topic, group_name, 'ready', ? FROM q_subscriptions WHERE topic = ?",
                (msg_id, now, topic),
            )
        return int(msg_id)

    # consumer side -----------------------------------------------------
    def receive(
        self,
        topic: str,
        group: str,
        *,
        max_messages: int = 1,
        visibility_timeout: float = 30.0,
    ) -> list[Delivery]:
        now = self.clock()
        out: list[Delivery] = []
        with transaction(self.conn) as c:
            # A lease that expired on its last allowed attempt means the worker
            # died every time. Do not hand it out again: dead-letter it.
            c.execute(
                "UPDATE q_deliveries SET state = 'dead', receipt = NULL,"
                " last_error = 'visibility timeout expired on final attempt'"
                " WHERE topic = ? AND group_name = ? AND state = 'inflight'"
                " AND visible_at <= ? AND attempts >= ?",
                (topic, group, now, self.max_attempts),
            )
            rows = c.execute(
                "SELECT d.message_id, d.attempts, m.key, m.payload, m.headers, m.created_at"
                " FROM q_deliveries d JOIN q_messages m ON m.id = d.message_id"
                " WHERE d.topic = ? AND d.group_name = ? AND d.state IN ('ready', 'inflight')"
                " AND d.visible_at <= ? ORDER BY d.message_id LIMIT ?",
                (topic, group, now, max_messages),
            ).fetchall()
            for r in rows:
                receipt = uuid.uuid4().hex
                c.execute(
                    "UPDATE q_deliveries SET state = 'inflight', attempts = attempts + 1,"
                    " visible_at = ?, receipt = ? WHERE message_id = ? AND group_name = ?",
                    (now + visibility_timeout, receipt, r["message_id"], group),
                )
                msg = Message(
                    id=r["message_id"],
                    topic=topic,
                    key=r["key"],
                    payload=json.loads(r["payload"]),
                    headers=json.loads(r["headers"]),
                    created_at=r["created_at"],
                )
                out.append(Delivery(msg, group, r["attempts"] + 1, receipt))
        return out

    def ack(self, delivery: Delivery) -> bool:
        cur = self.conn.execute(
            "UPDATE q_deliveries SET state = 'acked', receipt = NULL"
            " WHERE message_id = ? AND group_name = ? AND receipt = ? AND state = 'inflight'",
            (delivery.message.id, delivery.group, delivery.receipt),
        )
        return cur.rowcount == 1

    def nack(
        self,
        delivery: Delivery,
        *,
        delay: float = 0.0,
        error: str = "",
        dead_letter: bool = False,
    ) -> bool:
        dead = dead_letter or delivery.attempts >= self.max_attempts
        state = "dead" if dead else "ready"
        cur = self.conn.execute(
            "UPDATE q_deliveries SET state = ?, visible_at = ?, receipt = NULL, last_error = ?"
            " WHERE message_id = ? AND group_name = ? AND receipt = ? AND state = 'inflight'",
            (
                state,
                self.clock() + delay,
                error[:500],
                delivery.message.id,
                delivery.group,
                delivery.receipt,
            ),
        )
        return cur.rowcount == 1

    # operations --------------------------------------------------------
    def depth(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT topic, group_name, state, COUNT(*) AS n FROM q_deliveries"
            " GROUP BY topic, group_name, state ORDER BY topic, group_name, state"
        ).fetchall()
        return [
            {"topic": r["topic"], "group": r["group_name"], "state": r["state"], "count": r["n"]}
            for r in rows
        ]

    def dead_letters(
        self, topic: str | None = None, group: str | None = None, limit: int = 100
    ) -> list[DeadLetter]:
        sql = (
            "SELECT d.message_id, d.topic, d.group_name, d.attempts, d.last_error,"
            " m.key, m.payload, m.headers, m.created_at"
            " FROM q_deliveries d JOIN q_messages m ON m.id = d.message_id"
            " WHERE d.state = 'dead'"
        )
        args: list[Any] = []
        if topic:
            sql += " AND d.topic = ?"
            args.append(topic)
        if group:
            sql += " AND d.group_name = ?"
            args.append(group)
        sql += " ORDER BY d.message_id LIMIT ?"
        args.append(limit)
        out = []
        for r in self.conn.execute(sql, args).fetchall():
            msg = Message(
                r["message_id"],
                r["topic"],
                r["key"],
                json.loads(r["payload"]),
                json.loads(r["headers"]),
                r["created_at"],
            )
            out.append(DeadLetter(msg, r["group_name"], r["attempts"], r["last_error"] or ""))
        return out

    def replay(
        self,
        topic: str | None = None,
        group: str | None = None,
        message_ids: list[int] | None = None,
    ) -> int:
        """Move dead letters back to ready with a fresh attempt budget."""
        sql = (
            "UPDATE q_deliveries SET state = 'ready', attempts = 0, visible_at = ?,"
            " receipt = NULL WHERE state = 'dead'"
        )
        args: list[Any] = [self.clock()]
        if topic:
            sql += " AND topic = ?"
            args.append(topic)
        if group:
            sql += " AND group_name = ?"
            args.append(group)
        if message_ids:
            sql += f" AND message_id IN ({','.join('?' * len(message_ids))})"
            args.extend(message_ids)
        return self.conn.execute(sql, args).rowcount
