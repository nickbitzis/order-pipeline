"""Counters and latency histograms stored in the shared database.

Workers run in separate processes, so in-memory counters would only show one
process. Writing them to the shared store lets the API's /metrics show the
whole pipeline. At real scale each process would expose its own /metrics and
Prometheus would aggregate.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict

from orderpipeline.db import transaction
from orderpipeline.queue.base import MessageQueue

BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)


def _labels(**kw: str) -> str:
    return json.dumps(kw, sort_keys=True)


class Stats:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def record(self, service: str, topic: str, outcome: str, seconds: float) -> None:
        rows = [("messages_total", _labels(service=service, topic=topic, outcome=outcome), 1.0)]
        base = {"service": service, "topic": topic}
        for b in BUCKETS:
            if seconds <= b:
                rows.append(("processing_seconds_bucket", _labels(**base, le=str(b)), 1.0))
        rows.append(("processing_seconds_bucket", _labels(**base, le="+Inf"), 1.0))
        rows.append(("processing_seconds_sum", _labels(**base), seconds))
        rows.append(("processing_seconds_count", _labels(**base), 1.0))
        with transaction(self.conn) as c:
            c.executemany(
                "INSERT INTO stats(name, labels, value) VALUES (?, ?, ?)"
                " ON CONFLICT(name, labels) DO UPDATE SET value = value + excluded.value",
                rows,
            )


def _fmt(name: str, labels: dict[str, str], value: float) -> str:
    if labels:
        inner = ",".join(f'{k}="{v}"' for k, v in labels.items())
        return f"{name}{{{inner}}} {value:g}"
    return f"{name} {value:g}"


def render(conn: sqlite3.Connection, queue: MessageQueue) -> str:
    lines: list[str] = []

    lines += [
        "# HELP orderpipeline_queue_depth Deliveries per topic, group, and state.",
        "# TYPE orderpipeline_queue_depth gauge",
    ]
    dlq: dict[str, int] = defaultdict(int)
    for d in queue.depth():
        if d["state"] == "acked":
            continue
        lines.append(
            _fmt(
                "orderpipeline_queue_depth",
                {"topic": d["topic"], "group": d["group"], "state": d["state"]},
                d["count"],
            )
        )
        if d["state"] == "dead":
            dlq[d["topic"]] += d["count"]
    lines += ["# HELP orderpipeline_dlq_size Dead letters per topic.",
              "# TYPE orderpipeline_dlq_size gauge"]
    for topic, n in sorted(dlq.items()):
        lines.append(_fmt("orderpipeline_dlq_size", {"topic": topic}, n))
    if not dlq:
        lines.append(_fmt("orderpipeline_dlq_size", {"topic": "all"}, 0))

    pending = conn.execute("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]
    lines += ["# HELP orderpipeline_outbox_pending Events written but not yet published.",
              "# TYPE orderpipeline_outbox_pending gauge",
              _fmt("orderpipeline_outbox_pending", {}, pending)]

    lines += ["# HELP orderpipeline_orders Orders by saga state.",
              "# TYPE orderpipeline_orders gauge"]
    for r in conn.execute("SELECT status, COUNT(*) AS n FROM orders GROUP BY status ORDER BY 1"):
        lines.append(_fmt("orderpipeline_orders", {"status": r["status"]}, r["n"]))

    by_name: dict[str, list[tuple[dict, float]]] = defaultdict(list)
    for r in conn.execute("SELECT name, labels, value FROM stats ORDER BY name, labels"):
        by_name[r["name"]].append((json.loads(r["labels"]), r["value"]))
    lines += ["# HELP orderpipeline_messages_total Messages handled by outcome.",
              "# TYPE orderpipeline_messages_total counter"]
    for labels, v in by_name.get("messages_total", []):
        lines.append(_fmt("orderpipeline_messages_total", labels, v))
    lines += ["# HELP orderpipeline_processing_seconds Handler latency per message.",
              "# TYPE orderpipeline_processing_seconds histogram"]
    def le_key(item: tuple[dict, float]) -> tuple:
        lab = item[0]
        le = lab.get("le", "+Inf")
        return (lab["service"], lab["topic"], float("inf") if le == "+Inf" else float(le))

    for suffix in ("bucket", "sum", "count"):
        rows = sorted(by_name.get(f"processing_seconds_{suffix}", []), key=le_key)
        for labels, v in rows:
            lines.append(_fmt(f"orderpipeline_processing_seconds_{suffix}", labels, v))
    return "\n".join(lines) + "\n"
