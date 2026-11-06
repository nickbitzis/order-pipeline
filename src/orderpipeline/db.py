"""SQLite helpers and the application schema.

Every process opens its own connections. WAL mode lets readers run while one
writer commits, and `BEGIN IMMEDIATE` takes the write lock up front so two
writers never deadlock on a lock upgrade.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

CATALOG = {
    # sku: (price in cents, starting stock)
    "SKU-MUG": (1200, 200),
    "SKU-TEE": (2500, 150),
    "SKU-CAP": (1800, 120),
    "SKU-BAG": (4200, 100),
    "SKU-PEN": (300, 30),
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    amount_cents INTEGER NOT NULL,
    status TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS saga_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_saga_order ON saga_log(order_id);
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    payload TEXT NOT NULL,
    headers TEXT NOT NULL,
    created_at REAL NOT NULL,
    published_at REAL
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox(published_at, id);
CREATE TABLE IF NOT EXISTS processed_events (
    consumer TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed_at REAL NOT NULL,
    PRIMARY KEY (consumer, event_id)
);
CREATE TABLE IF NOT EXISTS payments (
    order_id TEXT PRIMARY KEY,
    charge_id TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    status TEXT NOT NULL,
    refund_id TEXT,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    price_cents INTEGER NOT NULL,
    initial_stock INTEGER NOT NULL,
    stock INTEGER NOT NULL CHECK (stock >= 0)
);
CREATE TABLE IF NOT EXISTS reservations (
    order_id TEXT PRIMARY KEY,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS provider_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    ref_id TEXT NOT NULL UNIQUE,
    order_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ledger_key ON provider_ledger(kind, idempotency_key);
CREATE TABLE IF NOT EXISTS stats (
    name TEXT NOT NULL,
    labels TEXT NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (name, labels)
);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    """Open a connection in autocommit mode. Use `transaction()` for writes."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """One atomic unit of work. Rolls back on any exception."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def init_db(path: str | Path, seed_stock: bool = True) -> None:
    """Create app and queue tables. Safe to call from every service on start."""
    from orderpipeline.queue.sqlite import SQLiteQueue

    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        if seed_stock:
            with transaction(conn):
                for sku, (price, stock) in CATALOG.items():
                    conn.execute(
                        "INSERT OR IGNORE INTO inventory(sku, price_cents, initial_stock, stock)"
                        " VALUES (?, ?, ?, ?)",
                        (sku, price, stock, stock),
                    )
    finally:
        conn.close()
    from orderpipeline.events import SUBSCRIPTIONS

    q = SQLiteQueue(path)
    try:
        for topic, groups in SUBSCRIPTIONS.items():
            for group in groups:
                q.subscribe(topic, group)
    finally:
        q.close()


def now() -> float:
    return time.time()
