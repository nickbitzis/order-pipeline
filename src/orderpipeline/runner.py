"""Build and run services, either one per process or all in one process."""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path

from orderpipeline.chaos import Chaos, ChaosQueue, SimulatedCrash
from orderpipeline.config import Settings
from orderpipeline.db import connect, init_db
from orderpipeline.logs import log
from orderpipeline.queue.sqlite import SQLiteQueue
from orderpipeline.services.inventory import InventoryWorker
from orderpipeline.services.payments import PaymentsWorker
from orderpipeline.services.relay import OutboxRelay

logger = logging.getLogger("orderpipeline.runner")
SERVICES = ("relay", "payments", "inventory")
HEARTBEAT_DIR = Path(os.environ.get("ORDERPIPELINE_HEARTBEAT_DIR", "/tmp"))


def make_chaos(s: Settings) -> Chaos:
    return Chaos(seed=s.seed, crash_rate=s.crash_rate, duplicate_rate=s.duplicate_rate,
                 drop_ack_rate=s.drop_ack_rate)


def build(service: str, s: Settings, chaos: Chaos | None = None):
    """Create one service instance with its own connections."""
    chaos = chaos or make_chaos(s)
    conn = connect(s.db_path)
    queue = ChaosQueue(SQLiteQueue(s.db_path, max_attempts=s.max_attempts), chaos)
    if service == "relay":
        return OutboxRelay(conn, queue, chaos, poll_interval=s.poll_interval)
    if service == "payments":
        return PaymentsWorker(conn, queue, s, chaos)
    if service == "inventory":
        return InventoryWorker(conn, queue, s, chaos)
    raise ValueError(f"unknown service {service}")


def supervise(service: str, s: Settings, stop: threading.Event, chaos: Chaos | None = None,
              heartbeat: bool = False) -> None:
    """Run a service and restart it after a simulated crash, like a process supervisor."""
    restarts = 0
    while not stop.is_set():
        svc = build(service, s, chaos)
        try:
            while not stop.is_set():
                if svc.run_once() == 0:
                    stop.wait(s.poll_interval)
                if heartbeat:
                    (HEARTBEAT_DIR / f"orderpipeline-{service}.beat").write_text(str(time.time()))
        except SimulatedCrash as e:
            restarts += 1
            log(logger, "service crashed, restarting", logging.WARNING, service=service,
                point=str(e), restarts=restarts)
            if s.crash_exits_process:
                os._exit(1)
        finally:
            svc.conn.close()
            svc.queue.close()


class Pipeline:
    """Relay plus N payments and M inventory workers on threads."""

    def __init__(self, s: Settings, payments: int = 2, inventory: int = 2) -> None:
        self.s = s
        self.chaos = make_chaos(s)
        init_db(s.db_path)
        self.stop_event = threading.Event()
        plan = ["relay"] + ["payments"] * payments + ["inventory"] * inventory
        self.threads = [
            threading.Thread(target=supervise, args=(name, s, self.stop_event, self.chaos),
                             name=f"{name}-{i}", daemon=True)
            for i, name in enumerate(plan)
        ]

    def start(self) -> Pipeline:
        for t in self.threads:
            t.start()
        return self

    def stop(self) -> None:
        self.stop_event.set()
        for t in self.threads:
            t.join(timeout=10)

    def busy(self) -> bool:
        conn = connect(self.s.db_path)
        try:
            pending = conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]
            live = conn.execute(
                "SELECT COUNT(*) FROM q_deliveries WHERE state IN ('ready','inflight')"
            ).fetchone()[0]
            return pending + live > 0
        finally:
            conn.close()

    def wait_idle(self, timeout: float = 120.0) -> bool:
        deadline = time.time() + timeout
        quiet = 0
        while time.time() < deadline:
            quiet = quiet + 1 if not self.busy() else 0
            if quiet >= 3:
                return True
            time.sleep(0.2)
        return False
