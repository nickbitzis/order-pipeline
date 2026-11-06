"""Command line entry point: run a service, inspect the queue, replay the DLQ."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

from orderpipeline.config import Settings
from orderpipeline.db import connect, init_db
from orderpipeline.logs import setup_logging


def _settings(args: argparse.Namespace) -> Settings:
    return Settings.from_env(db_path=getattr(args, "db", None))


def cmd_api(args: argparse.Namespace) -> int:
    import uvicorn

    from orderpipeline.api import create_app

    app = create_app(_settings(args))
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


def cmd_service(args: argparse.Namespace) -> int:
    from orderpipeline.runner import supervise

    s = _settings(args)
    init_db(s.db_path)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    supervise(args.cmd, s, stop, heartbeat=True)
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    s = _settings(args)
    init_db(s.db_path)
    print(f"initialized {s.db_path}")
    return 0


def cmd_queue(args: argparse.Namespace) -> int:
    from orderpipeline.queue.sqlite import SQLiteQueue

    s = _settings(args)
    init_db(s.db_path)
    q = SQLiteQueue(s.db_path, max_attempts=s.max_attempts)
    try:
        if args.action == "depth":
            print(f"{'topic':<20}{'group':<12}{'state':<10}{'count':>7}")
            for d in q.depth():
                print(f"{d['topic']:<20}{d['group']:<12}{d['state']:<10}{d['count']:>7}")
        elif args.action == "dlq":
            dead = q.dead_letters(args.topic, args.group, limit=args.limit)
            print(f"{len(dead)} dead letters")
            for dl in dead:
                p = dl.message.payload
                print(json.dumps({
                    "message_id": dl.message.id, "topic": dl.message.topic, "group": dl.group,
                    "attempts": dl.attempts, "error": dl.last_error,
                    "type": p.get("type") if isinstance(p, dict) else None,
                    "order_id": p.get("order_id") if isinstance(p, dict) else None,
                }))
        elif args.action == "replay":
            n = q.replay(args.topic, args.group, args.id or None)
            print(f"replayed {n} messages")
    finally:
        q.close()
    return 0


def cmd_saga(args: argparse.Namespace) -> int:
    from orderpipeline import saga

    s = _settings(args)
    conn = connect(s.db_path)
    if args.order_id:
        for h in saga.history(conn, args.order_id):
            print(f"{h['from_state'] or '-':<15} -> {h['to_state']:<15} {h['reason']}")
    else:
        for r in conn.execute("SELECT status, COUNT(*) FROM orders GROUP BY 1 ORDER BY 1"):
            print(f"{r[0]:<16}{r[1]:>6}")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    from orderpipeline.invariants import check_all, format_checks
    from orderpipeline.queue.sqlite import SQLiteQueue

    s = _settings(args)
    conn = connect(s.db_path)
    q = SQLiteQueue(s.db_path)
    checks = check_all(conn, q)
    print(format_checks(checks))
    return 0 if all(c.ok for c in checks) else 1


def cmd_demo(args: argparse.Namespace) -> int:
    import httpx
    import uvicorn

    from orderpipeline.api import create_app
    from orderpipeline.invariants import check_all, format_checks
    from orderpipeline.queue.sqlite import SQLiteQueue
    from orderpipeline.runner import Pipeline
    from orderpipeline.simulation import place_orders

    s = _settings(args)
    if args.fresh:
        for suffix in ("", "-wal", "-shm"):
            Path(s.db_path + suffix).unlink(missing_ok=True)
    s.poll_interval = min(s.poll_interval, 0.05)
    app = create_app(s)
    pipe = Pipeline(s).start()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=args.port,
                                           log_level="warning"))
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    while not server.started:
        time.sleep(0.05)
    base = f"http://127.0.0.1:{args.port}"
    print(f"API on {base}  (relay, 2 payments, 2 inventory workers on threads)", file=sys.stderr)
    with httpx.Client(base_url=base) as client:
        place_orders(client, args.orders, s.seed)
        # A few bulk orders for the scarcest SKU, so the saga has to compensate.
        for i in range(4):
            client.post("/orders", json={"customer_id": "cust-bulk", "sku": "SKU-PEN", "qty": 10},
                        headers={"Idempotency-Key": f"demo-bulk-{s.seed}-{i}"})
        pipe.wait_idle(60)
        conn = connect(s.db_path)
        print("\nOrders by saga state:")
        for r in conn.execute("SELECT status, COUNT(*) FROM orders GROUP BY 1 ORDER BY 1"):
            print(f"  {r[0]:<16}{r[1]:>5}")
        sample = conn.execute(
            "SELECT id FROM orders WHERE status = 'REFUNDED' LIMIT 1").fetchone() or \
            conn.execute("SELECT id FROM orders LIMIT 1").fetchone()
        if sample:
            o = client.get(f"/orders/{sample[0]}").json()
            print(f"\nSaga for {o['order_id']} (correlation id {o['correlation_id']}):")
            for h in o["saga"]:
                print(f"  {h['from_state'] or '-':<15} -> {h['to_state']:<15} {h['reason']}")
        print("\nInvariants:")
        print(format_checks(check_all(conn, SQLiteQueue(s.db_path))))
        m = client.get("/metrics").text
        print("\n/metrics (excerpt):")
        for line in m.splitlines():
            if line.startswith(("orderpipeline_queue_depth", "orderpipeline_dlq",
                                "orderpipeline_orders", "orderpipeline_messages_total")):
                print("  " + line)
        if args.serve:
            print(f"\nServing on {base}. Ctrl-C to stop.", file=sys.stderr)
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
    server.should_exit = True
    pipe.stop()
    return 0


def cmd_healthcheck(args: argparse.Namespace) -> int:
    service = os.environ.get("ORDERPIPELINE_SERVICE", "api")
    if service == "api":
        import urllib.request

        try:
            with urllib.request.urlopen("http://127.0.0.1:8000/healthz", timeout=3) as r:
                return 0 if r.status == 200 else 1
        except Exception:  # noqa: BLE001
            return 1
    from orderpipeline.runner import HEARTBEAT_DIR

    beat = HEARTBEAT_DIR / f"orderpipeline-{service}.beat"
    try:
        return 0 if time.time() - float(beat.read_text()) < 30 else 1
    except (OSError, ValueError):
        return 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="orderpipeline", description=__doc__)
    p.add_argument("--db", help="SQLite path (default: $ORDERPIPELINE_DB_PATH or data/...)")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("api", help="run the orders API")
    a.add_argument("--host", default="0.0.0.0")
    a.add_argument("--port", type=int, default=8000)
    a.set_defaults(func=cmd_api)
    for name in ("relay", "payments", "inventory"):
        sub.add_parser(name, help=f"run the {name} service").set_defaults(func=cmd_service)
    sub.add_parser("init-db", help="create tables and seed stock").set_defaults(func=cmd_init)

    q = sub.add_parser("queue", help="inspect the queue and DLQ")
    q.add_argument("action", choices=["depth", "dlq", "replay"])
    q.add_argument("--topic")
    q.add_argument("--group")
    q.add_argument("--id", type=int, action="append", help="message id to replay (repeatable)")
    q.add_argument("--limit", type=int, default=50)
    q.set_defaults(func=cmd_queue)

    sg = sub.add_parser("saga", help="order counts by state, or one order's saga history")
    sg.add_argument("order_id", nargs="?")
    sg.set_defaults(func=cmd_saga)
    sub.add_parser("check", help="run invariant checks").set_defaults(func=cmd_check)

    d = sub.add_parser("demo", help="run everything in one process and place sample orders")
    d.add_argument("--orders", type=int, default=40)
    d.add_argument("--port", type=int, default=8000)
    d.add_argument("--fresh", action="store_true", help="delete the database first")
    d.add_argument("--serve", action="store_true", help="keep running after the demo")
    d.set_defaults(func=cmd_demo)
    sub.add_parser("healthcheck", help="container health probe").set_defaults(
        func=cmd_healthcheck)
    return p


def main(argv: list[str] | None = None) -> int:
    setup_logging(os.environ.get("ORDERPIPELINE_LOG_LEVEL", "INFO"))
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
