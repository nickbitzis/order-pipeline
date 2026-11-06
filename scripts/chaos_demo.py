"""Run 500 orders through the pipeline with failures injected, then check invariants.

    uv run python scripts/chaos_demo.py [--orders 500] [--seed 42]

Exit code is 0 only when every invariant holds.
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from pathlib import Path

from orderpipeline.invariants import format_checks
from orderpipeline.logs import setup_logging
from orderpipeline.simulation import chaos_settings, replay_dlq, run


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--orders", type=int, default=500)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--failure-rate", type=float, default=0.3)
    p.add_argument("--crash-rate", type=float, default=0.03)
    p.add_argument("--duplicate-rate", type=float, default=0.05)
    p.add_argument("--drop-ack-rate", type=float, default=0.03)
    p.add_argument("--no-replay", action="store_true", help="skip the DLQ replay phase")
    p.add_argument("--db", help="keep the database here (default: a temp dir)")
    p.add_argument("--verbose", action="store_true", help="print JSON logs")
    a = p.parse_args()
    setup_logging("INFO" if a.verbose else "CRITICAL")
    if not a.verbose:
        logging.getLogger("orderpipeline").setLevel(logging.CRITICAL)

    db = a.db or str(Path(tempfile.mkdtemp()) / "chaos.db")
    s = chaos_settings(db, seed=a.seed, provider_failure_rate=a.failure_rate,
                       crash_rate=a.crash_rate, duplicate_rate=a.duplicate_rate,
                       drop_ack_rate=a.drop_ack_rate)
    print(f"chaos run: {a.orders} orders, seed {a.seed}, provider failure rate "
          f"{s.provider_failure_rate}, crash rate {s.crash_rate}, duplicate publish rate "
          f"{s.duplicate_rate}, dropped ack rate {s.drop_ack_rate}, max attempts "
          f"{s.max_attempts}")
    r = run(s, a.orders)
    print(f"finished in {r.seconds}s, pipeline drained: {r.idle}")
    print(f"client retries with the same Idempotency-Key: {r.client_retries}")
    print("orders by final state: " + ", ".join(f"{k} {v}" for k, v in r.statuses.items()))
    print("message outcomes: " + ", ".join(f"{k} {v}" for k, v in r.outcomes.items()))
    print(f"dead letters: {r.dlq}")
    print()
    print(format_checks(r.checks))
    print()
    print("ALL INVARIANTS HOLD" if r.ok else "INVARIANT VIOLATION")
    ok = r.ok
    if not a.no_replay and r.dlq:
        print()
        print("phase 2: provider healthy again, operator replays the DLQ")
        r2 = replay_dlq(s)
        print(f"replayed {r2.replayed} dead letters in {r2.seconds}s, drained: {r2.idle}")
        print("orders by final state: " + ", ".join(f"{k} {v}" for k, v in r2.statuses.items()))
        print(f"dead letters left: {r2.dlq}")
        print()
        print(format_checks(r2.checks))
        print()
        print("ALL INVARIANTS HOLD" if r2.ok else "INVARIANT VIOLATION")
        ok = ok and r2.ok
    print(f"database: {db}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
