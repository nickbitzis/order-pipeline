from __future__ import annotations

import pytest

from orderpipeline.config import Settings
from orderpipeline.db import connect, init_db
from orderpipeline.queue.sqlite import SQLiteQueue


class FakeClock:
    def __init__(self, t: float = 1_000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def db_path(tmp_path):
    p = str(tmp_path / "test.db")
    init_db(p)
    return p


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def queue(db_path, clock):
    q = SQLiteQueue(db_path, max_attempts=3, clock=clock)
    yield q
    q.close()


@pytest.fixture
def conn(db_path):
    c = connect(db_path)
    yield c
    c.close()


@pytest.fixture
def settings(db_path):
    return Settings(db_path=db_path, provider_failure_rate=0.0, provider_latency_ms=0.0,
                    max_attempts=3, visibility_timeout=5.0, backoff_base=0.01, backoff_max=0.05,
                    poll_interval=0.01)
