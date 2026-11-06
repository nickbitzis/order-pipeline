"""Settings from environment variables, prefix ORDERPIPELINE_."""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields


@dataclass
class Settings:
    db_path: str = "data/orderpipeline.db"
    seed: int = 7
    # fake payment provider
    provider_failure_rate: float = 0.1
    provider_latency_ms: float = 5.0
    # queue and retry policy
    max_attempts: int = 5
    visibility_timeout: float = 5.0
    backoff_base: float = 0.2
    backoff_max: float = 5.0
    poll_interval: float = 0.1
    # chaos
    crash_rate: float = 0.0
    duplicate_rate: float = 0.0
    drop_ack_rate: float = 0.0
    crash_exits_process: bool = False
    extra: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, **overrides: object) -> Settings:
        s = cls()
        for f in fields(cls):
            if f.name == "extra":
                continue
            raw = os.environ.get(f"ORDERPIPELINE_{f.name.upper()}")
            if raw is None:
                continue
            current = getattr(s, f.name)
            if isinstance(current, bool):
                setattr(s, f.name, raw.lower() in ("1", "true", "yes"))
            else:
                setattr(s, f.name, type(current)(raw))
        for k, v in overrides.items():
            if v is not None:
                setattr(s, k, v)
        return s
