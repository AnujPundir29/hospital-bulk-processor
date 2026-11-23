from __future__ import annotations

import os
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class Settings:
    """Runtime configuration. Every field can be overridden by the upper-cased env var of the same name."""

    upstream_base_url: str = "https://hospital-directory.onrender.com"
    # Upstream POST measured at ~5s; Render free-tier cold starts can hold a request for ~50s.
    upstream_timeout_seconds: float = 60.0
    # Global cap on in-flight upstream calls, shared by all jobs. 20 = one full CSV in a single wave.
    max_concurrency: int = 20
    max_attempts: int = 3
    retry_backoff_seconds: float = 0.5
    max_csv_rows: int = 20
    max_upload_bytes: int = 1_000_000
    max_jobs_in_memory: int = 1_000

    @classmethod
    def from_env(cls) -> Settings:
        overrides = {}
        for f in fields(cls):
            raw = os.getenv(f.name.upper())
            if raw:
                overrides[f.name] = type(f.default)(raw)
        return cls(**overrides)
