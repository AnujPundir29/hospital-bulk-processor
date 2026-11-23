"""Bulk job state and its in-memory store."""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum


class RowStatus(str, Enum):
    PENDING = "pending"
    CREATED = "created"  # exists upstream, still inactive
    ACTIVATED = "created_and_activated"
    FAILED = "failed"
    DISCARDED = "discarded"


class JobStatus(str, Enum):
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"  # some rows or the activation failed; batch left inactive and resumable
    DISCARDED = "discarded"


@dataclass
class RowState:
    row: int
    name: str
    address: str
    phone: str | None
    status: RowStatus = RowStatus.PENDING
    hospital_id: int | None = None
    error: str | None = None
    ambiguous: bool = False  # last failure may have created the hospital upstream → reconcile before retrying

    def mark_created(self, hospital_id: int) -> None:
        self.status, self.hospital_id, self.error, self.ambiguous = RowStatus.CREATED, hospital_id, None, False

    def mark_failed(self, error: str, *, ambiguous: bool = False) -> None:
        self.status, self.hospital_id, self.error, self.ambiguous = RowStatus.FAILED, None, error, ambiguous


@dataclass
class Job:
    batch_id: str
    rows: list[RowState]
    status: JobStatus = JobStatus.PROCESSING
    batch_activated: bool = False
    error: str | None = None
    started_at: float = field(default_factory=time.monotonic)
    finished_at: float | None = None

    @property
    def processed(self) -> int:
        return sum(r.hospital_id is not None for r in self.rows)

    @property
    def failed(self) -> int:
        return sum(r.status is RowStatus.FAILED for r in self.rows)

    @property
    def processing_time_seconds(self) -> float:
        return round((self.finished_at or time.monotonic()) - self.started_at, 3)

    def fail(self, error: str) -> None:
        self.status, self.error = JobStatus.FAILED, error


class JobStore:
    """In-memory, single-process job registry.

    ponytail: bounded OrderedDict; oldest job evicted past `max_jobs`. Running >1 worker/instance needs a
    shared store (Redis hash per batch with a TTL) — the processor only uses add/get, so it's a drop-in swap.
    """

    def __init__(self, max_jobs: int) -> None:
        self._jobs: OrderedDict[str, Job] = OrderedDict()
        self._max_jobs = max_jobs

    def add(self, job: Job) -> None:
        self._jobs[job.batch_id] = job
        while len(self._jobs) > self._max_jobs:
            self._jobs.popitem(last=False)

    def get(self, batch_id: str) -> Job | None:
        return self._jobs.get(batch_id)
