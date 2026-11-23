"""Bulk processing workflow: create every row concurrently with the batch id, then activate the batch.

The upstream "inactive until activated" batch is used as a staging area, so the batch goes live
all-or-nothing: activation happens only once every row exists upstream. On partial failure the batch stays
inactive and `resume` re-runs the same idempotent pipeline: reconcile with upstream → create what's
missing → activate.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict

from app.csv_parser import HospitalRow
from app.jobs import Job, JobStatus, JobStore, RowState, RowStatus
from app.upstream import HospitalDirectoryClient, UpstreamError

logger = logging.getLogger(__name__)


class Conflict(Exception):
    """Operation not allowed in the job's current state."""


def _key(name: str, address: str) -> tuple[str, str]:
    return name.casefold(), address.casefold()


class BulkProcessor:
    def __init__(self, client: HospitalDirectoryClient, store: JobStore) -> None:
        self.client = client
        self.store = store
        self._tasks: set[asyncio.Task] = set()  # strong refs so background jobs aren't garbage-collected

    def submit(self, rows: list[HospitalRow]) -> Job:
        job = Job(batch_id=str(uuid.uuid4()), rows=[RowState(r.row, r.name, r.address, r.phone) for r in rows])
        self.store.add(job)
        return job

    def start(self, job: Job, *, resume: bool = False) -> asyncio.Task:
        # State flips synchronously so a 202 shows "processing" and a concurrent resume gets 409.
        job.status, job.error, job.started_at, job.finished_at = JobStatus.PROCESSING, None, time.monotonic(), None
        task = asyncio.create_task(self._run(job, reconcile_first=resume))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def resume(self, job: Job) -> asyncio.Task:
        if job.status is not JobStatus.FAILED:
            raise Conflict(f"Batch is {job.status.value}; only failed batches can be resumed")
        return self.start(job, resume=True)

    async def discard(self, job: Job) -> int:
        if job.status is not JobStatus.FAILED:
            raise Conflict(f"Batch is {job.status.value}; only failed (inactive) batches can be discarded")
        job.status = JobStatus.DISCARDED  # claim before awaiting so a concurrent resume is rejected
        try:
            deleted = await self.client.delete_batch(job.batch_id)
        except UpstreamError:
            job.status = JobStatus.FAILED
            raise
        for row in job.rows:
            row.status = RowStatus.DISCARDED
        return deleted

    async def _run(self, job: Job, *, reconcile_first: bool) -> None:
        try:
            if reconcile_first:
                await self._reconcile(job)
            pending = [r for r in job.rows if r.hospital_id is None]
            for row in pending:
                row.status, row.error = RowStatus.PENDING, None
            await asyncio.gather(*(self._create(job.batch_id, row) for row in pending))

            if any(r.ambiguous for r in job.rows):
                await self._reconcile(job)

            missing = [r for r in job.rows if r.hospital_id is None]
            if missing:
                job.fail(
                    f"{len(missing)} of {len(job.rows)} hospital(s) failed; batch left inactive. "
                    f"Retry with POST /hospitals/bulk/{job.batch_id}/resume"
                )
                return
            await self._activate(job)
        except UpstreamError as exc:
            job.fail(str(exc))
        except Exception:
            logger.exception("Bulk job %s crashed", job.batch_id)
            job.fail("Internal error while processing batch")
        finally:
            job.finished_at = time.monotonic()
            logger.info("Bulk job %s finished: %s in %.2fs", job.batch_id, job.status.value, job.processing_time_seconds)

    async def _create(self, batch_id: str, row: RowState) -> None:
        try:
            hospital = await self.client.create_hospital(
                name=row.name, address=row.address, phone=row.phone, batch_id=batch_id
            )
        except UpstreamError as exc:
            row.mark_failed(str(exc), ambiguous=exc.ambiguous)
        except Exception:  # one bad row must not take down the batch
            logger.exception("Unexpected error creating row %d of batch %s", row.row, batch_id)
            row.mark_failed("Unexpected error creating hospital", ambiguous=True)
        else:
            row.mark_created(hospital["id"])

    async def _reconcile(self, job: Job) -> None:
        """Align local row state with what upstream actually holds for this batch.

        Rows are matched on (name, address), which the CSV parser guarantees unique per file. Recovers rows whose
        create "failed" but actually landed, and deletes duplicates so the batch matches the CSV exactly.
        """
        upstream: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for hospital in await self.client.list_batch(job.batch_id):
            upstream[_key(hospital["name"], hospital["address"])].append(hospital)

        for row in job.rows:
            matches = sorted(upstream.pop(_key(row.name, row.address), []), key=lambda h: h["id"])
            if not matches:
                if row.hospital_id is not None:
                    row.mark_failed("Hospital missing upstream")
                row.ambiguous = False  # confirmed absent → safe to create
                continue
            keep = next((h for h in matches if h["id"] == row.hospital_id), matches[0])
            row.mark_created(keep["id"])
            for duplicate in matches:
                if duplicate is not keep:
                    logger.warning("Batch %s: deleting duplicate hospital %s (row %d)", job.batch_id, duplicate["id"], row.row)
                    await self.client.delete_hospital(duplicate["id"])

    async def _activate(self, job: Job) -> None:
        try:
            activated = await self.client.activate_batch(job.batch_id)
        except UpstreamError as exc:
            # Upstream answers 400 to re-activation, and a timed-out PATCH may still have applied:
            # the batch listing is the source of truth.
            if not await self._is_live(job):
                job.fail(f"Batch activation failed ({exc}); all hospitals were created and remain inactive. Resume to retry.")
                return
            logger.info("Batch %s: activation reported %s but batch is live", job.batch_id, exc)
        else:
            if activated != len(job.rows):
                logger.warning("Batch %s: upstream activated %d hospitals, expected %d", job.batch_id, activated, len(job.rows))
        for row in job.rows:
            row.status = RowStatus.ACTIVATED
        job.status, job.batch_activated, job.error = JobStatus.COMPLETED, True, None

    async def _is_live(self, job: Job) -> bool:
        try:
            active_ids = {h["id"] for h in await self.client.list_batch(job.batch_id) if h["active"]}
        except UpstreamError:
            return False
        return all(r.hospital_id in active_ids for r in job.rows)
