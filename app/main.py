from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from uuid import UUID

import httpx
from fastapi import FastAPI, File, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import JSONResponse, RedirectResponse

from app.config import Settings
from app.csv_parser import ParseResult, parse_hospitals_csv
from app.jobs import Job, JobStore
from app.processor import BulkProcessor, Conflict
from app.schemas import BulkResult, ValidationReport
from app.upstream import HospitalDirectoryClient, UpstreamError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

DESCRIPTION = """
Bulk-creates hospitals from a CSV (`name,address,phone`, phone optional, max 20 rows) through the
[Hospital Directory API](https://hospital-directory.onrender.com/docs).

Rows are created **concurrently** under one batch id; the batch is **activated only if every row succeeds**,
otherwise it stays inactive and can be **resumed** (only missing rows are retried) or **discarded**.
Use `?wait=false` to get `202` immediately and poll `GET /hospitals/bulk/{batch_id}` for progress.
"""

WAIT_QUERY = Query(True, description="true: respond when processing finishes. false: respond 202 at once and poll.")


def create_app(settings: Settings | None = None, transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    """`transport` lets tests swap the real upstream for an in-memory fake."""
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # One pooled client for the app's lifetime: keep-alive connections are reused across rows and jobs.
        async with httpx.AsyncClient(
            base_url=settings.upstream_base_url,
            timeout=httpx.Timeout(settings.upstream_timeout_seconds, connect=10.0),
            limits=httpx.Limits(max_connections=settings.max_concurrency),
            transport=transport,
        ) as http:
            client = HospitalDirectoryClient(
                http,
                max_concurrency=settings.max_concurrency,
                max_attempts=settings.max_attempts,
                backoff_seconds=settings.retry_backoff_seconds,
            )
            app.state.processor = BulkProcessor(client, JobStore(settings.max_jobs_in_memory))
            yield

    app = FastAPI(title="Hospital Bulk Processor", version="1.0.0", description=DESCRIPTION, lifespan=lifespan)

    def processor(request: Request) -> BulkProcessor:
        return request.app.state.processor

    def find_job(request: Request, batch_id: UUID) -> Job:
        job = processor(request).store.get(str(batch_id))
        if job is None:
            raise HTTPException(404, f"Unknown batch_id {batch_id}")
        return job

    async def read_csv(file: UploadFile) -> ParseResult:
        data = await file.read(settings.max_upload_bytes + 1)
        if len(data) > settings.max_upload_bytes:
            raise HTTPException(413, f"File exceeds {settings.max_upload_bytes} bytes")
        return parse_hospitals_csv(data, max_rows=settings.max_csv_rows)

    async def respond(job: Job, task: asyncio.Task, wait: bool, response: Response) -> BulkResult:
        if wait:
            await asyncio.shield(task)  # a client disconnect must not cancel a half-created batch
        else:
            response.status_code = 202
            response.headers["Location"] = f"/hospitals/bulk/{job.batch_id}"
        return BulkResult.from_job(job)

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse("/docs")

    @app.get("/health", tags=["ops"])
    async def health() -> dict:
        return {"status": "ok"}

    @app.post(
        "/hospitals/bulk",
        response_model=BulkResult,
        tags=["bulk"],
        summary="Bulk create hospitals from CSV",
        responses={202: {"description": "Accepted (wait=false); poll the Location header"},
                   413: {"description": "File too large"},
                   422: {"model": ValidationReport, "description": "CSV failed validation; nothing was sent upstream"}},
    )
    async def bulk_create(request: Request, response: Response, file: UploadFile = File(...), wait: bool = WAIT_QUERY):
        parsed = await read_csv(file)
        if not parsed.ok:
            return JSONResponse(status_code=422, content=ValidationReport.from_parse(parsed).model_dump())
        job = processor(request).submit(parsed.rows)
        return await respond(job, processor(request).start(job), wait, response)

    @app.post("/hospitals/bulk/validate", response_model=ValidationReport, tags=["bulk"], summary="Validate a CSV without processing it")
    async def validate_csv(file: UploadFile = File(...)):
        return ValidationReport.from_parse(await read_csv(file))

    @app.get("/hospitals/bulk/{batch_id}", response_model=BulkResult, tags=["bulk"], summary="Batch status and progress")
    async def get_status(batch_id: UUID, request: Request):
        return BulkResult.from_job(find_job(request, batch_id))

    @app.post("/hospitals/bulk/{batch_id}/resume", response_model=BulkResult, tags=["bulk"], summary="Retry a failed batch",
              responses={409: {"description": "Batch is not in a resumable (failed) state"}})
    async def resume(batch_id: UUID, request: Request, response: Response, wait: bool = WAIT_QUERY):
        job = find_job(request, batch_id)
        try:
            task = processor(request).resume(job)
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from exc
        return await respond(job, task, wait, response)

    @app.delete("/hospitals/bulk/{batch_id}", response_model=BulkResult, tags=["bulk"], summary="Discard a failed batch",
                responses={409: {"description": "Only failed (inactive) batches can be discarded"}})
    async def discard(batch_id: UUID, request: Request):
        job = find_job(request, batch_id)
        try:
            await processor(request).discard(job)
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except UpstreamError as exc:
            raise HTTPException(502, str(exc)) from exc
        return BulkResult.from_job(job)

    return app


app = create_app()
