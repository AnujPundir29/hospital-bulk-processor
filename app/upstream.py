"""Async client for the Hospital Directory API.

Retry policy — two upstream writes are NOT idempotent (verified against the live API):
  * POST /hospitals/ has no idempotency key, so a blind retry can duplicate a hospital.
  * PATCH /hospitals/batch/{id}/activate returns 400 if the batch is already active.
So:
  * GET/DELETE: retried on any transient failure (transport error, 429, 5xx).
  * POST/PATCH: retried only when the request provably never reached the app (connect failure, 429, 503).
    Anything else ambiguous (read timeout, 500/502/504) is raised with `ambiguous=True`; the processor
    then checks GET /hospitals/batch/{id} for what actually happened instead of guessing.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_NOT_PROCESSED_STATUSES = {429, 503}  # rejected before the app handled it → safe to retry even a POST
_NOT_SENT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)


class UpstreamError(Exception):
    def __init__(self, message: str, *, status_code: int | None = None, ambiguous: bool = False) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.ambiguous = ambiguous  # True → the request may have been applied upstream


class HospitalDirectoryClient:
    def __init__(self, http: httpx.AsyncClient, *, max_concurrency: int, max_attempts: int, backoff_seconds: float) -> None:
        self._http = http
        # One semaphore for the whole process: bounds upstream load across all concurrent bulk jobs.
        self._slots = asyncio.Semaphore(max_concurrency)
        self._max_attempts = max_attempts
        self._backoff = backoff_seconds

    async def create_hospital(self, *, name: str, address: str, phone: str | None, batch_id: str) -> dict[str, Any]:
        payload = {"name": name, "address": address, "phone": phone, "creation_batch_id": batch_id}
        return (await self._request("POST", "/hospitals/", json=payload)).json()

    async def list_batch(self, batch_id: str) -> list[dict[str, Any]]:
        resp = await self._request("GET", f"/hospitals/batch/{batch_id}", allow_404=True)
        return [] if resp.status_code == 404 else resp.json()  # upstream returns 404, not [], for an empty batch

    async def activate_batch(self, batch_id: str) -> int:
        return (await self._request("PATCH", f"/hospitals/batch/{batch_id}/activate")).json()["activated_count"]

    async def delete_batch(self, batch_id: str) -> int:
        resp = await self._request("DELETE", f"/hospitals/batch/{batch_id}", allow_404=True)
        return 0 if resp.status_code == 404 else resp.json()["deleted_count"]

    async def delete_hospital(self, hospital_id: int) -> None:
        await self._request("DELETE", f"/hospitals/{hospital_id}", allow_404=True)

    async def _request(self, method: str, path: str, *, allow_404: bool = False, **kwargs: Any) -> httpx.Response:
        idempotent = method in ("GET", "DELETE")
        for attempt in range(1, self._max_attempts + 1):
            try:
                async with self._slots:
                    resp = await self._http.request(method, path, **kwargs)
            except _NOT_SENT_ERRORS as exc:
                error = UpstreamError(f"{method} {path}: upstream unreachable ({type(exc).__name__})")
            except httpx.TransportError as exc:
                error = UpstreamError(f"{method} {path}: no response ({type(exc).__name__})", ambiguous=True)
                if not idempotent:
                    raise error from exc
            else:
                if resp.status_code < 400 or (allow_404 and resp.status_code == 404):
                    return resp
                status = resp.status_code
                retryable = status in _NOT_PROCESSED_STATUSES or (idempotent and status >= 500)
                error = UpstreamError(
                    f"{method} {path}: HTTP {status} {_detail(resp)}",
                    status_code=status,
                    ambiguous=status >= 500 and status not in _NOT_PROCESSED_STATUSES,
                )
                if not retryable:
                    raise error

            if attempt < self._max_attempts:
                delay = self._backoff * 2 ** (attempt - 1) * (1 + random.random())
                logger.warning("%s (attempt %d/%d), retrying in %.2fs", error, attempt, self._max_attempts, delay)
                await asyncio.sleep(delay)
        raise error


def _detail(resp: httpx.Response) -> str:
    try:
        detail = resp.json().get("detail", "")
    except (ValueError, AttributeError):
        detail = resp.text
    return str(detail)[:300]
