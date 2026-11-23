"""Test fixtures: an in-memory fake of the Hospital Directory API, wired in via httpx.MockTransport.

The fake mirrors upstream behaviour verified against the live API: batch members start inactive, unknown
batches return 404 on GET/PATCH/DELETE, activate returns {"activated_count": n}, re-activation returns 400.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


class FakeDirectory:
    """Fake upstream. Faults are queued per hospital name (create) or globally (activate) and consumed one per call."""

    def __init__(self) -> None:
        self.hospitals: dict[int, dict] = {}
        self.next_id = 100
        self.delay = 0.0
        self.calls: list[tuple[str, str]] = []
        self.create_faults: dict[str, list[str]] = {}
        self.activate_faults: list[str] = []
        self.list_faults: list[str] = []
        self.inflight = 0
        self.max_inflight = 0

    # --- helpers for assertions -------------------------------------------------------
    def count(self, method: str) -> int:
        return sum(1 for m, _ in self.calls if m == method)

    def batch(self, batch_id: str) -> list[dict]:
        return [h for h in self.hospitals.values() if h["creation_batch_id"] == batch_id]

    def create(self, body: dict) -> dict:
        hospital = {
            "id": self.next_id,
            "name": body["name"],
            "address": body["address"],
            "phone": body.get("phone"),
            "creation_batch_id": body.get("creation_batch_id"),
            "active": body.get("creation_batch_id") is None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.hospitals[self.next_id] = hospital
        self.next_id += 1
        return hospital

    # --- transport handler ------------------------------------------------------------
    async def handler(self, request: httpx.Request) -> httpx.Response:
        method, path = request.method, request.url.path
        self.calls.append((method, path))

        if method == "POST" and path == "/hospitals/":
            body = json.loads(request.content)
            fault = _pop(self.create_faults.get(body["name"]))
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
            try:
                await asyncio.sleep(self.delay)
            finally:
                self.inflight -= 1
            if fault == "connect_error":
                raise httpx.ConnectError("connection refused", request=request)
            if fault == "crash":
                raise RuntimeError("unexpected client failure")
            if fault and fault.isdigit():
                return httpx.Response(int(fault), json={"detail": "injected failure"})
            hospital = self.create(body)
            if fault == "timeout_after_create":  # request landed, response lost → ambiguous
                raise httpx.ReadTimeout("read timed out", request=request)
            return httpx.Response(200, json=hospital)

        match = re.fullmatch(r"/hospitals/batch/([^/]+)(/activate)?", path)
        if match:
            batch_id, activate = match.groups()
            members = self.batch(batch_id)
            not_found = httpx.Response(404, json={"detail": "No hospitals found with the specified batch ID"})
            if method == "PATCH" and activate:
                fault = _pop(self.activate_faults)
                if fault and fault.isdigit():
                    return httpx.Response(int(fault), json={"detail": "injected failure"})
                if not members:
                    return not_found
                if any(h["active"] for h in members):  # real upstream rejects re-activation
                    return httpx.Response(400, json={"detail": "Cannot activate batch: one or more hospitals in the batch are already active"})
                for hospital in members:
                    hospital["active"] = True
                if fault == "timeout_after_apply":  # activation applied, response lost → ambiguous
                    raise httpx.ReadTimeout("read timed out", request=request)
                return httpx.Response(200, json={"activated_count": len(members), "message": "ok"})
            if method == "GET":
                fault = _pop(self.list_faults)
                if fault:
                    return httpx.Response(int(fault), json={"detail": "injected failure"})
                return httpx.Response(200, json=members) if members else not_found
            if not members:
                return not_found
            if method == "DELETE":
                for hospital in members:
                    del self.hospitals[hospital["id"]]
                return httpx.Response(200, json={"deleted_count": len(members), "message": "ok"})

        match = re.fullmatch(r"/hospitals/(\d+)", path)
        if match and method == "DELETE":
            if self.hospitals.pop(int(match.group(1)), None) is None:
                return httpx.Response(404, json={"detail": "Hospital not found"})
            return httpx.Response(204)

        return httpx.Response(404, json={"detail": "Not Found"})


def _pop(queue: list[str] | None) -> str | None:
    return queue.pop(0) if queue else None


def csv_file(*names: str) -> dict:
    """Multipart payload with one row per name: '<name>,<i> Main St,555-000<i>' (1-based i)."""
    rows = "".join(f"{name},{i} Main St,555-000{i}\n" for i, name in enumerate(names, start=1))
    return {"file": ("hospitals.csv", "name,address,phone\n" + rows, "text/csv")}


@pytest.fixture
def fake() -> FakeDirectory:
    return FakeDirectory()


@pytest.fixture
def make_client(fake):
    opened: list[TestClient] = []

    def _make(**overrides) -> TestClient:
        settings = Settings(**{"upstream_base_url": "http://upstream.test", "retry_backoff_seconds": 0.0, **overrides})
        client = TestClient(create_app(settings, transport=httpx.MockTransport(fake.handler)))
        client.__enter__()
        opened.append(client)
        return client

    yield _make
    for client in opened:
        client.__exit__(None, None, None)


@pytest.fixture
def client(make_client) -> TestClient:
    return make_client()
