"""Integration test against the real, shared Hospital Directory API. Opt-in: RUN_LIVE_TESTS=1 pytest.

Always deletes the batch it creates.
"""

import os
import uuid

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app

pytestmark = pytest.mark.skipif(os.getenv("RUN_LIVE_TESTS") != "1", reason="set RUN_LIVE_TESTS=1 to hit the real upstream")


def test_end_to_end_against_real_upstream():
    settings = Settings.from_env()
    tag = uuid.uuid4().hex[:8]
    csv = "name,address,phone\n" + "".join(f"Live Test {tag} {i},{i} Test Ave,555-01{i:02d}\n" for i in range(3))
    with TestClient(create_app(settings)) as client:
        body = client.post("/hospitals/bulk", files={"file": ("h.csv", csv, "text/csv")}).json()
    try:
        assert body["status"] == "completed", body
        assert body["batch_activated"] is True
        upstream = httpx.get(f"{settings.upstream_base_url}/hospitals/batch/{body['batch_id']}", timeout=60).json()
        assert sorted(h["id"] for h in upstream) == sorted(h["hospital_id"] for h in body["hospitals"])
        assert all(h["active"] for h in upstream)
    finally:
        if "batch_id" in body:
            httpx.delete(f"{settings.upstream_base_url}/hospitals/batch/{body['batch_id']}", timeout=60)
