"""Bulk API end to end against the fake upstream.

Cases: happy path · bounded concurrency (perf) · invalid CSV rejected before any upstream call ·
oversized upload · partial failure never activates · safe retries (connect error / 503) · retries bounded ·
ambiguous timeout reconciled, never re-POSTed (no duplicates) · unexpected client error isolated per row ·
resume re-creates only failed rows · resume recovers late-landed rows and deletes duplicates ·
activation retried (idempotent) · activation failure resumable without re-creating ·
async mode + progress polling · 404/409/422 on status/resume · discard · validate endpoint · health.
"""

from __future__ import annotations

import time
import uuid

from tests.conftest import csv_file


def upload(client, *names, wait=True):
    return client.post("/hospitals/bulk", params={"wait": str(wait).lower()}, files=csv_file(*names))


def resume(client, batch_id):
    return client.post(f"/hospitals/bulk/{batch_id}/resume")


def test_bulk_create_happy_path(client, fake):
    r = upload(client, "General", "City", "Mercy")
    assert r.status_code == 200
    body = r.json()
    uuid.UUID(body["batch_id"])
    assert body["status"] == "completed"
    assert body["batch_activated"] is True
    assert (body["total_hospitals"], body["processed_hospitals"], body["failed_hospitals"]) == (3, 3, 0)
    assert body["processing_time_seconds"] >= 0
    assert [(h["row"], h["name"], h["status"]) for h in body["hospitals"]] == [
        (1, "General", "created_and_activated"),
        (2, "City", "created_and_activated"),
        (3, "Mercy", "created_and_activated"),
    ]
    members = fake.batch(body["batch_id"])
    assert len(members) == 3 and all(h["active"] for h in members)
    assert sorted(h["hospital_id"] for h in body["hospitals"]) == sorted(h["id"] for h in members)
    assert members[0]["phone"] == "555-0001"
    assert fake.count("PATCH") == 1


def test_rows_are_created_concurrently_within_limit(make_client, fake):
    fake.delay = 0.2
    client = make_client(max_concurrency=5)
    started = time.perf_counter()
    body = upload(client, *[f"H{i}" for i in range(20)]).json()
    elapsed = time.perf_counter() - started
    assert body["batch_activated"] is True
    assert fake.max_inflight == 5  # never exceeds the limit, and actually reaches it
    assert elapsed < 2.0  # sequential would take 20 × 0.2 = 4s; 4 waves of 5 ≈ 0.8s


def test_invalid_csv_rejected_before_any_upstream_call(client, fake):
    r = client.post("/hospitals/bulk", files={"file": ("h.csv", b"name,address\n,1 Main St\n", "text/csv")})
    assert r.status_code == 422
    assert r.json()["valid"] is False
    assert r.json()["errors"] == [{"row": 1, "field": "name", "message": "name is required"}]
    assert fake.calls == []


def test_oversized_upload_rejected(make_client, fake):
    client = make_client(max_upload_bytes=50)
    r = client.post("/hospitals/bulk", files={"file": ("h.csv", b"name,address\n" + b"x" * 100, "text/csv")})
    assert r.status_code == 413
    assert fake.calls == []


def test_partial_failure_leaves_batch_inactive(client, fake):
    fake.create_faults["Bad"] = ["422"]
    r = upload(client, "Good1", "Bad", "Good2")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "failed"
    assert body["batch_activated"] is False
    assert (body["processed_hospitals"], body["failed_hospitals"]) == (2, 1)
    good1, bad, good2 = body["hospitals"]
    assert bad["status"] == "failed" and bad["hospital_id"] is None and "422" in bad["error"]
    assert good1["status"] == good2["status"] == "created"
    assert fake.count("PATCH") == 0
    assert not any(h["active"] for h in fake.batch(body["batch_id"]))
    assert fake.count("POST") == 3  # a 4xx is not retried


def test_failures_before_request_lands_are_retried(client, fake):
    fake.create_faults["Flaky"] = ["connect_error", "503"]
    body = upload(client, "Flaky", "Steady").json()
    assert body["status"] == "completed"
    assert fake.count("POST") == 4  # Flaky: 3 attempts, Steady: 1
    assert len(fake.batch(body["batch_id"])) == 2


def test_retries_are_bounded(client, fake):
    fake.create_faults["Down"] = ["503"] * 10
    body = upload(client, "Down").json()
    assert body["status"] == "failed"
    assert fake.count("POST") == 3


def test_ambiguous_timeout_is_reconciled_not_retried(client, fake):
    fake.create_faults["Slow"] = ["timeout_after_create"]
    body = upload(client, "Slow", "Fast").json()
    assert fake.count("POST") == 2  # never re-POSTed → no duplicate
    assert body["status"] == "completed" and body["batch_activated"] is True
    assert len(fake.batch(body["batch_id"])) == 2
    assert body["hospitals"][0]["status"] == "created_and_activated"
    assert body["hospitals"][0]["hospital_id"] is not None


def test_unexpected_error_is_isolated_to_its_row(client, fake):
    fake.create_faults["Boom"] = ["crash"]
    body = upload(client, "Boom", "Fine").json()
    assert body["status"] == "failed"
    assert body["hospitals"][0]["status"] == "failed"
    assert body["hospitals"][1]["status"] == "created"


def test_resume_recreates_only_failed_rows_then_activates(client, fake):
    fake.create_faults["Bad"] = ["500"]
    first = upload(client, "Good", "Bad").json()
    assert first["status"] == "failed"
    posts_before = fake.count("POST")

    r = resume(client, first["batch_id"])
    assert r.status_code == 200
    body = r.json()
    assert body["batch_id"] == first["batch_id"]
    assert body["status"] == "completed" and body["batch_activated"] is True
    assert fake.count("POST") - posts_before == 1
    assert body["hospitals"][0]["hospital_id"] == first["hospitals"][0]["hospital_id"]
    assert {h["status"] for h in body["hospitals"]} == {"created_and_activated"}
    assert len(fake.batch(first["batch_id"])) == 2


def test_resume_recovers_rows_that_landed_late_and_removes_duplicates(client, fake):
    fake.create_faults["Bad"] = ["500"]
    first = upload(client, "Good", "Bad").json()
    batch_id = first["batch_id"]
    for _ in range(2):  # the "failed" request actually landed — twice
        fake.create({"name": "Bad", "address": "2 Main St", "phone": "555-0002", "creation_batch_id": batch_id})
    posts_before = fake.count("POST")

    body = resume(client, batch_id).json()
    assert body["status"] == "completed"
    assert fake.count("POST") == posts_before  # found upstream, not re-created
    assert len(fake.batch(batch_id)) == 2  # duplicate deleted
    assert body["hospitals"][1]["hospital_id"] in {h["id"] for h in fake.batch(batch_id)}


def test_activation_retried_only_when_request_never_landed(client, fake):
    fake.activate_faults = ["503", "503"]
    body = upload(client, "A").json()
    assert body["status"] == "completed"
    assert fake.count("PATCH") == 3


def test_ambiguous_activation_is_verified_not_retried(client, fake):
    # Regression: upstream returns 400 on re-activation, so blindly retrying a PATCH whose response was lost
    # turned a live batch into a "failed" one.
    fake.activate_faults = ["timeout_after_apply"]
    body = upload(client, "A", "B").json()
    assert body["status"] == "completed" and body["batch_activated"] is True
    assert fake.count("PATCH") == 1


def test_resume_when_activation_silently_applied(client, fake):
    fake.activate_faults = ["timeout_after_apply"]
    fake.list_faults = ["500"] * 3  # verification also fails → reported failed while actually live
    first = upload(client, "A").json()
    assert first["status"] == "failed"
    assert all(h["active"] for h in fake.batch(first["batch_id"]))

    body = resume(client, first["batch_id"]).json()  # upstream answers 400 "already active" → verified live
    assert body["status"] == "completed" and body["batch_activated"] is True
    assert fake.count("POST") == 1


def test_all_rows_failing_then_resume(client, fake):
    fake.create_faults = {"A": ["500"], "B": ["500"]}  # ambiguous → reconcile hits upstream's 404 for an empty batch
    first = upload(client, "A", "B").json()
    assert first["status"] == "failed"
    assert (first["processed_hospitals"], first["failed_hospitals"]) == (0, 2)
    assert fake.count("PATCH") == 0

    body = resume(client, first["batch_id"]).json()
    assert body["status"] == "completed" and body["batch_activated"] is True
    assert len(fake.batch(first["batch_id"])) == 2


def test_activation_failure_is_resumable_without_recreating(client, fake):
    fake.activate_faults = ["500"]
    first = upload(client, "A", "B").json()
    assert first["status"] == "failed" and first["batch_activated"] is False
    assert first["processed_hospitals"] == 2
    assert {h["status"] for h in first["hospitals"]} == {"created"}
    assert "activat" in first["error"].lower()
    posts = fake.count("POST")

    body = resume(client, first["batch_id"]).json()
    assert body["status"] == "completed" and body["batch_activated"] is True
    assert fake.count("POST") == posts


def test_async_mode_returns_202_and_progress_is_pollable(client, fake):
    fake.delay = 0.2
    r = upload(client, "A", "B", "C", wait=False)
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "processing"
    assert r.headers["location"] == f"/hospitals/bulk/{body['batch_id']}"

    deadline = time.monotonic() + 5
    while (status := client.get(r.headers["location"]).json())["status"] == "processing":
        assert time.monotonic() < deadline, status
        time.sleep(0.05)
    assert status["status"] == "completed"
    assert status["processed_hospitals"] == 3


def test_status_and_resume_errors(client, fake):
    unknown = uuid.uuid4()
    assert client.get(f"/hospitals/bulk/{unknown}").status_code == 404
    assert resume(client, unknown).status_code == 404
    assert client.get("/hospitals/bulk/not-a-uuid").status_code == 422
    done = upload(client, "A").json()
    assert resume(client, done["batch_id"]).status_code == 409


def test_discard_deletes_failed_batch(client, fake):
    fake.create_faults["Bad"] = ["422"]
    first = upload(client, "Good", "Bad").json()
    r = client.delete(f"/hospitals/bulk/{first['batch_id']}")
    assert r.status_code == 200
    assert r.json()["status"] == "discarded"
    assert fake.batch(first["batch_id"]) == []
    assert resume(client, first["batch_id"]).status_code == 409


def test_discard_when_nothing_was_created(client, fake):
    fake.create_faults["A"] = ["422"]
    first = upload(client, "A").json()
    r = client.delete(f"/hospitals/bulk/{first['batch_id']}")  # upstream answers 404 for the empty batch
    assert r.status_code == 200 and r.json()["status"] == "discarded"


def test_discard_refuses_completed_batch(client, fake):
    done = upload(client, "A").json()
    assert client.delete(f"/hospitals/bulk/{done['batch_id']}").status_code == 409
    assert len(fake.batch(done["batch_id"])) == 1


def test_validate_endpoint_never_calls_upstream(client, fake):
    ok = client.post("/hospitals/bulk/validate", files=csv_file("A", "B"))
    assert ok.status_code == 200
    assert ok.json()["valid"] is True and ok.json()["total_rows"] == 2 and ok.json()["errors"] == []

    bad = client.post("/hospitals/bulk/validate", files={"file": ("h.csv", b"name,address\nA,\n", "text/csv")})
    assert bad.status_code == 200
    assert bad.json()["valid"] is False
    assert bad.json()["errors"] == [{"row": 1, "field": "address", "message": "address is required"}]
    assert fake.calls == []


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}
