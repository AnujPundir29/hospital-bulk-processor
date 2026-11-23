"""API response models."""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.csv_parser import ParseResult
from app.jobs import Job, JobStatus, RowStatus


class HospitalResult(BaseModel):
    row: int = Field(description="1-based data row in the CSV (header excluded)")
    hospital_id: int | None
    name: str
    status: RowStatus
    error: str | None = None


class BulkResult(BaseModel):
    batch_id: str
    status: JobStatus
    total_hospitals: int
    processed_hospitals: int = Field(description="Rows that exist upstream")
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    error: str | None = None
    hospitals: list[HospitalResult]

    @classmethod
    def from_job(cls, job: Job) -> BulkResult:
        return cls(
            batch_id=job.batch_id,
            status=job.status,
            total_hospitals=len(job.rows),
            processed_hospitals=job.processed,
            failed_hospitals=job.failed,
            processing_time_seconds=job.processing_time_seconds,
            batch_activated=job.batch_activated,
            error=job.error,
            hospitals=[
                HospitalResult(row=r.row, hospital_id=r.hospital_id, name=r.name, status=r.status, error=r.error)
                for r in job.rows
            ],
        )


class ValidationIssue(BaseModel):
    row: int | None = Field(description="1-based data row; null for file-level problems")
    field: str | None
    message: str


class ValidationReport(BaseModel):
    valid: bool
    total_rows: int
    errors: list[ValidationIssue]

    @classmethod
    def from_parse(cls, parsed: ParseResult) -> ValidationReport:
        return cls(
            valid=parsed.ok,
            total_rows=parsed.total_rows,
            errors=[ValidationIssue(row=e.row, field=e.field, message=e.message) for e in parsed.errors],
        )
