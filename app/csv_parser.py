"""CSV parsing and validation. Pure: bytes in, rows + errors out — no I/O, so it is shared by
POST /hospitals/bulk and POST /hospitals/bulk/validate and unit-tested without a server."""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field

REQUIRED_COLUMNS = ("name", "address")
OPTIONAL_COLUMNS = ("phone",)
_PHONE = re.compile(r"^\+?[\d\s().-]+(?:\s*(?:x|ext\.?)\s*\d{1,6})?$", re.IGNORECASE)


@dataclass(frozen=True)
class HospitalRow:
    row: int  # 1-based position among data rows (header excluded)
    name: str
    address: str
    phone: str | None = None


@dataclass(frozen=True)
class RowError:
    row: int | None  # None = file-level problem
    message: str
    field: str | None = None


@dataclass
class ParseResult:
    rows: list[HospitalRow] = field(default_factory=list)  # only meaningful when `ok`
    errors: list[RowError] = field(default_factory=list)
    total_rows: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors


def _file_error(message: str) -> ParseResult:
    return ParseResult(errors=[RowError(row=None, message=message)])


def _valid_phone(phone: str) -> bool:
    return bool(_PHONE.match(phone)) and sum(c.isdigit() for c in phone) >= 3


def parse_hospitals_csv(data: bytes, max_rows: int = 20) -> ParseResult:
    """Parse `name,address[,phone]` CSV (header required, any column order).

    All-or-nothing: any error means the file must not be processed, so a batch is never half-valid.
    """
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return _file_error("File must be UTF-8 encoded text")

    try:
        records = [r for r in csv.reader(io.StringIO(text)) if any(c.strip() for c in r)]
    except csv.Error as exc:
        return _file_error(f"Malformed CSV: {exc}")
    if not records:
        return _file_error("CSV file is empty")

    header = [c.strip().lower() for c in records[0]]
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    unknown = [c for c in header if c not in REQUIRED_COLUMNS + OPTIONAL_COLUMNS]
    dupes = sorted({c for c in header if header.count(c) > 1})
    problems = [
        f"{label}: {', '.join(cols)}"
        for label, cols in (("Missing required column(s)", missing), ("Unknown column(s)", unknown), ("Duplicate column(s)", dupes))
        if cols
    ]
    if problems:
        return _file_error("; ".join(problems) + ". Expected header: name,address,phone (phone optional)")

    data_rows = records[1:]
    if not data_rows:
        return _file_error("CSV has no hospital rows")
    result = ParseResult(total_rows=len(data_rows))
    if len(data_rows) > max_rows:
        result.errors.append(RowError(None, f"CSV has {len(data_rows)} hospital rows; maximum is {max_rows}"))
        return result

    seen: dict[tuple[str, str], int] = {}
    for row_no, record in enumerate(data_rows, start=1):
        if len(record) > len(header):
            result.errors.append(RowError(row_no, f"Row has {len(record)} values; header has {len(header)} columns"))
            continue
        values = dict(zip(header, (c.strip() for c in record)))
        name, address, phone = values.get("name", ""), values.get("address", ""), values.get("phone") or None

        row_errors = [RowError(row_no, f"{col} is required", col) for col, val in (("name", name), ("address", address)) if not val]
        if phone and not _valid_phone(phone):
            row_errors.append(RowError(row_no, f"Invalid phone number: {phone!r}", "phone"))
        if not row_errors:
            # (name, address) must be unique per file: it is the key used to reconcile with upstream.
            key = (name.casefold(), address.casefold())
            if key in seen:
                row_errors.append(RowError(row_no, f"Duplicate of row {seen[key]} (same name and address)"))
            seen.setdefault(key, row_no)

        if row_errors:
            result.errors.extend(row_errors)
        else:
            result.rows.append(HospitalRow(row_no, name, address, phone))
    return result
