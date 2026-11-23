"""CSV parsing/validation — pure function, no network.

Cases: happy path · header case/whitespace/BOM · phone column absent · blank phone → None ·
quoted commas · blank lines skipped · empty file · header only · missing required column ·
unknown column · empty name/address · too many values · row-limit boundary (20 ok, 21 rejected) ·
non-UTF-8 · duplicate rows · phone format.
"""

from app.csv_parser import HospitalRow, parse_hospitals_csv


def parse(text: str, **kw):
    return parse_hospitals_csv(text.encode("utf-8"), **kw)


def messages(result):
    return [e.message for e in result.errors]


def test_happy_path_with_optional_phone():
    result = parse("name,address,phone\nGeneral Hospital,1 Main St,555-1234\nCity Clinic,2 Oak Ave,\n")
    assert result.ok, result.errors
    assert result.rows == [
        HospitalRow(row=1, name="General Hospital", address="1 Main St", phone="555-1234"),
        HospitalRow(row=2, name="City Clinic", address="2 Oak Ave", phone=None),
    ]


def test_header_is_case_insensitive_trimmed_and_bom_tolerant():
    data = "﻿ Name , ADDRESS ,Phone\n  General  ,  1 Main St  , 555-1234 \n".encode("utf-8")
    result = parse_hospitals_csv(data)
    assert result.ok, result.errors
    assert result.rows == [HospitalRow(row=1, name="General", address="1 Main St", phone="555-1234")]


def test_phone_column_is_optional():
    result = parse("address,name\n1 Main St,General\n")
    assert result.ok, result.errors
    assert result.rows[0].phone is None
    assert result.rows[0].name == "General"


def test_quoted_fields_and_blank_lines():
    result = parse('name,address\n\n"Mercy, North","12 Main St, Suite 5"\n,\n\nSt. Luke,9 Elm\n')
    assert result.ok, result.errors
    assert [(r.row, r.name, r.address) for r in result.rows] == [
        (1, "Mercy, North", "12 Main St, Suite 5"),
        (2, "St. Luke", "9 Elm"),
    ]


def test_empty_file():
    assert messages(parse("")) == ["CSV file is empty"]
    assert messages(parse("  \n\n")) == ["CSV file is empty"]


def test_header_only():
    assert messages(parse("name,address,phone\n")) == ["CSV has no hospital rows"]


def test_missing_required_column():
    result = parse("name,phone\nGeneral,555-1234\n")
    assert not result.ok
    assert "Missing required column(s): address" in messages(result)[0]


def test_unknown_column():
    result = parse("name,address,email\nGeneral,1 Main St,a@b.c\n")
    assert "Unknown column(s): email" in messages(result)[0]


def test_duplicate_header_column():
    result = parse("name,address,name\nA,B,C\n")
    assert "Duplicate column(s): name" in messages(result)[0]


def test_empty_name_and_address_are_row_errors():
    result = parse("name,address\n  ,1 Main St\nGeneral,\n")
    assert [(e.row, e.field) for e in result.errors] == [(1, "name"), (2, "address")]
    assert result.rows == []


def test_too_many_values_in_row():
    result = parse("name,address\nGeneral,1 Main St,extra\n")
    assert [(e.row, e.message) for e in result.errors] == [(1, "Row has 3 values; header has 2 columns")]


def test_row_limit_boundary():
    header = "name,address\n"
    ok = parse(header + "".join(f"H{i},Addr {i}\n" for i in range(20)))
    assert ok.ok and len(ok.rows) == 20
    over = parse(header + "".join(f"H{i},Addr {i}\n" for i in range(21)))
    assert messages(over) == ["CSV has 21 hospital rows; maximum is 20"]
    assert over.rows == []


def test_custom_row_limit():
    assert messages(parse("name,address\nA,B\nC,D\n", max_rows=1)) == ["CSV has 2 hospital rows; maximum is 1"]


def test_non_utf8_rejected():
    result = parse_hospitals_csv("name,address\nCafé,1 Rue\n".encode("latin-1"))
    assert messages(result) == ["File must be UTF-8 encoded text"]


def test_duplicate_rows_rejected_case_insensitively():
    result = parse("name,address\nGeneral,1 Main St\ngeneral , 1 MAIN ST\n")
    assert [(e.row, e.message) for e in result.errors] == [(2, "Duplicate of row 1 (same name and address)")]


def test_phone_formats():
    good = ["555-1234", "+1 (555) 123-4567", "080.1234.5678", "555-1234 ext 12", "555 1234 x9"]
    for phone in good:
        result = parse(f"name,address,phone\nGeneral,1 Main St,{phone}\n")
        assert result.ok, (phone, result.errors)
    for phone in ["N/A", "call us", "12", "+++---"]:
        result = parse(f"name,address,phone\nGeneral,1 Main St,{phone}\n")
        assert [(e.row, e.field) for e in result.errors] == [(1, "phone")], phone
