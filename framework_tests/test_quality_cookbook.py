"""The example rules in framework_ingestion/QUALITY_RULES.md: each is accepted by the validator,
and each has sample values that must pass and sample values that must fail.

CASES is also run on a real Spark session once, to prove the rules behave as the doc says
(see the doc's "Try a rule before you deploy it"). Here, the part that needs no Spark runs.
"""

from __future__ import annotations

import re

import pytest

from lakeflow_framework.quality import assert_safe_rule_expression


def case(name, expr, good, bad, types=None, pattern=None):
    """``good`` and ``bad`` are lists of {column: value}. ``types`` gives a column its SQL type
    (default string). ``pattern`` marks a rule that is one ``rlike``, so Python can check it too."""

    return {"name": name, "expr": expr, "good": good, "bad": bad, "types": types or {}, "pattern": pattern}


CASES = [
    # Presence
    case(
        "present",
        "invoice_id IS NOT NULL AND length(trim(invoice_id)) > 0",
        [{"invoice_id": "INV-1"}],
        [{"invoice_id": ""}, {"invoice_id": "   "}, {"invoice_id": None}],
    ),
    # Regex
    case(
        "regex_format",
        "invoice_id rlike '^INV-[0-9]{6}$'",
        [{"invoice_id": "INV-000123"}],
        [{"invoice_id": "INV-12"}, {"invoice_id": "xINV-000123"}, {"invoice_id": "INV-000123x"}],
        pattern="^INV-[0-9]{6}$",
    ),
    case(
        "regex_email",
        "email rlike '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}$'",
        [{"email": "a.b@x.com"}, {"email": "first+tag@mail.example.org"}],
        [{"email": "a@b"}, {"email": "a b@x.com"}, {"email": "@x.com"}],
        pattern="^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}$",
    ),
    case(
        "regex_date_text",
        "order_date rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'",
        [{"order_date": "2026-11-19"}],
        [{"order_date": "19/11/2026"}, {"order_date": "2026-1-9"}],
        pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    ),
    case(
        "regex_ignore_case",
        "status rlike '(?i)^(new|open|closed)$'",
        [{"status": "NEW"}, {"status": "Open"}, {"status": "closed"}],
        [{"status": "opened"}, {"status": "x"}],
        pattern="(?i)^(new|open|closed)$",
    ),
    case(
        "regex_timestamp_text",
        "started_at rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$'",
        [{"started_at": "2026-11-19 10:00:00"}, {"started_at": "2026-11-19T10:00:00"}],
        [{"started_at": "2026-11-19"}, {"started_at": "19/11/2026 10:00"}],
        pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$",
    ),
    case(
        "regex_uuid",
        "request_id rlike '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'",
        [{"request_id": "0f8fad5b-d9cb-469f-a165-70867728950e"}],
        [{"request_id": "0F8FAD5B-D9CB-469F-A165-70867728950E"}, {"request_id": "not-a-uuid"}],
        pattern="^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    ),
    case(
        "regex_postcode",
        "postcode rlike '^[0-9]{4}$'",
        [{"postcode": "2000"}],
        [{"postcode": "200"}, {"postcode": "20000"}, {"postcode": "ABCD"}],
        pattern="^[0-9]{4}$",
    ),
    case(
        "regex_digits_only",
        "code rlike '^[0-9]+$'",
        [{"code": "123"}],
        [{"code": "12a"}, {"code": ""}, {"code": None}],
        pattern="^[0-9]+$",
    ),
    case(
        "regex_number_text",
        "lat rlike '^-?[0-9]+([.][0-9]+)?$'",
        [{"lat": "-40.7"}, {"lat": "40"}],
        [{"lat": "40."}, {"lat": "N/A"}],
        pattern="^-?[0-9]+([.][0-9]+)?$",
    ),
    case(
        "regex_fixed_id",
        "ride_id rlike '^[0-9A-Fa-f]{16}$'",
        [{"ride_id": "811C6F5F6C3C16E7"}],
        [{"ride_id": "811C6F5F6C3C16E"}, {"ride_id": "ZZZC6F5F6C3C16E7"}],
        pattern="^[0-9A-Fa-f]{16}$",
    ),
    case(
        "regex_latitude",
        "lat rlike '^-?(([0-8]?[0-9])([.][0-9]+)?|90([.]0+)?)$'",
        [{"lat": "40.7"}, {"lat": "90"}, {"lat": "-90.0"}],
        [{"lat": "90.5"}, {"lat": "91"}],
        pattern="^-?(([0-8]?[0-9])([.][0-9]+)?|90([.]0+)?)$",
    ),
    case(
        "optional_not_blank",
        "end_station IS NULL OR length(trim(end_station)) > 0",
        [{"end_station": None}, {"end_station": "A"}],
        [{"end_station": "  "}],
    ),
    case(
        "regex_unanchored_matches_anywhere",
        "code rlike '[0-9]{3}'",
        [{"code": "123"}, {"code": "abc123xyz"}],
        [{"code": "ab12"}],
        pattern="[0-9]{3}",
    ),
    case(
        "regex_optional",
        "phone IS NULL OR phone rlike '^[+]?[0-9]{8,15}$'",
        [{"phone": None}, {"phone": "+61412345678"}, {"phone": "0412345678"}],
        [{"phone": "04-12"}, {"phone": "abc"}],
    ),
    case(
        "regex_extract",
        "length(regexp_extract(code, '^([A-Z]{3})-[0-9]+$', 1)) = 3",
        [{"code": "ABC-12"}],
        [{"code": "ab-1"}, {"code": "ABCD-1"}],
    ),
    case(
        "regex_absent",
        "NOT name rlike '[0-9]'",
        [{"name": "Alice"}],
        [{"name": "Al1ce"}, {"name": None}],
    ),
    case(
        "regex_only_these_characters",
        "length(regexp_replace(sku, '[^A-Z0-9]', '')) = length(sku)",
        [{"sku": "AB12"}],
        [{"sku": "AB-12"}, {"sku": "ab12"}],
    ),
    case("no_padding", "name = trim(name)", [{"name": "Alice"}], [{"name": " Alice"}]),
    case(
        "upper_case",
        "country_code = upper(country_code)",
        [{"country_code": "AU"}],
        [{"country_code": "au"}],
    ),
    # Length
    case(
        "length_range",
        "length(name) BETWEEN 1 AND 10",
        [{"name": "Alice"}],
        [{"name": ""}, {"name": "ABCDEFGHIJK"}],
    ),
    # Lists
    case(
        "list_in",
        "status IN ('NEW', 'OPEN', 'CLOSED')",
        [{"status": "NEW"}],
        [{"status": "new"}, {"status": "X"}],
    ),
    case(
        "list_in_any_case",
        "upper(status) IN ('NEW', 'OPEN', 'CLOSED')",
        [{"status": "new"}],
        [{"status": "x"}],
    ),
    case(
        "list_not_in",
        "status NOT IN ('TEST', 'DUMMY')",
        [{"status": "NEW"}],
        [{"status": "TEST"}],
    ),
    case(
        "starts_with",
        "startswith(code, 'AU-')",
        [{"code": "AU-1"}],
        [{"code": "NZ-1"}],
    ),
    case(
        "free_text_clean",
        "note IS NULL OR NOT contains(note, 'TODO')",
        [{"note": None}, {"note": "all good"}],
        [{"note": "TODO fix"}],
    ),
    # Numbers (a column declared with a number type)
    case(
        "number_not_negative",
        "amount >= 0",
        [{"amount": 0}, {"amount": 10.5}],
        [{"amount": -1}, {"amount": None}],
        types={"amount": "DECIMAL(18,2)"},
    ),
    case(
        "number_range",
        "amount BETWEEN 0 AND 1000000",
        [{"amount": 999999}],
        [{"amount": 1000001}],
        types={"amount": "DECIMAL(18,2)"},
    ),
    case(
        "number_two_decimals",
        "round(amount, 2) = amount",
        [{"amount": 10.25}],
        [{"amount": 10.255}],
        types={"amount": "DECIMAL(18,4)"},
    ),
    case(
        "number_positive_whole",
        "quantity > 0",
        [{"quantity": 1}],
        [{"quantity": 0}],
        types={"quantity": "INT"},
    ),
    case(
        "number_adds_up",
        "abs(line_total - quantity * unit_price) < 0.01",
        [{"line_total": 20.00, "quantity": 2, "unit_price": 10.00}],
        [{"line_total": 25.00, "quantity": 2, "unit_price": 10.00}],
        types={"line_total": "DECIMAL(12,2)", "quantity": "INT", "unit_price": "DECIMAL(10,2)"},
    ),
    # Dates and times held as text
    case(
        "date_valid",
        "try_cast(order_date AS date) IS NOT NULL",
        [{"order_date": "2026-11-19"}],
        [{"order_date": "2026-13-45"}, {"order_date": "abc"}, {"order_date": None}],
    ),
    case(
        "date_not_in_the_future",
        "try_cast(order_date AS date) <= current_date()",
        [{"order_date": "2020-01-01"}],
        [{"order_date": "2999-01-01"}],
    ),
    case(
        "date_year_range",
        "year(try_cast(order_date AS date)) BETWEEN 2000 AND 2100",
        [{"order_date": "2026-01-01"}],
        [{"order_date": "1999-12-31"}],
    ),
    case(
        "date_not_too_old",
        "datediff(current_date(), try_cast(order_date AS date)) <= 3650",
        [{"order_date": "2026-01-01"}],
        [{"order_date": "1990-01-01"}],
    ),
    case(
        "timestamp_valid",
        "try_cast(started_at AS timestamp) IS NOT NULL",
        [{"started_at": "2026-11-19 10:00:00"}],
        [{"started_at": "nope"}],
    ),
    # Across columns
    case(
        "end_not_before_start",
        "try_cast(ended_at AS timestamp) >= try_cast(started_at AS timestamp)",
        [{"started_at": "2026-11-19 10:00:00", "ended_at": "2026-11-19 11:00:00"}],
        [{"started_at": "2026-11-19 11:00:00", "ended_at": "2026-11-19 10:00:00"}],
    ),
    case(
        "required_when",
        "CASE WHEN status = 'CLOSED' THEN closed_at IS NOT NULL ELSE true END",
        [{"status": "CLOSED", "closed_at": "2026-11-19"}, {"status": "OPEN", "closed_at": None}],
        [{"status": "CLOSED", "closed_at": None}],
    ),
    case(
        "all_or_none",
        "(city IS NULL) = (postcode IS NULL)",
        [{"city": None, "postcode": None}, {"city": "Sydney", "postcode": "2000"}],
        [{"city": "Sydney", "postcode": None}],
    ),
]


def _columns(item) -> set[str]:
    return {name for row in (*item["good"], *item["bad"]) for name in row}


@pytest.mark.parametrize("item", CASES, ids=[item["name"] for item in CASES])
def test_every_example_rule_is_accepted_by_the_validator(item) -> None:
    errors: list[str] = []
    assert_safe_rule_expression(
        item["expr"], _columns(item), label="doc", name=item["name"], errors=errors
    )
    assert errors == []


def test_every_example_has_values_that_pass_and_values_that_fail() -> None:
    names = [item["name"] for item in CASES]
    assert len(names) == len(set(names))
    for item in CASES:
        assert item["good"] and item["bad"], item["name"]


@pytest.mark.parametrize(
    "item", [i for i in CASES if i["pattern"]], ids=[i["name"] for i in CASES if i["pattern"]]
)
def test_a_single_regex_rule_means_what_the_doc_says(item) -> None:
    column = next(iter(_columns(item)))
    pattern = re.compile(item["pattern"])
    assert all(pattern.search(row[column]) for row in item["good"])
    assert not any(row[column] is not None and pattern.search(row[column]) for row in item["bad"])


def test_what_the_validator_refuses_is_what_the_doc_warns_about() -> None:
    def refused(expression: str) -> bool:
        errors: list[str] = []
        assert_safe_rule_expression(expression, {"a"}, label="doc", name="x", errors=errors)
        return bool(errors)

    assert refused("a rlike 'x'; DROP")  # a second statement
    assert refused("a IS NOT NULL -- comment")  # a comment
    assert refused("a rlike '^(add|remove)$'")  # a denied word, even inside the pattern
    assert refused("a IN (SELECT b FROM t)")  # a subquery
    assert refused("try_cast(a AS double) > 0")  # a type name that is not approved
    assert refused("b IS NOT NULL")  # a column the table does not have
    assert refused("upper_case_all(a)")  # a function that is not approved
    assert not refused("a rlike '^[A-Z]{3}$'")
