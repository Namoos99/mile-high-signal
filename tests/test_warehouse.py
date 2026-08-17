"""Warehouse logic tests that don't require a live Postgres connection.

These test the pure functions — date-key math and the NaN/NaT sanitization that
a live-database test (test_warehouse_integration.py) actually caught bugs in.
Keeping these separate means the fast, no-dependency tests run in CI (which has
no Postgres service configured — see AD-014 in docs/DECISIONS.md), while the
integration tests are for local/manual verification against a real database.
"""

from __future__ import annotations

import math
from datetime import date

import pandas as pd
import pytest
from warehouse.load_warehouse import (
    _clean_float,
    _clean_int,
    _clean_str,
    _clean_timestamp,
    date_dimension_row,
    date_key,
)


def test_date_key_format():
    assert date_key(date(2026, 8, 17)) == 20260817


def test_date_key_pads_single_digit_month_and_day():
    assert date_key(date(2026, 1, 5)) == 20260105


def test_date_dimension_row_shape():
    row = date_dimension_row(date(2026, 8, 17))  # a Monday
    assert row["date_key"] == 20260817
    assert row["year"] == 2026
    assert row["month"] == 8
    assert row["day"] == 17
    assert row["is_weekend"] is False


def test_date_dimension_row_flags_saturday_as_weekend():
    row = date_dimension_row(date(2026, 8, 15))  # a Saturday
    assert row["is_weekend"] is True


# --------------------------------------------------------------------------
# NaN/NaT sanitization — this is the exact bug class a live-Postgres run caught:
# pandas represents "missing" differently depending on column dtype, and every
# one of those representations breaks a naive SQL parameter binding differently.
# --------------------------------------------------------------------------


def test_clean_str_passes_through_real_values():
    assert _clean_str("Public Works") == "Public Works"


def test_clean_str_converts_pandas_nan_to_none():
    assert _clean_str(float("nan")) is None


def test_clean_str_passes_through_none():
    assert _clean_str(None) is None


def test_clean_int_converts_nan_to_none():
    assert _clean_int(float("nan")) is None


def test_clean_int_converts_real_float_to_int():
    assert _clean_int(10.0) == 10


def test_clean_float_converts_nan_to_none():
    assert _clean_float(float("nan")) is None


def test_clean_float_passes_through_real_value():
    assert _clean_float(2.5) == 2.5


def test_clean_timestamp_converts_nat_to_none():
    assert _clean_timestamp(pd.NaT) is None


def test_clean_timestamp_converts_real_pandas_timestamp():
    ts = pd.Timestamp("2026-08-17 10:30:00")
    result = _clean_timestamp(ts)
    assert result.year == 2026
    assert result.hour == 10


def test_clean_timestamp_passes_through_none():
    assert _clean_timestamp(None) is None


def test_isnan_guard_does_not_choke_on_strings():
    """A defensive check: math.isnan() raises TypeError on a string, and an
    earlier version of this cleaning logic didn't guard against that."""
    with pytest.raises(TypeError):
        math.isnan("Public Works")  # documents why _clean_str can't use isnan directly
    assert _clean_str("Public Works") == "Public Works"  # but the real function handles it
