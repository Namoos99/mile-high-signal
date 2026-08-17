"""Transform unit tests.

Runs against a real local Spark session (no cluster, no S3) with a handful of
synthetic rows. Local-mode Spark starts in ~2-3 seconds, which is why these are
kept in their own module — a `session`-scoped pytest fixture pays that cost once
for the whole file instead of once per test.
"""

from __future__ import annotations

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from spark_jobs.transform import (
    RAW_SCHEMA,
    cast_and_clean,
    clean_service_requests,
    deduplicate,
    derive_fields,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("test-denver311-transform")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _row(**overrides) -> dict:
    base = {
        "OBJECTID": 1,
        "Case_Summary": "Pothole",
        "Case_Status": "New",
        "Case_Source": "Mobile App",
        "Case_Created_Date": 1705314600000,  # 2024-01-15T10:30:00Z
        "Case_Created_dttm": "1/15/2024 10:30:00 AM",
        "Case_Closed_Date": None,
        "Case_Closed_dttm": None,
        "First_Call_Resolution": "N",
        "Customer_Zip_Code": "80202",
        "Incident_Address_1": "500 16th St",
        "Incident_Address_2": None,
        "Incident_Intersection_1": None,
        "Incident_Intersection_2": None,
        "Incident_Zip_Code": "80202",
        "Longitude": -104.99,
        "Latitude": 39.74,
        "Agency": "Public Works",
        "Division": "Streets",
        "Major_Area": "Infrastructure",
        "Type": "Pothole",
        "Topic": "Road Maintenance",
        "Council_District": 10,
        "Police_District": 6,
        "Neighborhood": "CBD",
    }
    base.update(overrides)
    return base


def make_df(spark, rows: list[dict]):
    return spark.createDataFrame([Row(**r) for r in rows], schema=RAW_SCHEMA)


# --------------------------------------------------------------------------
# deduplicate
# --------------------------------------------------------------------------


def test_dedup_removes_repeated_objectid(spark):
    df = make_df(spark, [_row(OBJECTID=1), _row(OBJECTID=1), _row(OBJECTID=2)])
    result = deduplicate(df)
    assert result.count() == 2


def test_dedup_keeps_latest_created_date_on_conflict(spark):
    df = make_df(
        spark,
        [
            _row(OBJECTID=1, Case_Status="New", Case_Created_Date=1000),
            _row(OBJECTID=1, Case_Status="Closed", Case_Created_Date=2000),
        ],
    )
    result = deduplicate(df).collect()
    assert len(result) == 1
    assert result[0]["Case_Status"] == "Closed"


def test_dedup_is_a_noop_when_no_duplicates(spark):
    df = make_df(spark, [_row(OBJECTID=1), _row(OBJECTID=2), _row(OBJECTID=3)])
    assert deduplicate(df).count() == 3


# --------------------------------------------------------------------------
# cast_and_clean
# --------------------------------------------------------------------------


def test_epoch_ms_becomes_a_real_timestamp(spark):
    """Assert on the underlying stored instant (via unix_millis), not on the string
    representation — that string is rendered in the driver JVM's local timezone by
    default regardless of spark.sql.session.timeZone, so asserting on it is flaky
    across machines in different timezones. The underlying instant is what actually
    matters and is what this checks."""
    df = make_df(spark, [_row(Case_Created_Date=1705314600000)])
    result = cast_and_clean(df).select(F.unix_millis("created_at").alias("ms")).collect()[0]
    assert result["ms"] == 1705314600000


def test_null_closed_date_stays_null(spark):
    df = make_df(spark, [_row(Case_Closed_Date=None)])
    result = cast_and_clean(df).collect()[0]
    assert result["closed_at"] is None


def test_blank_string_becomes_null_not_empty_string(spark):
    """A row where the source sent whitespace instead of an actual null — this
    happens on real ArcGIS data and silently breaks downstream null-checks if not
    normalized here."""
    df = make_df(spark, [_row(Neighborhood="   ", Type="")])
    result = cast_and_clean(df).collect()[0]
    assert result["Neighborhood"] is None
    assert result["Type"] is None


def test_real_value_is_trimmed_not_dropped(spark):
    df = make_df(spark, [_row(Agency="  Public Works  ")])
    result = cast_and_clean(df).collect()[0]
    assert result["Agency"] == "Public Works"


# --------------------------------------------------------------------------
# derive_fields
# --------------------------------------------------------------------------


def test_open_case_is_not_closed_and_has_no_resolution_time(spark):
    df = derive_fields(cast_and_clean(make_df(spark, [_row(Case_Closed_Date=None)])))
    result = df.collect()[0]
    assert result["is_closed"] is False
    assert result["resolution_hours"] is None


def test_closed_case_gets_correct_resolution_hours(spark):
    # Created 2024-01-15T10:30:00Z, closed exactly 2 hours later.
    df = derive_fields(
        cast_and_clean(
            make_df(spark, [_row(Case_Created_Date=1705314600000, Case_Closed_Date=1705321800000)])
        )
    )
    result = df.collect()[0]
    assert result["is_closed"] is True
    assert result["resolution_hours"] == pytest.approx(2.0)


def test_internal_digest_email_is_flagged(spark):
    """The exact real-world row shape we found in production data: an internal
    mailbox digest, not a citizen service request."""
    df = derive_fields(
        cast_and_clean(
            make_df(
                spark,
                [_row(Case_Source="Email - Mayor's Office", Case_Summary="End User Digest: 3 New Messages")],
            )
        )
    )
    assert df.collect()[0]["is_likely_internal"] is True


def test_genuine_service_request_is_not_flagged_as_internal(spark):
    df = derive_fields(
        cast_and_clean(make_df(spark, [_row(Case_Source="Mobile App", Case_Summary="Pothole")]))
    )
    assert df.collect()[0]["is_likely_internal"] is False


def test_coordinates_inside_denver_are_valid(spark):
    df = derive_fields(cast_and_clean(make_df(spark, [_row(Latitude=39.74, Longitude=-104.99)])))
    assert df.collect()[0]["has_valid_coordinates"] is True


def test_null_island_coordinates_are_flagged_invalid(spark):
    """(0, 0) is the classic geocoding-failure sentinel value."""
    df = derive_fields(cast_and_clean(make_df(spark, [_row(Latitude=0.0, Longitude=0.0)])))
    assert df.collect()[0]["has_valid_coordinates"] is False


def test_null_coordinates_are_not_flagged_valid_or_invalid_incorrectly(spark):
    df = derive_fields(cast_and_clean(make_df(spark, [_row(Latitude=None, Longitude=None)])))
    assert df.collect()[0]["has_valid_coordinates"] is False


# --------------------------------------------------------------------------
# full pipeline
# --------------------------------------------------------------------------


def test_full_pipeline_end_to_end(spark):
    rows = [
        _row(OBJECTID=1, Case_Created_Date=1000, Case_Status="New"),
        _row(OBJECTID=1, Case_Created_Date=2000, Case_Status="Closed"),  # dup, newer wins
        _row(OBJECTID=2, Case_Source="Email - DLCP", Case_Summary="End User Digest: 1 New Message"),
        _row(OBJECTID=3, Neighborhood="  ", Case_Closed_Date=1705321800000, Case_Created_Date=1705314600000),
    ]
    df = make_df(spark, rows)
    result = clean_service_requests(df)

    assert result.count() == 3  # 4 rows in, 1 duplicate removed

    by_id = {r["OBJECTID"]: r for r in result.collect()}
    assert by_id[1]["Case_Status"] == "Closed"
    assert by_id[2]["is_likely_internal"] is True
    assert by_id[3]["Neighborhood"] is None
    assert by_id[3]["is_closed"] is True
    assert by_id[3]["resolution_hours"] == pytest.approx(2.0)
