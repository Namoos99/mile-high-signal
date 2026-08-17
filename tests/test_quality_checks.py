"""Data quality gate tests.

The important property to prove here isn't just "checks run without crashing" —
it's that a HARD failure actually raises and stops the pipeline, and a WARNING
condition does not. Several tests below construct deliberately broken data to
confirm the gate catches it, not just that it passes on good data.
"""

from __future__ import annotations

from datetime import UTC

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import types as T
from spark_jobs.quality_checks import (
    DataQualityError,
    QualityReport,
    check_business_key_not_null,
    check_business_key_unique,
    check_closed_before_created,
    check_coordinate_validity_rate,
    check_created_at_not_null,
    check_internal_notification_rate,
    check_schema,
    run_quality_checks,
)

TEST_SCHEMA = T.StructType(
    [
        T.StructField("OBJECTID", T.LongType(), True),
        T.StructField("created_at", T.TimestampType(), True),
        T.StructField("closed_at", T.TimestampType(), True),
        T.StructField("is_closed", T.BooleanType(), True),
        T.StructField("resolution_hours", T.DoubleType(), True),
        T.StructField("is_likely_internal", T.BooleanType(), True),
        T.StructField("has_valid_coordinates", T.BooleanType(), True),
        T.StructField("Latitude", T.DoubleType(), True),
        T.StructField("Longitude", T.DoubleType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("test-quality-checks")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


GOOD_COLUMNS = [
    "OBJECTID",
    "created_at",
    "closed_at",
    "is_closed",
    "resolution_hours",
    "is_likely_internal",
    "has_valid_coordinates",
    "Latitude",
    "Longitude",
]


def _good_row(**overrides):
    from datetime import datetime

    base = {
        "OBJECTID": 1,
        "created_at": datetime(2026, 8, 1, tzinfo=UTC),
        "closed_at": datetime(2026, 8, 2, tzinfo=UTC),
        "is_closed": True,
        "resolution_hours": 24.0,
        "is_likely_internal": False,
        "has_valid_coordinates": True,
        "Latitude": 39.74,
        "Longitude": -104.99,
    }
    base.update(overrides)
    return Row(**base)


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=TEST_SCHEMA)


# --------------------------------------------------------------------------
# Schema check
# --------------------------------------------------------------------------


def test_schema_check_passes_with_all_required_columns(spark):
    df = make_df(spark, [_good_row()])
    report = QualityReport()
    check_schema(df, report)
    assert report.hard_failures == []


def test_schema_check_fails_when_column_missing(spark):
    minimal_schema = T.StructType([T.StructField("OBJECTID", T.LongType(), True)])
    df = spark.createDataFrame([Row(OBJECTID=1)], schema=minimal_schema)
    report = QualityReport()
    check_schema(df, report)
    assert len(report.hard_failures) == 1
    assert "Missing required columns" in report.hard_failures[0]


# --------------------------------------------------------------------------
# Business key checks
# --------------------------------------------------------------------------


def test_business_key_not_null_passes_on_good_data(spark):
    df = make_df(spark, [_good_row(OBJECTID=1), _good_row(OBJECTID=2)])
    report = QualityReport()
    check_business_key_not_null(df, report)
    assert report.hard_failures == []


def test_business_key_not_null_catches_null_objectid(spark):
    df = make_df(spark, [_good_row(OBJECTID=1), _good_row(OBJECTID=None)])
    report = QualityReport()
    check_business_key_not_null(df, report)
    assert len(report.hard_failures) == 1
    assert "null OBJECTID" in report.hard_failures[0]


def test_business_key_unique_passes_on_deduped_data(spark):
    df = make_df(spark, [_good_row(OBJECTID=1), _good_row(OBJECTID=2)])
    report = QualityReport()
    check_business_key_unique(df, report)
    assert report.hard_failures == []


def test_business_key_unique_catches_a_dedup_regression(spark):
    """This is the scenario that matters: if transform.py's dedup logic ever
    regresses, this check is what catches it before corrupted duplicate facts
    reach the warehouse."""
    df = make_df(spark, [_good_row(OBJECTID=1), _good_row(OBJECTID=1)])
    report = QualityReport()
    check_business_key_unique(df, report)
    assert len(report.hard_failures) == 1
    assert "not unique" in report.hard_failures[0]


# --------------------------------------------------------------------------
# Timestamp integrity
# --------------------------------------------------------------------------


def test_created_at_not_null_passes_on_good_data(spark):
    df = make_df(spark, [_good_row()])
    report = QualityReport()
    check_created_at_not_null(df, report)
    assert report.hard_failures == []


def test_created_at_not_null_catches_missing_timestamp(spark):
    df = make_df(spark, [_good_row(created_at=None)])
    report = QualityReport()
    check_created_at_not_null(df, report)
    assert len(report.hard_failures) == 1


def test_closed_before_created_passes_on_valid_ordering(spark):
    df = make_df(spark, [_good_row()])  # closed 1 day after created
    report = QualityReport()
    check_closed_before_created(df, report)
    assert report.hard_failures == []


def test_closed_before_created_catches_impossible_ordering(spark):
    """A case that closed before it opened. Almost certainly a timestamp bug
    upstream, and exactly the kind of thing that quietly wrecks a resolution
    time chart if it reaches the warehouse."""
    from datetime import datetime

    df = make_df(
        spark,
        [
            _good_row(
                created_at=datetime(2026, 8, 5, tzinfo=UTC),
                closed_at=datetime(2026, 8, 1, tzinfo=UTC),
            )
        ],
    )
    report = QualityReport()
    check_closed_before_created(df, report)
    assert len(report.hard_failures) == 1


def test_open_case_with_null_closed_at_is_not_flagged(spark):
    """A currently-open case has no closed_at yet — that's normal, not a
    violation of ordering."""
    df = make_df(spark, [_good_row(closed_at=None, is_closed=False)])
    report = QualityReport()
    check_closed_before_created(df, report)
    assert report.hard_failures == []


# --------------------------------------------------------------------------
# Warning-tier checks: must warn, must NOT hard-fail
# --------------------------------------------------------------------------


def test_internal_rate_below_threshold_produces_no_warning(spark):
    rows = [_good_row(OBJECTID=i, is_likely_internal=False) for i in range(10)]
    df = make_df(spark, rows)
    report = QualityReport()
    check_internal_notification_rate(df, report)
    assert report.warnings == []
    assert report.hard_failures == []


def test_internal_rate_above_threshold_warns_but_does_not_hard_fail(spark):
    """50% internal is above the 40% threshold — should warn, and critically,
    should NOT appear in hard_failures, since this tier must never block a
    load on its own."""
    rows = [_good_row(OBJECTID=i, is_likely_internal=(i % 2 == 0)) for i in range(10)]
    df = make_df(spark, rows)
    report = QualityReport()
    check_internal_notification_rate(df, report)
    assert len(report.warnings) == 1
    assert report.hard_failures == []


def test_coordinate_validity_below_threshold_warns_only(spark):
    rows = [
        _good_row(OBJECTID=i, has_valid_coordinates=(i < 5), Latitude=39.7, Longitude=-104.9)
        for i in range(10)
    ]
    df = make_df(spark, rows)
    report = QualityReport()
    check_coordinate_validity_rate(df, report)
    assert len(report.warnings) == 1
    assert report.hard_failures == []


def test_rows_without_attempted_coordinates_are_excluded_from_the_rate(spark):
    """A row with no Latitude at all (never geocoded) shouldn't count against
    the validity rate the same way a row with a bad Latitude should."""
    rows = [_good_row(OBJECTID=1, Latitude=None, Longitude=None, has_valid_coordinates=False)]
    df = make_df(spark, rows)
    report = QualityReport()
    check_coordinate_validity_rate(df, report)
    assert report.warnings == []  # nothing to warn about — no coordinates were attempted


# --------------------------------------------------------------------------
# Full gate: the property that actually matters
# --------------------------------------------------------------------------


def test_run_quality_checks_passes_clean_data(spark):
    df = make_df(spark, [_good_row(OBJECTID=1), _good_row(OBJECTID=2)])
    report = run_quality_checks(df)
    assert report.passed is True
    assert report.hard_failures == []


def test_run_quality_checks_raises_on_hard_failure(spark):
    """The property that matters most: a hard failure must actually raise, not
    just get logged and ignored — this is what makes it a gate rather than a
    report."""
    df = make_df(spark, [_good_row(OBJECTID=1), _good_row(OBJECTID=1)])  # duplicate
    with pytest.raises(DataQualityError, match="not unique"):
        run_quality_checks(df)


def test_run_quality_checks_does_not_raise_on_warnings_only(spark):
    """The other half of the property: a warning-tier issue must NOT raise —
    otherwise every batch with normal internal-notification noise would halt
    the pipeline."""
    rows = [_good_row(OBJECTID=i, is_likely_internal=True) for i in range(10)]
    df = make_df(spark, rows)
    report = run_quality_checks(df)  # must not raise
    assert report.passed is True
    assert len(report.warnings) == 1
