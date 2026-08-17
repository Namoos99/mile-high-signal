"""Data quality gate applied between the Spark transform and the warehouse load.

Two severity tiers (see docs/DECISIONS.md AD-015 for the reasoning):
  - HARD FAILS raise and stop the pipeline. Reserved for problems that mean the
    data literally cannot be loaded correctly — a null business key, a schema
    that doesn't match what the loader expects.
  - WARNINGS are logged and included in the run's quality report, but don't
    block the load. Reserved for real but non-fatal issues — a spike in null
    rates, an unusual proportion of flagged-internal rows — where blocking
    every run over a shifting-but-not-broken distribution would make the
    pipeline more fragile than the data problem it's guarding against.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = {
    "OBJECTID",
    "created_at",
    "is_closed",
    "resolution_hours",
    "is_likely_internal",
    "has_valid_coordinates",
}

# If more than this fraction of rows are flagged internal, something has likely
# changed upstream (e.g. Denver started routing a new mail queue through this
# feed) and a human should look, even though it's not fatal on its own.
MAX_INTERNAL_RATE_WARNING = 0.40


class DataQualityError(RuntimeError):
    """Raised on a hard-fail check. Stops the pipeline — see module docstring."""


@dataclass
class QualityReport:
    row_count: int = 0
    passed: bool = True
    hard_failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "row_count": self.row_count,
            "passed": self.passed,
            "hard_failures": self.hard_failures,
            "warnings": self.warnings,
        }


def check_schema(df: DataFrame, report: QualityReport) -> None:
    """Hard fail: the columns the warehouse loader depends on must exist. This is
    the schema-drift detector promised in transform.py's RAW_SCHEMA comment —
    if ArcGIS silently drops a field we rely on, this catches it here rather than
    as a cryptic KeyError three modules downstream in the loader."""
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        report.hard_failures.append(f"Missing required columns: {sorted(missing)}")


def check_business_key_not_null(df: DataFrame, report: QualityReport) -> None:
    """Hard fail: OBJECTID is our only unique identifier (AD-011) — a null here
    means a row can never be upserted correctly."""
    null_count = df.where(F.col("OBJECTID").isNull()).count()
    if null_count > 0:
        report.hard_failures.append(f"{null_count} rows have a null OBJECTID")


def check_business_key_unique(df: DataFrame, report: QualityReport) -> None:
    """Hard fail: the Spark dedup step should guarantee this, so a violation
    here means the transform's dedup logic itself is broken, not just messy
    source data — this check exists to catch a regression in transform.py,
    not to catch normal source noise."""
    total = df.count()
    distinct = df.select("OBJECTID").distinct().count()
    if total != distinct:
        report.hard_failures.append(
            f"OBJECTID is not unique after dedup: {total} rows, {distinct} distinct IDs"
        )


def check_created_at_not_null(df: DataFrame, report: QualityReport) -> None:
    """Hard fail: every row must have a creation timestamp — it's the watermark
    field (AD-*) and the warehouse's date dimension join key. A null here means
    a row can't be placed in time at all."""
    null_count = df.where(F.col("created_at").isNull()).count()
    if null_count > 0:
        report.hard_failures.append(f"{null_count} rows have a null created_at")


def check_closed_before_created(df: DataFrame, report: QualityReport) -> None:
    """Hard fail: a case closed before it was created is not messy data, it's
    impossible data — almost certainly a timestamp parsing bug rather than a
    real source anomaly, and the kind of thing that quietly corrupts a
    resolution-time dashboard if allowed through."""
    bad = df.where(F.col("closed_at").isNotNull() & (F.col("closed_at") < F.col("created_at"))).count()
    if bad > 0:
        report.hard_failures.append(f"{bad} rows have closed_at before created_at")


def check_internal_notification_rate(df: DataFrame, report: QualityReport) -> None:
    """Warning only (see AD-012): these rows are expected in every batch, so this
    checks the *rate*, not the presence — a sudden jump suggests something
    changed upstream worth a human look, but isn't itself corrupted data."""
    total = df.count()
    if total == 0:
        return
    internal = df.where(F.col("is_likely_internal")).count()
    rate = internal / total
    if rate > MAX_INTERNAL_RATE_WARNING:
        report.warnings.append(
            f"{rate:.0%} of rows flagged is_likely_internal (threshold {MAX_INTERNAL_RATE_WARNING:.0%})"
        )


def check_coordinate_validity_rate(df: DataFrame, report: QualityReport) -> None:
    """Warning only: some rows (internal notifications, phone requests without a
    geocoded address) legitimately have no coordinates. Flag an unusual rate,
    don't block on it."""
    with_coords_attempted = df.where(F.col("Latitude").isNotNull()).count()
    if with_coords_attempted == 0:
        return
    valid = df.where(F.col("Latitude").isNotNull() & F.col("has_valid_coordinates")).count()
    rate = valid / with_coords_attempted
    if rate < 0.90:
        report.warnings.append(f"Only {rate:.0%} of rows with coordinates have valid Denver-area coordinates")


ALL_CHECKS = [
    check_schema,
    check_business_key_not_null,
    check_business_key_unique,
    check_created_at_not_null,
    check_closed_before_created,
    check_internal_notification_rate,
    check_coordinate_validity_rate,
]


def run_quality_checks(df: DataFrame) -> QualityReport:
    """Run every check and return a report. Raises DataQualityError if any hard
    check failed — the caller (run_transform.py / the Airflow task) doesn't need
    to know which checks exist, only whether the gate passed."""
    report = QualityReport(row_count=df.count())

    for check in ALL_CHECKS:
        check(df, report)

    report.passed = len(report.hard_failures) == 0

    if not report.passed:
        raise DataQualityError(
            f"Data quality gate failed with {len(report.hard_failures)} hard failure(s): "
            f"{report.hard_failures}"
        )

    return report
