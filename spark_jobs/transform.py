"""Clean and enrich raw 311 service-request records.

Design choice: this module is pure PySpark transform logic with no I/O. Every
function takes a DataFrame and returns a DataFrame. That's what makes it testable
with a handful of in-memory rows instead of a live Spark cluster reading from S3 —
see tests/test_transform.py, which runs these functions in a local Spark session
against synthetic data in under a second.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# The schema we insist on at the Spark boundary. Raw NDJSON is schema-agnostic by
# design (see AD-003), but once data enters Spark we want a fixed, typed shape —
# this IS the schema-drift detector: if ArcGIS adds/renames/retypes a field,
# reading raw JSON against this schema either coerces cleanly or nulls out the
# mismatched column, and a data-quality check downstream can catch the latter.
RAW_SCHEMA = T.StructType(
    [
        T.StructField("OBJECTID", T.LongType(), True),
        T.StructField("Case_Summary", T.StringType(), True),
        T.StructField("Case_Status", T.StringType(), True),
        T.StructField("Case_Source", T.StringType(), True),
        T.StructField("Case_Created_Date", T.LongType(), True),  # epoch ms
        T.StructField("Case_Created_dttm", T.StringType(), True),
        T.StructField("Case_Closed_Date", T.LongType(), True),  # epoch ms
        T.StructField("Case_Closed_dttm", T.StringType(), True),
        T.StructField("First_Call_Resolution", T.StringType(), True),
        T.StructField("Customer_Zip_Code", T.StringType(), True),
        T.StructField("Incident_Address_1", T.StringType(), True),
        T.StructField("Incident_Address_2", T.StringType(), True),
        T.StructField("Incident_Intersection_1", T.StringType(), True),
        T.StructField("Incident_Intersection_2", T.StringType(), True),
        T.StructField("Incident_Zip_Code", T.StringType(), True),
        T.StructField("Longitude", T.DoubleType(), True),
        T.StructField("Latitude", T.DoubleType(), True),
        T.StructField("Agency", T.StringType(), True),
        T.StructField("Division", T.StringType(), True),
        T.StructField("Major_Area", T.StringType(), True),
        T.StructField("Type", T.StringType(), True),
        T.StructField("Topic", T.StringType(), True),
        T.StructField("Council_District", T.IntegerType(), True),
        T.StructField("Police_District", T.IntegerType(), True),
        T.StructField("Neighborhood", T.StringType(), True),
    ]
)

# Denver's approximate bounding box, generous padding included. Used only to flag
# (not drop) coordinates that are obviously wrong — e.g. null-island (0, 0), or a
# geocoding error that placed a request in another state.
DENVER_LAT_RANGE = (39.5, 40.0)
DENVER_LON_RANGE = (-105.2, -104.6)


def get_spark(app_name: str = "denver311-clean") -> SparkSession:
    return SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC").getOrCreate()


def deduplicate(df: DataFrame) -> DataFrame:
    """Drop exact-duplicate OBJECTIDs, keeping the row with the latest
    Case_Created_Date (arbitrary but deterministic tiebreak).

    OBJECTID is our natural key because the source has no true case number — see
    docs/DECISIONS.md AD-011. This handles the at-least-once duplication that comes
    from re-reading the same window after a crash (see AD-005).
    """
    window_rank = F.row_number().over(
        Window.partitionBy("OBJECTID").orderBy(F.col("Case_Created_Date").desc_nulls_last())
    )
    return df.withColumn("_rn", window_rank).where(F.col("_rn") == 1).drop("_rn")


def cast_and_clean(df: DataFrame) -> DataFrame:
    """Type conversions and basic hygiene: epoch-ms -> timestamp, blank strings ->
    null, whitespace trimmed. Nothing here drops rows."""

    def blank_to_null(col: str) -> F.Column:
        trimmed = F.trim(F.col(col))
        return F.when((trimmed == "") | trimmed.isNull(), None).otherwise(trimmed)

    string_cols = [
        "Case_Summary",
        "Case_Status",
        "Case_Source",
        "Incident_Address_1",
        "Incident_Address_2",
        "Agency",
        "Division",
        "Major_Area",
        "Type",
        "Topic",
        "Neighborhood",
    ]
    out = df
    for c in string_cols:
        out = out.withColumn(c, blank_to_null(c))

    return out.withColumn("created_at", F.timestamp_millis(F.col("Case_Created_Date"))).withColumn(
        "closed_at", F.timestamp_millis(F.col("Case_Closed_Date"))
    )


def derive_fields(df: DataFrame) -> DataFrame:
    """Add analytical columns that don't exist on the source but are cheap to
    compute once and expensive to recompute in every downstream dashboard query."""
    return (
        df.withColumn("is_closed", F.col("closed_at").isNotNull())
        .withColumn(
            "resolution_hours",
            F.when(
                F.col("closed_at").isNotNull(),
                (F.col("closed_at").cast("long") - F.col("created_at").cast("long")) / 3600.0,
            ),
        )
        .withColumn(
            # See docs/DECISIONS.md AD-012: some rows in this feed are internal
            # system notifications, not public service requests. We flag rather
            # than drop, so downstream consumers can decide whether to include
            # them rather than losing that judgment call inside a Spark job.
            "is_likely_internal",
            (F.col("Case_Source").rlike("(?i)^email") | F.col("Case_Summary").rlike("(?i)end user digest")),
        )
        .withColumn(
            "has_valid_coordinates",
            F.coalesce(
                F.col("Latitude").between(*DENVER_LAT_RANGE) & F.col("Longitude").between(*DENVER_LON_RANGE),
                F.lit(False),
            ),
        )
    )


def clean_service_requests(df: DataFrame) -> DataFrame:
    """Full pipeline: dedup -> cast/clean -> derive. Composition lives in one place
    so the CLI entrypoint and the tests exercise identical logic."""
    return derive_fields(cast_and_clean(deduplicate(df)))
