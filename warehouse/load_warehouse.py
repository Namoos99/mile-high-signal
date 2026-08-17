"""Load cleaned service-request data into the Postgres star schema.

Pure functions for the SQL-shaping logic (build_date_key, dimension upserts as
SQL text) are separated from the psycopg2 I/O so the shaping logic is testable
without a live database — see tests/test_warehouse.py for the parts that don't
need Postgres, and tests/test_warehouse_integration.py for the parts that do.

IDEMPOTENCY: every load is a upsert keyed on `object_id` (see schema.sql).
Re-running the loader against the same processed data twice produces the same
row count both times — this matters because Airflow retries a failed task by
re-running it, and a loader that isn't idempotent would double-count on retry.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

logger = logging.getLogger(__name__)


def date_key(d: date) -> int:
    """YYYYMMDD integer key. Simple, sortable, and joinable without a date cast."""
    return d.year * 10000 + d.month * 100 + d.day


def date_dimension_row(d: date) -> dict[str, Any]:
    return {
        "date_key": date_key(d),
        "full_date": d,
        "year": d.year,
        "month": d.month,
        "day": d.day,
        "day_of_week": (d.weekday() + 1) % 7,  # Python: Mon=0 -> we want Sun=0
        "is_weekend": d.weekday() >= 5,
    }


@dataclass
class LoadStats:
    dim_agency_upserted: int = 0
    dim_service_type_upserted: int = 0
    dim_neighborhood_upserted: int = 0
    dim_date_upserted: int = 0
    facts_upserted: int = 0


def get_or_create_dimension_key(
    cursor, table: str, key_col: str, unique_cols: list[str], values: dict[str, Any]
) -> int | None:
    """Generic upsert-and-return-key for a dimension row.

    Uses ON CONFLICT DO NOTHING followed by a SELECT rather than a single
    RETURNING clause, because ON CONFLICT ... RETURNING doesn't return the row
    on a no-op conflict — we'd get NULL on the second and every subsequent
    insert of the same value, which is most of them once the dimension fills
    in.
    """
    if any(v is None for v in values.values() if unique_cols and list(values.keys())):
        pass  # allow nulls in non-unique columns; unique_cols themselves are checked below

    if any(values.get(c) is None for c in unique_cols):
        return None  # can't dimension an unknown value — fact row stores NULL FK

    cols = list(values.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    conflict_cols = ", ".join(unique_cols)

    cursor.execute(
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({conflict_cols}) DO NOTHING",
        [values[c] for c in cols],
    )
    where_clause = " AND ".join(f"{c} = %s" for c in unique_cols)
    cursor.execute(f"SELECT {key_col} FROM {table} WHERE {where_clause}", [values[c] for c in unique_cols])
    row = cursor.fetchone()
    return row[0] if row else None


def _clean_str(v: Any) -> str | None:
    """pandas represents a missing string column as float NaN after a parquet
    round-trip, not None or empty string. Left uncleaned, that NaN gets passed to
    psycopg2 as a float and blows up any comparison against a text column — this
    is exactly the bug a live-Postgres integration test caught before it could
    reach production."""
    if v is None:
        return None
    try:
        import math

        if isinstance(v, float) and math.isnan(v):
            return None
    except TypeError:
        pass
    return v


def _clean_timestamp(v: Any) -> datetime | None:
    """pandas represents a missing timestamp as NaT after a parquet round-trip —
    a distinct sentinel from NaN, and Postgres rejects it outright rather than
    treating it as NULL. Same failure family as _clean_str, different type."""
    if v is None:
        return None
    if hasattr(v, "to_pydatetime"):
        import pandas as pd

        if pd.isna(v):
            return None
        return v.to_pydatetime()
    return v


def load_dataframe(conn, pdf, stats: LoadStats | None = None) -> LoadStats:
    """Load a pandas DataFrame (the Spark job's cleaned output) into the warehouse.

    Takes a pandas DataFrame rather than a Spark DataFrame deliberately — by
    the time data reaches the warehouse it's small enough (thousands of rows
    per run, not millions) that pandas + psycopg2 executemany is simpler and
    faster to reason about than a Spark JDBC writer, and it keeps this module
    testable without a Spark session.
    """
    stats = stats or LoadStats()
    cursor = conn.cursor()

    for _, row in pdf.iterrows():
        agency_key = None
        agency_name = _clean_str(row.get("Agency"))
        if agency_name:
            agency_key = get_or_create_dimension_key(
                cursor, "dim_agency", "agency_key", ["agency_name"], {"agency_name": agency_name}
            )
            stats.dim_agency_upserted += 1

        service_type_key = None
        type_name = _clean_str(row.get("Type"))
        topic_name = _clean_str(row.get("Topic"))
        if type_name or topic_name:
            service_type_key = get_or_create_dimension_key(
                cursor,
                "dim_service_type",
                "service_type_key",
                ["type_name", "topic_name"],
                {"type_name": type_name, "topic_name": topic_name},
            )
            stats.dim_service_type_upserted += 1

        neighborhood_key = None
        neighborhood_name = _clean_str(row.get("Neighborhood"))
        if neighborhood_name:
            neighborhood_key = get_or_create_dimension_key(
                cursor,
                "dim_neighborhood",
                "neighborhood_key",
                ["neighborhood_name"],
                {
                    "neighborhood_name": neighborhood_name,
                    "council_district": _clean_int(row.get("Council_District")),
                    "police_district": _clean_int(row.get("Police_District")),
                },
            )
            stats.dim_neighborhood_upserted += 1

        created_at = _clean_timestamp(row["created_at"])
        created_date_key = None
        if created_at is not None:
            drow = date_dimension_row(created_at.date())
            cursor.execute(
                "INSERT INTO dim_date (date_key, full_date, year, month, day, day_of_week, is_weekend) "
                "VALUES (%(date_key)s, %(full_date)s, %(year)s, %(month)s, %(day)s, %(day_of_week)s, "
                "%(is_weekend)s) ON CONFLICT (date_key) DO NOTHING",
                drow,
            )
            created_date_key = drow["date_key"]
            stats.dim_date_upserted += 1

        closed_at = _clean_timestamp(row.get("closed_at"))

        cursor.execute(
            """
            INSERT INTO fact_service_request (
                object_id, agency_key, service_type_key, neighborhood_key,
                created_date_key, created_at, closed_at, case_status, case_source,
                is_closed, resolution_hours, is_likely_internal, has_valid_coordinates,
                latitude, longitude, incident_address
            ) VALUES (
                %(object_id)s, %(agency_key)s, %(service_type_key)s, %(neighborhood_key)s,
                %(created_date_key)s, %(created_at)s, %(closed_at)s, %(case_status)s, %(case_source)s,
                %(is_closed)s, %(resolution_hours)s, %(is_likely_internal)s, %(has_valid_coordinates)s,
                %(latitude)s, %(longitude)s, %(incident_address)s
            )
            ON CONFLICT (object_id) DO UPDATE SET
                case_status = EXCLUDED.case_status,
                closed_at = EXCLUDED.closed_at,
                is_closed = EXCLUDED.is_closed,
                resolution_hours = EXCLUDED.resolution_hours,
                loaded_at = now()
            """,
            {
                "object_id": int(row["OBJECTID"]),
                "agency_key": agency_key,
                "service_type_key": service_type_key,
                "neighborhood_key": neighborhood_key,
                "created_date_key": created_date_key,
                "created_at": created_at,
                "closed_at": closed_at,
                "case_status": _clean_str(row.get("Case_Status")),
                "case_source": _clean_str(row.get("Case_Source")),
                "is_closed": bool(row["is_closed"]),
                "resolution_hours": _clean_float(row.get("resolution_hours")),
                "is_likely_internal": bool(row["is_likely_internal"]),
                "has_valid_coordinates": bool(row["has_valid_coordinates"]),
                "latitude": _clean_float(row.get("Latitude")),
                "longitude": _clean_float(row.get("Longitude")),
                "incident_address": _clean_str(row.get("Incident_Address_1")),
            },
        )
        stats.facts_upserted += 1

    conn.commit()
    cursor.close()
    return stats


def _clean_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        import math

        if isinstance(v, float) and math.isnan(v):
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def _clean_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        import math

        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load cleaned parquet into the Postgres warehouse.")
    parser.add_argument("--parquet-dir", default=None, help="Local directory of parquet files to load.")
    args = parser.parse_args(argv)

    from denver311.common.config import get_settings
    from denver311.common.logging_setup import configure_logging

    settings = get_settings()
    configure_logging(settings.log_level)

    import pandas as pd
    import psycopg2

    parquet_dir = args.parquet_dir or "output/processed_parquet"
    pdf = pd.read_parquet(parquet_dir)
    logger.info("Loading %d rows from %s", len(pdf), parquet_dir)

    conn = psycopg2.connect(
        host=settings.warehouse_host,
        port=settings.warehouse_port,
        dbname=settings.warehouse_db,
        user=settings.warehouse_user,
        password=settings.warehouse_password,
    )
    try:
        with open(Path(__file__).parent / "schema.sql") as f:
            conn.cursor().execute(f.read())
        conn.commit()

        stats = load_dataframe(conn, pdf)
        logger.info("=== load complete: %s ===", stats)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
