"""SQL queries backing the dashboard, kept separate from dashboard/app.py's UI code.

Every function here takes a connection and returns a pandas DataFrame — no
Streamlit imports in this file. That's what makes it possible to unit test the
actual SQL against a real Postgres instance (tests/test_dashboard_data.py) without
spinning up a browser or a Streamlit server.
"""

from __future__ import annotations

import pandas as pd


def total_requests(conn) -> int:
    return pd.read_sql("SELECT COUNT(*) AS n FROM fact_service_request WHERE NOT is_likely_internal", conn)[
        "n"
    ].iloc[0]


def requests_by_agency(conn) -> pd.DataFrame:
    """The headline query: volume and resolution time per agency. Internal
    notifications (AD-012) are excluded — they were never citizen requests, and
    including them would understate every agency's real resolution performance."""
    return pd.read_sql(
        """
        SELECT
            a.agency_name,
            COUNT(*) AS total_requests,
            SUM(CASE WHEN f.is_closed THEN 1 ELSE 0 END) AS closed_requests,
            ROUND(AVG(f.resolution_hours)::numeric, 1) AS avg_resolution_hours
        FROM fact_service_request f
        JOIN dim_agency a ON a.agency_key = f.agency_key
        WHERE NOT f.is_likely_internal
        GROUP BY a.agency_name
        ORDER BY total_requests DESC
        """,
        conn,
    )


def requests_by_day(conn) -> pd.DataFrame:
    """Daily volume trend — the basic time series every 311 dashboard needs."""
    return pd.read_sql(
        """
        SELECT
            d.full_date,
            COUNT(*) AS request_count
        FROM fact_service_request f
        JOIN dim_date d ON d.date_key = f.created_date_key
        WHERE NOT f.is_likely_internal
        GROUP BY d.full_date
        ORDER BY d.full_date
        """,
        conn,
    )


def resolution_time_distribution(conn) -> pd.DataFrame:
    """Raw resolution hours for closed requests, for a histogram. Median is a
    better headline stat than mean for this — resolution time is heavily
    right-skewed (most requests close fast, a long tail takes weeks), and a mean
    gets dragged around by that tail in a way a viewer reading a single number
    would misread as typical."""
    return pd.read_sql(
        "SELECT resolution_hours FROM fact_service_request "
        "WHERE is_closed AND resolution_hours IS NOT NULL AND NOT is_likely_internal",
        conn,
    )


def requests_by_neighborhood(conn, limit: int = 15) -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT
            n.neighborhood_name,
            COUNT(*) AS total_requests,
            ROUND(AVG(f.resolution_hours)::numeric, 1) AS avg_resolution_hours
        FROM fact_service_request f
        JOIN dim_neighborhood n ON n.neighborhood_key = f.neighborhood_key
        WHERE NOT f.is_likely_internal
        GROUP BY n.neighborhood_name
        ORDER BY total_requests DESC
        LIMIT %(limit)s
        """,
        conn,
        params={"limit": limit},
    )


def map_points(conn, limit: int = 5000) -> pd.DataFrame:
    """Points for the map view. Only rows with coordinates the transform stage
    already validated (AD: has_valid_coordinates) — plotting a null-island or
    geocoding-error point would put a marker in the Gulf of Guinea on a map
    titled "Denver 311 requests," which is a worse failure than showing fewer
    points."""
    return pd.read_sql(
        """
        SELECT latitude, longitude, case_status, incident_address
        FROM fact_service_request
        WHERE has_valid_coordinates AND NOT is_likely_internal
        LIMIT %(limit)s
        """,
        conn,
        params={"limit": limit},
    )
