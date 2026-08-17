"""Denver 311 dashboard — Streamlit UI.

    streamlit run dashboard/app.py

Deliberately thin: every query lives in dashboard/data.py so the actual SQL is
unit-testable without a browser (see tests/test_dashboard_data.py). This file's
only job is layout and chart rendering.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dashboard.data import (  # noqa: E402
    map_points,
    requests_by_agency,
    requests_by_day,
    requests_by_neighborhood,
    resolution_time_distribution,
    total_requests,
)
from denver311.common.config import get_settings  # noqa: E402

st.set_page_config(page_title="Denver 311 Dashboard", layout="wide")


@st.cache_resource
def get_connection():
    settings = get_settings()
    return psycopg2.connect(
        host=settings.warehouse_host,
        port=settings.warehouse_port,
        dbname=settings.warehouse_db,
        user=settings.warehouse_user,
        password=settings.warehouse_password,
    )


st.title("Denver 311 Service Requests")
st.caption(
    "Live view of the warehouse loaded by the pipeline. Internal-notification rows "
    "(see docs/DECISIONS.md AD-012) are excluded from every metric below."
)

try:
    conn = get_connection()
except Exception as e:  # noqa: BLE001 - a connection failure here is a user-facing message, not a crash
    st.error(
        f"Could not connect to the warehouse database: {e}\n\n"
        "Run `make up && make ingest && make transform && make load-warehouse` first, "
        "or check that Postgres is running (`docker compose -f docker/docker-compose.yml ps`)."
    )
    st.stop()

col1, col2, col3 = st.columns(3)
total = total_requests(conn)
by_agency = requests_by_agency(conn)
resolution = resolution_time_distribution(conn)

col1.metric("Total requests (excl. internal)", f"{total:,}")
col2.metric("Agencies represented", len(by_agency))
col3.metric(
    "Median resolution time",
    f"{resolution['resolution_hours'].median():.1f} hrs" if len(resolution) else "N/A",
)

st.divider()

left, right = st.columns([2, 1])

with left:
    st.subheader("Requests by agency")
    st.bar_chart(by_agency.set_index("agency_name")["total_requests"])

with right:
    st.subheader("Avg. resolution hours by agency")
    st.dataframe(
        by_agency[["agency_name", "avg_resolution_hours", "closed_requests"]],
        hide_index=True,
        use_container_width=True,
    )

st.divider()

trend_col, hist_col = st.columns(2)

with trend_col:
    st.subheader("Daily request volume")
    daily = requests_by_day(conn)
    if len(daily):
        st.line_chart(daily.set_index("full_date")["request_count"])
    else:
        st.info("No data loaded yet.")

with hist_col:
    st.subheader("Resolution time distribution (closed requests)")
    if len(resolution):
        # Median, not mean, called out in the caption — resolution time is
        # heavily right-skewed (see dashboard/data.py docstring), so a single
        # mean number would mislead a viewer about the typical case.
        st.bar_chart(pd.cut(resolution["resolution_hours"], bins=20).value_counts().sort_index())
    else:
        st.info("No closed requests yet.")

st.divider()

st.subheader("Top neighborhoods by request volume")
st.dataframe(requests_by_neighborhood(conn), hide_index=True, use_container_width=True)

st.subheader("Request locations")
points = map_points(conn)
if len(points):
    st.map(points.rename(columns={"latitude": "lat", "longitude": "lon"}))
else:
    st.info("No geocoded requests yet.")
