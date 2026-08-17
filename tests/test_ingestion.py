"""Ingestion unit tests.

No network. ArcGIS is stubbed with `responses`; S3 is stubbed with `moto`.
That combination is deliberate: it means CI can run the full ingestion path on
every PR without credentials and without depending on Denver's uptime.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta

import boto3
import pytest
import responses
from moto import mock_aws

from denver311.common.config import Settings
from denver311.ingestion.arcgis_client import (
    ArcGISFeatureClient,
    ArcGISQueryError,
    build_where_clause,
    epoch_ms_to_datetime,
)
from denver311.ingestion.discovery import DiscoveryError, resolve_layer_url
from denver311.ingestion.landing import (
    build_key,
    ensure_bucket,
    read_watermark,
    write_page,
    write_watermark,
)

PORTAL = "https://portal.test"
SERVICE = "https://services9.arcgis.com/abc123/arcgis/rest/services/ODC_311/FeatureServer"
LAYER = f"{SERVICE}/66"


@pytest.fixture
def settings() -> Settings:
    return Settings(
        arcgis_portal=PORTAL,
        arcgis_item_id="item123",
        arcgis_layer_id=66,
        arcgis_service_url_override=None,
        page_size=2,
        max_pages=10,
        # Explicit here so these tests don't silently drift if the real-world
        # config defaults change (they did — see AD-011 in docs/DECISIONS.md).
        watermark_field="Requested_Datetime",
        natural_key_field="Case_Number",
        s3_bucket="test-bucket",
        aws_region="us-east-1",
        aws_endpoint_url=None,  # moto intercepts real endpoints
        initial_lookback_days=30,
        auto_create_bucket=True,
    )


def _feature(oid: int, case: str, requested_ms: int) -> dict:
    return {
        "attributes": {
            "OBJECTID": oid,
            "Case_Number": case,
            "Requested_Datetime": requested_ms,
            "Case_Summary": "Pothole",
            "Agency_Name": "Public Works",
        }
    }


# --------------------------------------------------------------------------
# discovery
# --------------------------------------------------------------------------


@responses.activate
def test_resolve_layer_url_from_item_id(settings):
    responses.add(
        responses.GET,
        f"{PORTAL}/sharing/rest/content/items/item123",
        json={"id": "item123", "url": SERVICE},
        status=200,
    )
    assert resolve_layer_url(settings) == LAYER


@responses.activate
def test_resolve_raises_when_item_has_no_url(settings):
    responses.add(
        responses.GET,
        f"{PORTAL}/sharing/rest/content/items/item123",
        json={"id": "item123"},
        status=200,
    )
    with pytest.raises(DiscoveryError, match="no service URL"):
        resolve_layer_url(settings)


def test_override_short_circuits_discovery(settings):
    settings.arcgis_service_url_override = f"{LAYER}/"
    # No responses registered — if it tried the network this would fail.
    assert resolve_layer_url(settings) == LAYER


# --------------------------------------------------------------------------
# paging
# --------------------------------------------------------------------------


@responses.activate
def test_iter_pages_walks_offsets_until_exhausted(settings):
    responses.add(
        responses.GET,
        f"{LAYER}/query",
        json={"features": [_feature(1, "A", 0), _feature(2, "B", 0)], "exceededTransferLimit": True},
    )
    responses.add(
        responses.GET,
        f"{LAYER}/query",
        json={"features": [_feature(3, "C", 0)], "exceededTransferLimit": False},
    )

    client = ArcGISFeatureClient(LAYER, settings)
    pages = list(client.iter_pages())

    assert [len(p) for p in pages] == [2, 1]
    assert [r["Case_Number"] for r in pages[0]] == ["A", "B"]
    # second request must have advanced the offset
    assert "resultOffset=2" in responses.calls[1].request.url


@responses.activate
def test_empty_first_page_terminates_cleanly(settings):
    responses.add(responses.GET, f"{LAYER}/query", json={"features": []})
    client = ArcGISFeatureClient(LAYER, settings)
    assert list(client.iter_pages()) == []


@responses.activate
def test_arcgis_error_body_with_http_200_is_raised(settings):
    """ArcGIS returns 200 OK with an error body. Silently ingesting nothing is the
    failure mode this test exists to prevent."""
    responses.add(
        responses.GET,
        f"{LAYER}/query",
        json={"error": {"code": 400, "message": "Invalid field: Bogus_Field"}},
        status=200,
    )
    client = ArcGISFeatureClient(LAYER, settings)
    with pytest.raises(ArcGISQueryError, match="Invalid field"):
        list(client.iter_pages())


@responses.activate
def test_max_pages_circuit_breaker(settings):
    settings.max_pages = 3
    responses.add(
        responses.GET,
        f"{LAYER}/query",
        json={"features": [_feature(1, "A", 0), _feature(2, "B", 0)], "exceededTransferLimit": True},
    )
    client = ArcGISFeatureClient(LAYER, settings)
    assert len(list(client.iter_pages())) == 3


@responses.activate
def test_count_only_query(settings):
    responses.add(responses.GET, f"{LAYER}/query", json={"count": 4211})
    assert ArcGISFeatureClient(LAYER, settings).count() == 4211


# --------------------------------------------------------------------------
# time handling
# --------------------------------------------------------------------------


def test_epoch_ms_conversion():
    # 2024-01-15T10:30:00Z
    assert epoch_ms_to_datetime(1705314600000) == datetime(2024, 1, 15, 10, 30, tzinfo=UTC)


def test_epoch_ms_handles_null():
    assert epoch_ms_to_datetime(None) is None


def test_seconds_would_be_wrong():
    """Guard against the classic seconds/millis mixup: if someone 'fixes' the divisor,
    this test fails loudly instead of producing 1970 dates."""
    assert epoch_ms_to_datetime(1705314600000).year == 2024


def test_where_clause_full_refresh():
    assert build_where_clause("Requested_Datetime", None) == "1=1"


def test_where_clause_incremental_is_utc():
    since = datetime(2024, 3, 1, 12, 0, tzinfo=UTC)
    assert build_where_clause("Requested_Datetime", since) == (
        "Requested_Datetime > TIMESTAMP '2024-03-01 12:00:00'"
    )


# --------------------------------------------------------------------------
# landing zone
# --------------------------------------------------------------------------


def test_key_layout_is_hive_partitioned():
    key = build_key("raw/sr", datetime(2024, 7, 4, tzinfo=UTC), 3, "run-abc")
    assert key == "raw/sr/ingest_year=2024/ingest_month=07/ingest_day=04/run-abc_part-00003.jsonl.gz"


@mock_aws
def test_write_page_roundtrip(settings):
    s3 = boto3.client("s3", region_name="us-east-1")
    ensure_bucket(s3, settings.s3_bucket, settings.aws_region)

    records = [{"Case_Number": "A", "Requested_Datetime": 1705314600000}]
    key, size = write_page(s3, settings, records, datetime(2024, 1, 15, tzinfo=UTC), "run-1", 0)

    body = s3.get_object(Bucket=settings.s3_bucket, Key=key)["Body"].read()
    lines = gzip.decompress(body).decode().strip().split("\n")
    assert json.loads(lines[0])["Case_Number"] == "A"
    assert size > 0


@mock_aws
def test_watermark_cold_start_returns_none(settings):
    s3 = boto3.client("s3", region_name="us-east-1")
    ensure_bucket(s3, settings.s3_bucket, settings.aws_region)
    assert read_watermark(s3, settings) is None


@mock_aws
def test_watermark_roundtrip(settings):
    s3 = boto3.client("s3", region_name="us-east-1")
    ensure_bucket(s3, settings.s3_bucket, settings.aws_region)

    mark = datetime.now(UTC).replace(microsecond=0)
    write_watermark(s3, settings, mark, "run-1")
    assert read_watermark(s3, settings) == mark


@mock_aws
def test_ensure_bucket_is_idempotent(settings):
    s3 = boto3.client("s3", region_name="us-east-1")
    ensure_bucket(s3, settings.s3_bucket, settings.aws_region)
    ensure_bucket(s3, settings.s3_bucket, settings.aws_region)  # must not raise


# --------------------------------------------------------------------------
# end-to-end (stubbed source + stubbed S3)
# --------------------------------------------------------------------------


@mock_aws
@responses.activate
def test_full_ingest_run_lands_files_and_advances_watermark(settings):
    from denver311.ingestion.run_ingest import ingest

    recent_ms = int((datetime.now(UTC) - timedelta(days=1)).timestamp() * 1000)

    responses.add(
        responses.GET,
        f"{PORTAL}/sharing/rest/content/items/item123",
        json={"url": SERVICE},
    )
    responses.add(responses.GET, f"{LAYER}/query", json={"count": 3})
    responses.add(
        responses.GET,
        f"{LAYER}/query",
        json={
            "features": [_feature(1, "A", recent_ms), _feature(2, "B", recent_ms)],
            "exceededTransferLimit": True,
        },
    )
    responses.add(
        responses.GET,
        f"{LAYER}/query",
        json={"features": [_feature(3, "C", recent_ms)], "exceededTransferLimit": False},
    )

    manifest = ingest(settings)

    assert manifest["status"] == "ok"
    assert manifest["actual_record_count"] == 3
    assert manifest["file_count"] == 2
    assert manifest["watermark_after"] is not None

    s3 = boto3.client("s3", region_name="us-east-1")
    landed = s3.list_objects_v2(Bucket=settings.s3_bucket, Prefix="raw/service_requests/ingest_")
    assert landed["KeyCount"] == 2


@mock_aws
@responses.activate
def test_short_read_is_flagged_and_watermark_not_advanced(settings):
    """If the source says 100 rows and we land 1, we must not advance the watermark —
    otherwise the missing 99 rows are lost forever."""
    from denver311.ingestion.run_ingest import ingest

    recent_ms = int(datetime.now(UTC).timestamp() * 1000)
    responses.add(responses.GET, f"{PORTAL}/sharing/rest/content/items/item123", json={"url": SERVICE})
    responses.add(responses.GET, f"{LAYER}/query", json={"count": 100})
    responses.add(
        responses.GET,
        f"{LAYER}/query",
        json={"features": [_feature(1, "A", recent_ms)], "exceededTransferLimit": False},
    )

    manifest = ingest(settings)
    assert manifest["status"] == "short_read"

    s3 = boto3.client("s3", region_name="us-east-1")
    assert read_watermark(s3, settings) is None


@mock_aws
@responses.activate
def test_dry_run_writes_nothing(settings):
    from denver311.ingestion.run_ingest import ingest

    responses.add(responses.GET, f"{PORTAL}/sharing/rest/content/items/item123", json={"url": SERVICE})
    responses.add(responses.GET, f"{LAYER}/query", json={"count": 1})
    responses.add(
        responses.GET,
        f"{LAYER}/query",
        json={"features": [_feature(1, "A", 1705314600000)], "exceededTransferLimit": False},
    )

    manifest = ingest(settings, dry_run=True)
    assert manifest["dry_run"] is True
    assert manifest["file_count"] == 0
