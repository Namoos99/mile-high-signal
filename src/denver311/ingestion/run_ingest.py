"""Ingestion entrypoint.

python -m denver311.ingestion.run_ingest              # incremental
python -m denver311.ingestion.run_ingest --full       # ignore watermark
python -m denver311.ingestion.run_ingest --dry-run    # hit the API, write nothing
python -m denver311.ingestion.run_ingest --describe   # print source schema and exit
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import UTC, datetime, timedelta

import requests

from denver311.common.config import Settings, get_settings
from denver311.common.logging_setup import configure_logging
from denver311.ingestion.arcgis_client import (
    ArcGISFeatureClient,
    build_where_clause,
    epoch_ms_to_datetime,
)
from denver311.ingestion.discovery import describe_layer, resolve_layer_url
from denver311.ingestion.landing import (
    ensure_bucket,
    make_s3_client,
    read_watermark,
    write_manifest,
    write_page,
    write_watermark,
)

logger = logging.getLogger(__name__)


def _max_watermark(records: list[dict], field: str) -> datetime | None:
    values = [epoch_ms_to_datetime(r.get(field)) for r in records]
    values = [v for v in values if v is not None]
    return max(values) if values else None


def ingest(settings: Settings, *, full_refresh: bool = False, dry_run: bool = False) -> dict:
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    run_ts = datetime.now(UTC)
    logger.info("=== ingestion run %s (full_refresh=%s dry_run=%s) ===", run_id, full_refresh, dry_run)

    session = requests.Session()
    layer_url = resolve_layer_url(settings, session=session)
    client = ArcGISFeatureClient(layer_url, settings, session=session)

    s3 = make_s3_client(settings)
    if settings.should_create_bucket and not dry_run:
        ensure_bucket(s3, settings.s3_bucket, settings.aws_region)

    # --- decide the extraction window -------------------------------------
    if full_refresh:
        since = None
    else:
        since = read_watermark(s3, settings) if not dry_run else None
        if since is None:
            since = datetime.now(UTC) - timedelta(days=settings.initial_lookback_days)
            logger.info("Cold start: reaching back %d days", settings.initial_lookback_days)

    where = build_where_clause(settings.watermark_field, since)
    logger.info("WHERE %s", where)

    expected = client.count(where)
    logger.info("Source reports %d matching records", expected)

    # --- page, land, track -------------------------------------------------
    keys: list[str] = []
    total_records = 0
    total_bytes = 0
    high_water = since

    for part, page in enumerate(client.iter_pages(where)):
        total_records += len(page)
        page_max = _max_watermark(page, settings.watermark_field)
        if page_max and (high_water is None or page_max > high_water):
            high_water = page_max

        if dry_run:
            continue

        key, size = write_page(s3, settings, page, run_ts, run_id, part)
        keys.append(key)
        total_bytes += size

    manifest = {
        "run_id": run_id,
        "run_started_at": run_ts.isoformat(),
        "run_finished_at": datetime.now(UTC).isoformat(),
        "source_layer_url": layer_url,
        "where_clause": where,
        "expected_record_count": expected,
        "actual_record_count": total_records,
        "file_count": len(keys),
        "bytes_written": total_bytes,
        "files": keys,
        "watermark_before": since.isoformat() if since else None,
        "watermark_after": high_water.isoformat() if high_water else None,
        "full_refresh": full_refresh,
        "dry_run": dry_run,
    }

    # Count reconciliation. New records can legitimately arrive between the count
    # and the paging, so a small positive drift is fine; anything else is loud.
    if total_records < expected:
        logger.error("SHORT READ: expected %d, landed %d", expected, total_records)
        manifest["status"] = "short_read"
    else:
        manifest["status"] = "ok"

    if not dry_run:
        write_manifest(s3, settings, manifest, run_id)
        # Only advance the watermark after every file is durably written. If we
        # crash mid-run, the next run re-reads the same window — at-least-once
        # delivery, with dedup handled downstream on the natural key.
        if high_water and manifest["status"] == "ok":
            write_watermark(s3, settings, high_water, run_id)

    logger.info("=== done: %d records, %d files ===", total_records, len(keys))
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest Denver 311 service requests.")
    parser.add_argument("--full", action="store_true", help="Ignore watermark, pull everything available.")
    parser.add_argument("--dry-run", action="store_true", help="Query the API but write nothing.")
    parser.add_argument("--describe", action="store_true", help="Print the source layer schema and exit.")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)

    if args.describe:
        layer_url = resolve_layer_url(settings)
        meta = describe_layer(layer_url, settings)
        fields = [{"name": f["name"], "type": f["type"]} for f in meta.get("fields", [])]
        print(
            json.dumps(
                {
                    "layer_url": layer_url,
                    "name": meta.get("name"),
                    "maxRecordCount": meta.get("maxRecordCount"),
                    "field_count": len(fields),
                    "fields": fields,
                },
                indent=2,
            )
        )
        return 0

    manifest = ingest(settings, full_refresh=args.full, dry_run=args.dry_run)
    return 0 if manifest["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
