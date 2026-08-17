"""Run the cleaning transform end to end.

    python -m spark_jobs.run_transform

Reads every raw file under the configured S3 prefix, cleans it with
spark_jobs/transform.py, and writes two things:
  1. Parquet to the "processed/" zone in S3 — this is what the warehouse load
     (next component) will read from.
  2. A single readable CSV to ./output/service_requests_preview.csv — not part of
     the production pipeline, but genuinely useful for a human to open and look at
     instead of decompressing gzipped NDJSON by hand.

ARCHITECTURE NOTE: this reads/writes S3 via boto3 to a local temp directory, then
points Spark at local files, rather than having Spark read/write s3a:// URLs
directly. Real production Spark almost always talks to S3 directly through the
hadoop-aws connector. We don't do that here because it requires downloading extra
JARs at runtime, which is a bad experience on a laptop with no reliable internet
during a job run. The transform logic in transform.py is identical either way —
only the I/O plumbing in this file would change to point at s3a:// paths instead
of local ones. Worth saying exactly this in an interview if asked.
"""

from __future__ import annotations

import argparse
import gzip
import logging
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from denver311.common.config import get_settings  # noqa: E402
from denver311.common.logging_setup import configure_logging  # noqa: E402
from denver311.ingestion.landing import make_s3_client  # noqa: E402

logger = logging.getLogger(__name__)


def download_raw_files(s3, bucket: str, prefix: str, dest_dir: Path) -> list[Path]:
    """Pull every raw part file to local disk, decompressing as we go — Spark can
    read .gz directly, but decompressing here lets us also count/inspect rows
    cheaply if a run fails partway."""
    paginator = s3.get_paginator("list_objects_v2")
    written: list[Path] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".jsonl.gz"):
                continue  # skip _manifests/ and _state/
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            out_path = dest_dir / (Path(key).stem)  # strip .gz, keep .jsonl
            out_path.write_bytes(gzip.decompress(body))
            written.append(out_path)

    logger.info("Downloaded %d raw files to %s", len(written), dest_dir)
    return written


def upload_parquet(s3, bucket: str, local_dir: Path, prefix: str) -> int:
    count = 0
    for path in local_dir.rglob("*.parquet"):
        key = f"{prefix}/{path.relative_to(local_dir)}"
        s3.upload_file(str(path), bucket, key)
        count += 1
    logger.info("Uploaded %d parquet files to s3://%s/%s", count, bucket, prefix)
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Clean and enrich raw 311 service requests.")
    parser.add_argument(
        "--preview-rows", type=int, default=200, help="Rows to include in the human-readable CSV preview."
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)

    # Imported here, not at module top, so `--help` and simple argument errors
    # don't pay Spark's session-startup cost.
    from pyspark.sql import functions as F

    from spark_jobs.quality_checks import run_quality_checks
    from spark_jobs.transform import RAW_SCHEMA, clean_service_requests, get_spark

    s3 = make_s3_client(settings)

    with tempfile.TemporaryDirectory(prefix="denver311_raw_") as raw_tmp:
        raw_dir = Path(raw_tmp)
        files = download_raw_files(s3, settings.s3_bucket, settings.s3_raw_prefix, raw_dir)
        if not files:
            logger.error(
                "No raw files found under s3://%s/%s — run ingestion first.",
                settings.s3_bucket,
                settings.s3_raw_prefix,
            )
            return 1

        spark = get_spark()
        try:
            raw_df = spark.read.schema(RAW_SCHEMA).json(str(raw_dir))
            raw_count = raw_df.count()

            clean_df = clean_service_requests(raw_df)
            clean_count = clean_df.count()

            # Gate: raises DataQualityError on a hard failure, which propagates
            # up and exits non-zero — nothing below this line runs, so a bad
            # batch never reaches Parquet, S3, or the warehouse. See AD-015.
            quality_report = run_quality_checks(clean_df)
            if quality_report.warnings:
                for w in quality_report.warnings:
                    logger.warning("Data quality warning: %s", w)

            logger.info(
                "Raw rows: %d | Clean rows (post-dedup): %d | Duplicates removed: %d",
                raw_count,
                clean_count,
                raw_count - clean_count,
            )

            # --- write Parquet: persistent local path + push to S3's processed/ zone
            # A persistent local path (not a temp dir) is deliberate: the warehouse
            # loader (warehouse/load_warehouse.py) reads from here directly for
            # local/dev runs, without needing a round-trip through S3 and back.
            out_dir = Path("output")
            out_dir.mkdir(exist_ok=True)
            parquet_dir = out_dir / "processed_parquet"
            clean_df.write.mode("overwrite").parquet(str(parquet_dir))
            upload_parquet(s3, settings.s3_bucket, parquet_dir, "processed/service_requests")

            # --- human-readable preview -------------------------------------
            preview_csv = out_dir / "service_requests_preview.csv"
            preview_pdf = (
                clean_df.select(
                    "OBJECTID",
                    "created_at",
                    "closed_at",
                    "is_closed",
                    "resolution_hours",
                    "Case_Status",
                    "Case_Source",
                    "Agency",
                    "Type",
                    "Topic",
                    "Neighborhood",
                    "Incident_Address_1",
                    "is_likely_internal",
                    "has_valid_coordinates",
                )
                .orderBy(F.desc("created_at"))
                .limit(args.preview_rows)
                .toPandas()
            )
            preview_pdf.to_csv(preview_csv, index=False)
            logger.info("Wrote human-readable preview: %s (%d rows)", preview_csv, len(preview_pdf))

            summary = {
                "raw_rows": raw_count,
                "clean_rows": clean_count,
                "duplicates_removed": raw_count - clean_count,
                "internal_flagged": clean_df.where("is_likely_internal").count(),
                "invalid_coordinates": clean_df.where(
                    "NOT has_valid_coordinates AND Latitude IS NOT NULL"
                ).count(),
                "quality_gate": "passed" if quality_report.passed else "FAILED",
                "quality_warnings": len(quality_report.warnings),
            }
            logger.info("=== transform summary: %s ===", summary)
        finally:
            spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
