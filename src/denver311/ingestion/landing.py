"""Write raw extracts to object storage.

Landing-zone contract (architecture decision — see README):
  - Raw is IMMUTABLE and append-only. We never overwrite a landed file.
  - Format is newline-delimited JSON, gzipped. Not Parquet.
    Raw should be a faithful, schema-agnostic copy of what the API said. If the
    source adds a field, NDJSON absorbs it; a Parquet schema would reject it or
    silently drop it, and you lose the ability to replay history. Parquet starts
    at the Spark output, not here.
  - Keys are Hive-style partitioned by ingestion date so Spark can prune, and
    suffixed with a run id so two runs on the same day never collide.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
from datetime import UTC, datetime

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from denver311.common.config import Settings

logger = logging.getLogger(__name__)


def make_s3_client(settings: Settings):
    """One boto3 client factory. LocalStack vs AWS differs only by endpoint_url."""
    return boto3.client(
        "s3",
        endpoint_url=settings.aws_endpoint_url,  # None => real AWS
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )


def ensure_bucket(client, bucket: str, region: str) -> None:
    """Idempotent bucket creation — only used for LocalStack.

    On real AWS the bucket is Terraform's job, not the application's. This exists
    so `docker compose up` gives you a working environment with no manual steps.
    """
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in {"404", "NoSuchBucket", "403"}:
            raise

    kwargs = {"Bucket": bucket}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    client.create_bucket(**kwargs)
    logger.info("Created bucket %s", bucket)


def build_key(prefix: str, run_ts: datetime, part: int, run_id: str) -> str:
    return (
        f"{prefix}/"
        f"ingest_year={run_ts:%Y}/ingest_month={run_ts:%m}/ingest_day={run_ts:%d}/"
        f"{run_id}_part-{part:05d}.jsonl.gz"
    )


def write_page(
    client,
    settings: Settings,
    records: list[dict],
    run_ts: datetime,
    run_id: str,
    part: int,
) -> tuple[str, int]:
    """Serialise one page to gzipped NDJSON and put it. Returns (key, bytes_written)."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz:
        for record in records:
            gz.write(json.dumps(record, separators=(",", ":"), default=str).encode())
            gz.write(b"\n")

    body = buffer.getvalue()
    key = build_key(settings.s3_raw_prefix, run_ts, part, run_id)

    client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    logger.info("Wrote s3://%s/%s (%d records, %d bytes)", settings.s3_bucket, key, len(records), len(body))
    return key, len(body)


def write_manifest(client, settings: Settings, manifest: dict, run_id: str) -> str:
    """A per-run manifest makes the pipeline auditable and gives downstream Spark an
    explicit file list instead of relying on a prefix scan that might be eventually
    consistent or catch a half-finished run."""
    key = f"{settings.s3_raw_prefix}/_manifests/{run_id}.json"
    client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=json.dumps(manifest, indent=2, default=str).encode(),
        ContentType="application/json",
    )
    logger.info("Wrote manifest s3://%s/%s", settings.s3_bucket, key)
    return key


def read_watermark(client, settings: Settings) -> datetime | None:
    """Read the high-water mark from object storage.

    ARCHITECTURE DECISION: the watermark lives in S3, not in Airflow XCom or a
    local file. Reason — it must survive Airflow being rebuilt, and it must be
    readable by a backfill run executed outside Airflow entirely. One JSON object
    is the cheapest durable state we can get away with.
    """
    key = f"{settings.s3_raw_prefix}/_state/watermark.json"
    try:
        obj = client.get_object(Bucket=settings.s3_bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            logger.info("No watermark found — cold start")
            return None
        raise
    payload = json.loads(obj["Body"].read())
    return datetime.fromisoformat(payload["watermark"]).astimezone(UTC)


def write_watermark(client, settings: Settings, value: datetime, run_id: str) -> None:
    key = f"{settings.s3_raw_prefix}/_state/watermark.json"
    client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=json.dumps(
            {
                "watermark": value.astimezone(UTC).isoformat(),
                "updated_by_run": run_id,
                "updated_at": datetime.now(UTC).isoformat(),
            },
            indent=2,
        ).encode(),
        ContentType="application/json",
    )
    logger.info("Advanced watermark to %s", value.isoformat())
