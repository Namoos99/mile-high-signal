"""List what has landed in the raw bucket, using the same config as the pipeline.

Exists so `make ls-raw` does not require a separate AWS CLI install — it reuses the
boto3 client the pipeline already builds, which also means it automatically follows
whatever `.env` points at (LocalStack or real AWS).

    python -m scripts.ls_raw
    python scripts/ls_raw.py --prefix raw/service_requests/_manifests
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from denver311.common.config import get_settings  # noqa: E402
from denver311.ingestion.landing import make_s3_client  # noqa: E402


def human(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}TB"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", default=None, help="Key prefix to list.")
    parser.add_argument("--cat", metavar="KEY", help="Print a JSON object from the bucket.")
    args = parser.parse_args()

    settings = get_settings()
    s3 = make_s3_client(settings)

    if args.cat:
        body = s3.get_object(Bucket=settings.s3_bucket, Key=args.cat)["Body"].read()
        print(json.dumps(json.loads(body), indent=2))
        return 0

    prefix = args.prefix if args.prefix is not None else settings.s3_raw_prefix
    paginator = s3.get_paginator("list_objects_v2")

    count = 0
    total = 0
    print(f"s3://{settings.s3_bucket}/{prefix}")
    for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            print(f"  {human(obj['Size']):>9}  {obj['LastModified']:%Y-%m-%d %H:%M}  {obj['Key']}")
            count += 1
            total += obj["Size"]

    if count == 0:
        print("  (empty — has an ingestion run completed?)")
    else:
        print(f"\n  {count} objects, {human(total)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
