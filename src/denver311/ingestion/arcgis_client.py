"""Paged reader for an ArcGIS FeatureServer layer.

Handles the three things that break naive ingestion scripts:
  1. Server-side record caps (you ask for 5000, you get 1000, silently).
  2. Transient 5xx / connection resets on long paging runs.
  3. Epoch-millisecond timestamps that look like plausible integers.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from denver311.common.config import Settings

logger = logging.getLogger(__name__)

RETRYABLE = (requests.HTTPError, requests.ConnectionError, requests.Timeout)


class ArcGISQueryError(RuntimeError):
    pass


def build_where_clause(watermark_field: str, since: datetime | None) -> str:
    """Build an incremental predicate.

    ArcGIS wants timestamps as `TIMESTAMP 'YYYY-MM-DD HH:MM:SS'` in a WHERE clause.
    `1=1` is the standard "everything" predicate — it is not a placeholder to be
    replaced later, it is the actual idiom.
    """
    if since is None:
        return "1=1"
    stamp = since.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    return f"{watermark_field} > TIMESTAMP '{stamp}'"


class ArcGISFeatureClient:
    def __init__(
        self,
        layer_url: str,
        settings: Settings,
        session: requests.Session | None = None,
    ) -> None:
        self.layer_url = layer_url.rstrip("/")
        self.settings = settings
        self.session = session or requests.Session()

    @retry(
        retry=retry_if_exception_type(RETRYABLE),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _query(self, params: dict) -> dict:
        response = self.session.get(
            f"{self.layer_url}/query",
            params=params,
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            # Not retryable — a bad WHERE clause will fail identically forever.
            raise ArcGISQueryError(f"Query rejected by ArcGIS: {payload['error']}")
        return payload

    def count(self, where: str = "1=1") -> int:
        """Cheap pre-flight count. Lets us log expected vs actual and detect truncation."""
        payload = self._query({"where": where, "returnCountOnly": "true", "f": "json"})
        return int(payload.get("count", 0))

    def iter_pages(self, where: str = "1=1") -> Iterator[list[dict]]:
        """Yield pages of attribute dicts, ordered by OBJECTID.

        ORDERING IS LOAD-BEARING. Offset paging without a stable sort key can skip
        or duplicate rows when the server reshuffles between requests. OBJECTID is
        monotonic and immutable on ArcGIS hosted layers, so we sort on it.
        """
        offset = 0
        pages = 0

        while pages < self.settings.max_pages:
            payload = self._query(
                {
                    "where": where,
                    "outFields": "*",
                    "returnGeometry": "false",
                    "orderByFields": "OBJECTID ASC",
                    "resultOffset": offset,
                    "resultRecordCount": self.settings.page_size,
                    "f": "json",
                }
            )

            features = payload.get("features", [])
            if not features:
                return

            records = [f.get("attributes", {}) for f in features]
            logger.info("Fetched page %d (%d records, offset %d)", pages + 1, len(records), offset)
            yield records

            # `exceededTransferLimit` is ArcGIS's "there is more" flag. Absence of it
            # combined with a short page means we are done.
            more = payload.get("exceededTransferLimit", False)
            if not more and len(records) < self.settings.page_size:
                return

            offset += len(records)
            pages += 1

        logger.warning(
            "Hit max_pages=%d circuit breaker at offset %d. Data may be incomplete.",
            self.settings.max_pages,
            offset,
        )

    def fetch_all(self, where: str = "1=1") -> list[dict]:
        return [record for page in self.iter_pages(where) for record in page]


def epoch_ms_to_datetime(value: int | float | None) -> datetime | None:
    """ArcGIS returns dates as epoch milliseconds, UTC. Not seconds. Not ISO strings.

    Getting this wrong yields dates in 1970 that look almost plausible in a chart,
    which is why it lives in one tested function instead of being inlined.
    """
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=UTC)
