"""Resolve the live ArcGIS FeatureServer endpoint from a stable Hub item id.

WHY THIS EXISTS (architecture decision — see README):
Denver publishes 311 through ArcGIS Hub. The dataset page URL contains an item id
(`46a685dd1b284ff2a3bf68e062051635`) and a layer id (`66`). The *actual* query
endpoint lives on an Esri-managed host like
`https://services<N>.arcgis.com/<org-key>/arcgis/rest/services/<name>/FeatureServer`.

That host is not stable. Orgs get re-provisioned, services get republished. If you
hardcode it, your pipeline silently 404s months later. The item id is the durable
identifier, so we resolve host -> layer at runtime through the public ArcGIS
sharing API and cache it for the life of the process.

Cost: one extra HTTP call per run. Benefit: the pipeline survives a source
migration without a code change.
"""

from __future__ import annotations

import logging

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from denver311.common.config import Settings

logger = logging.getLogger(__name__)


class DiscoveryError(RuntimeError):
    """Raised when the source endpoint cannot be resolved."""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def _get_json(session: requests.Session, url: str, params: dict, timeout: int) -> dict:
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    # ArcGIS is cheerfully un-RESTful: it returns HTTP 200 with an {"error": ...}
    # body. Treat that as a failure explicitly or you will silently ingest nothing.
    if isinstance(payload, dict) and "error" in payload:
        raise DiscoveryError(f"ArcGIS returned an error for {url}: {payload['error']}")
    return payload


def resolve_layer_url(settings: Settings, session: requests.Session | None = None) -> str:
    """Return the fully-qualified `/FeatureServer/<layer>` URL for the source layer."""
    if settings.arcgis_service_url_override:
        logger.info("Using ARCGIS_SERVICE_URL_OVERRIDE; skipping discovery")
        return settings.arcgis_service_url_override.rstrip("/")

    session = session or requests.Session()
    item_url = f"{settings.arcgis_portal}/sharing/rest/content/items/{settings.arcgis_item_id}"

    payload = _get_json(
        session,
        item_url,
        params={"f": "json"},
        timeout=settings.request_timeout_seconds,
    )

    service_url = payload.get("url")
    if not service_url:
        raise DiscoveryError(
            f"Hub item {settings.arcgis_item_id} has no service URL. "
            "The dataset may have been unpublished. Check the Hub page."
        )

    layer_url = f"{service_url.rstrip('/')}/{settings.arcgis_layer_id}"
    logger.info("Resolved source layer: %s", layer_url)
    return layer_url


def describe_layer(layer_url: str, settings: Settings, session: requests.Session | None = None) -> dict:
    """Fetch layer metadata: field names/types, record cap, capabilities.

    Used by the schema-drift check in the data-quality stage, and useful on day one
    to confirm the real field names before you write any transform logic.
    """
    session = session or requests.Session()
    return _get_json(
        session,
        layer_url,
        params={"f": "json"},
        timeout=settings.request_timeout_seconds,
    )
