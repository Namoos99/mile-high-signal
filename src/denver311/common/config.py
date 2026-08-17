"""Central configuration.

Every environment-dependent value is resolved here and nowhere else. This is what
makes "point it at real AWS with a config change" true rather than aspirational:
no module below this one is allowed to read os.environ directly.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- Source: Denver Open Data (ArcGIS Hub) --------------------------------
    # We store the Hub *item id*, not a hardcoded FeatureServer hostname.
    # Denver has migrated hosting orgs before; the item id is the stable handle.
    # See common/discovery.py for how this is resolved to a live endpoint.
    arcgis_item_id: str = "46a685dd1b284ff2a3bf68e062051635"
    arcgis_layer_id: int = 66
    arcgis_portal: str = "https://www.arcgis.com"

    # Escape hatch: if discovery ever breaks, set this and it wins.
    arcgis_service_url_override: str | None = None

    # ArcGIS caps a single query response (usually 1000-2000 features).
    # We page with resultOffset; this is the page size we ask for.
    page_size: int = 1000
    max_pages: int = 500  # circuit breaker against an infinite paging loop
    request_timeout_seconds: int = 60

    # Field on the source layer used for incremental extraction.
    watermark_field: str = "Case_Created_Date"
    # There is no true business key on this layer (no case/ticket number field —
    # confirmed against the live schema). OBJECTID is the only unique identifier,
    # but it's ArcGIS-assigned and not guaranteed stable across a full republish
    # of the service. See docs/DECISIONS.md AD-011 for the dedup implication.
    natural_key_field: str = "OBJECTID"

    # ---- Sink: object storage -------------------------------------------------
    s3_bucket: str = "denver311-raw"
    s3_raw_prefix: str = "raw/service_requests"
    aws_region: str = "us-west-2"

    # When set, boto3 talks to LocalStack instead of AWS. Unset it for real AWS.
    aws_endpoint_url: str | None = "http://localhost:4566"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"

    # ---- Sink: Postgres warehouse ---------------------------------------------
    warehouse_host: str = "localhost"
    warehouse_port: int = 5432
    warehouse_db: str = "denver311"
    warehouse_user: str = "postgres"
    warehouse_password: str = "postgres"

    # ---- Run behaviour --------------------------------------------------------
    # How far back to reach on a cold start (no watermark stored yet).
    initial_lookback_days: int = 30
    log_level: str = "INFO"
    environment: str = Field(default="local")

    # Whether the app may create its own bucket. True locally; False on real AWS,
    # where the bucket is Terraform's responsibility and the app should fail loudly
    # if it is missing rather than quietly conjuring an unmanaged one.
    auto_create_bucket: bool = False

    @property
    def is_localstack(self) -> bool:
        return bool(self.aws_endpoint_url)

    @property
    def should_create_bucket(self) -> bool:
        return self.auto_create_bucket or self.is_localstack


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
