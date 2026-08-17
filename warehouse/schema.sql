-- Denver 311 warehouse: star schema.
--
-- Design choices worth explaining in an interview:
--   1. Dimensions are populated by the loader as it encounters new values
--      (a classic "late-arriving dimension" pattern), not pre-seeded. Agency
--      and Type values on a public feed change over time; hardcoding them
--      would require a migration every time Denver adds a department.
--   2. Surrogate integer keys (SERIAL) on every dimension, not the natural
--      string value. Joins on int are cheap; if Denver renames "Public Works"
--      to "Department of Transportation & Infrastructure" mid-stream, the
--      surrogate key means historical fact rows aren't silently orphaned.
--   3. fact_service_request keeps OBJECTID as a unique business key (see
--      docs/DECISIONS.md AD-011 for why OBJECTID and not a true case number)
--      so the loader can upsert idempotently — rerunning a load for the same
--      day never creates duplicate fact rows.

CREATE TABLE IF NOT EXISTS dim_agency (
    agency_key      SERIAL PRIMARY KEY,
    agency_name     TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_service_type (
    service_type_key SERIAL PRIMARY KEY,
    type_name         TEXT,
    topic_name        TEXT,
    UNIQUE (type_name, topic_name)
);

CREATE TABLE IF NOT EXISTS dim_neighborhood (
    neighborhood_key SERIAL PRIMARY KEY,
    neighborhood_name TEXT NOT NULL UNIQUE,
    council_district   INTEGER,
    police_district    INTEGER
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER PRIMARY KEY,       -- YYYYMMDD, int join key is cheap and index-friendly
    full_date   DATE NOT NULL UNIQUE,
    year        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    day         INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,           -- 0=Sunday
    is_weekend  BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_service_request (
    request_key         BIGSERIAL PRIMARY KEY,
    object_id            BIGINT NOT NULL UNIQUE,  -- source natural key, see AD-011
    agency_key            INTEGER REFERENCES dim_agency(agency_key),
    service_type_key      INTEGER REFERENCES dim_service_type(service_type_key),
    neighborhood_key       INTEGER REFERENCES dim_neighborhood(neighborhood_key),
    created_date_key        INTEGER REFERENCES dim_date(date_key),
    created_at               TIMESTAMPTZ NOT NULL,
    closed_at                 TIMESTAMPTZ,
    case_status                 TEXT,
    case_source                  TEXT,
    is_closed                     BOOLEAN NOT NULL,
    resolution_hours               NUMERIC(10, 2),
    is_likely_internal              BOOLEAN NOT NULL DEFAULT FALSE,
    has_valid_coordinates            BOOLEAN NOT NULL DEFAULT FALSE,
    latitude                          DOUBLE PRECISION,
    longitude                         DOUBLE PRECISION,
    incident_address                   TEXT,
    loaded_at                           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The indexes a dashboard actually needs: filter by date range, group by
-- agency, and exclude internal-notification noise. Built to match the
-- queries in dashboard/data.py, not speculatively.
CREATE INDEX IF NOT EXISTS idx_fact_created_at ON fact_service_request (created_at);
CREATE INDEX IF NOT EXISTS idx_fact_agency ON fact_service_request (agency_key);
CREATE INDEX IF NOT EXISTS idx_fact_is_internal ON fact_service_request (is_likely_internal);
