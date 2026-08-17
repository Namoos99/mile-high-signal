# Architecture decisions

Each entry: what was decided, what it was decided *against*, and what it costs. These are the
things an interviewer will probe — the format is deliberately "here's the trade-off I accepted,"
not "here's the best practice."

---

## AD-001 — Dataset: 311 service requests over RTD GTFS

**Decided:** Denver 311 service requests (ArcGIS Hub item `46a685dd1b284ff2a3bf68e062051635`, layer 66).

**Against:** RTD transit/GTFS.

**Why:** 311 exposes a key-free REST endpoint with a reliable creation timestamp, which is what
makes incremental extraction possible at all. RTD's realtime feed is protobuf over a credentialed
endpoint; static GTFS is a zip you re-download wholesale. GTFS would demonstrate protobuf parsing
but adds an auth dependency that breaks for anyone cloning the repo.

**Cost:** 311 is a rolling 12-month window with no historical archive, so the "retained history"
value of the warehouse only accrues going forward. Mention this before an interviewer notices it.

---

## AD-002 — Endpoint discovery instead of a hardcoded URL

**Decided:** Store the ArcGIS Hub *item id* and resolve the FeatureServer host at runtime via
`/sharing/rest/content/items/{id}?f=json`.

**Against:** Hardcoding `https://services<N>.arcgis.com/<org>/arcgis/rest/services/.../FeatureServer/66`.

**Why:** The host is Esri-managed and changes when a service is republished or an org is
re-provisioned. The item id is the durable handle. A hardcoded host produces a pipeline that works
for months and then 404s with no code change to blame.

**Cost:** One extra HTTP call per run, and a dependency on the sharing API being up. Mitigated by
`ARCGIS_SERVICE_URL_OVERRIDE`, which short-circuits discovery entirely.

---

## AD-003 — Raw layer is gzipped NDJSON, not Parquet

**Decided:** Land raw extracts as newline-delimited JSON, gzipped, immutable, append-only.

**Against:** Writing Parquet at ingestion.

**Why:** Raw's job is to be a faithful record of what the source said, so history can be replayed
when transform logic changes. NDJSON is schema-agnostic — a new source field just appears in the
file. A fixed Parquet schema either rejects the write or drops the field silently, and either way
you've lost data you can never recover because the source is a rolling window.

**Cost:** Larger files and slower reads than Parquet. Acceptable because raw is read once per
batch by Spark, not queried interactively. Parquet starts at the Spark *output*, where the schema
is ours and stable.

---

## AD-004 — Watermark in object storage, not Airflow XCom

**Decided:** `_state/watermark.json` in the same bucket as the data.

**Against:** Airflow XCom, an Airflow Variable, or a local file.

**Why:** Two requirements: it must survive the Airflow instance being rebuilt from scratch, and it
must be readable by a manual backfill run executed with no Airflow involved at all. XCom fails
both. A local file fails the first and doesn't work across containers.

**Cost:** An extra S3 round-trip per run, and no transactional guarantee between "files written"
and "watermark advanced." Handled by AD-005.

---

## AD-005 — At-least-once delivery: advance the watermark last

**Decided:** Write all files → write the manifest → only then advance the watermark. Never advance
on a short read.

**Against:** Advancing the watermark first, or in the same step as the write.

**Why:** If the run crashes after writing three of five files, an already-advanced watermark means
the missing two are lost permanently and silently — the worst kind of data loss, because nothing
errors. Advancing last means a crash causes the next run to re-read the same window. Duplicates are
recoverable; missing rows from a rolling-window source are not.

**Cost:** Duplicate records in raw. Resolved downstream by deduping on `Case_Number`, keeping the
latest by update timestamp. This is the standard at-least-once + idempotent-sink pattern.

---

## AD-006 — Reconcile a pre-flight count against landed rows

**Decided:** Query `returnCountOnly=true` before paging, compare to what actually landed, flag
`short_read` and refuse to advance the watermark on a mismatch.

**Against:** Trusting the paging loop to terminate correctly.

**Why:** The failure mode this catches is a page that returns empty due to a transient server issue
rather than genuine exhaustion. Without the count you cannot distinguish "done" from "broke," and
you'd advance the watermark past data you never read.

**Cost:** One extra query, plus false positives are possible in reverse — new records can arrive
*between* the count and the paging, making the actual count slightly higher. Only a *shortfall* is
treated as an error.

---

## AD-007 — Page ordered by `OBJECTID`

**Decided:** `orderByFields=OBJECTID ASC` on every paged query.

**Against:** Unordered offset paging.

**Why:** `resultOffset` without a stable sort is undefined behaviour. If the server's underlying
ordering shifts between requests — which it can, under concurrent writes — offset paging skips and
duplicates rows. `OBJECTID` is monotonic and immutable on hosted ArcGIS layers.

**Cost:** Forces a server-side sort on every page. Negligible at this volume.

---

## AD-008 — LocalStack for local development

**Decided:** LocalStack for S3, with the endpoint swapped by a single config value.

**Against:** Developing directly against real AWS, or mocking S3 in application code.

**Why:** Anyone cloning the repo can run the full pipeline with no AWS account and no cost. Because
only `endpoint_url` differs, the code path exercised locally is the real boto3 path — not a mock —
so local success is meaningful evidence the AWS path works.

**Cost:** LocalStack's S3 is not byte-identical to AWS in edge cases (consistency semantics, some
error codes). Anything that depends on those needs verification against real AWS before you trust it.

---

## AD-009 — App may create its bucket locally, never on AWS

**Decided:** `AUTO_CREATE_BUCKET` defaults to false; true only for local.

**Against:** Always creating the bucket if missing.

**Why:** On real AWS the bucket is Terraform's responsibility. An application that quietly creates
infrastructure produces unmanaged resources that drift from IaC — the exact problem Terraform exists
to prevent. A missing bucket in production should fail loudly.

**Cost:** One more config flag. Worth it; this was also a real bug caught by the test suite, where
tying the behaviour to the LocalStack endpoint check broke every non-LocalStack environment.

---

## AD-010 — Tests stub the source and the sink

**Decided:** `responses` for ArcGIS, `moto` for S3. No network in CI.

**Against:** Integration tests against the live API.

**Why:** CI that depends on a third-party public API fails for reasons unrelated to your code, and
teaches everyone to ignore red builds. Stubbing lets us test the cases that actually matter and are
hard to trigger on demand: ArcGIS returning HTTP 200 with an error body, empty first pages, short
reads, the paging circuit breaker.

**Cost:** Stubs encode assumptions about the API's shape. If Denver changes response format, tests
stay green while production breaks. Mitigated by `make describe`, which prints the live schema — run
it when something looks wrong.
