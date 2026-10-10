# PortPulse

Real-time global port congestion analytics. Tracks live cargo-ship traffic
at 5 major container ports via AIS and computes a per-port congestion
index (vessel count, dwell time, queue length) with a short-horizon
forecast.

**Status: all 11 phases built and tested** (Phases 1-9 as originally
planned, plus Phase 10 — a DynamoDB watermark/dedup table — and Phase 11
— a classical hypothesis-testing module). This is a portfolio project
built incrementally and validated at each phase — see
[Build status](#build-status) for exactly what's working today, what's
verified against real AWS vs. `moto`/local-only, and what's still
aspirational. **Nothing is currently deployed to AWS except one empty S3
bucket** — a partial Phase 2 deployment (Kinesis stream, IAM roles, Glue
database/table) ran for about an hour, then was torn down for real with
`teardown.sh` once Phases 1-9 were complete (see Cost below). Phase 10's
DynamoDB table was likewise deployed for real, exercised for about 3
minutes, and torn down, with the deletion verified. The Glue
crawler and everything downstream of it were blocked on an
account-level issue that's still unresolved. No component here has been
deployed as a continuously-running production pipeline — see the Resume
bullet section for exactly what is and isn't claimed.

## Ports tracked

| Port | ID | Center | Bounding box (approx, ±0.35 lat / ±0.45 lon) |
|---|---|---|---|
| Los Angeles / Long Beach, USA | `USLAX` | 33.7, -118.2 | see [`ingestion/config.py`](ingestion/config.py) |
| Shanghai, China | `CNSHA` | 30.6, 122.0 | " |
| Singapore | `SGSIN` | 1.26, 103.8 | " |
| Rotterdam, Netherlands | `NLRTM` | 51.9, 4.0 | " |
| New York / New Jersey, USA | `USNYC` | 40.6, -74.0 | " |

These boxes are deliberately generous rectangles around each port's
approach/anchorage area, not authoritative port-authority boundaries.
They control what AISStream.io streams to us; Phase 3's geofencing does
tighter radius-based zone assignment on top.

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full data flow.
In short: AISStream.io (WebSocket) → Kinesis Data Stream → Firehose → S3
(Parquet, raw zone) → Glue Catalog → EMR Serverless (PySpark enrichment)
→ S3 (curated zone) → Redshift Serverless (star schema) → dashboard, with
a SARIMAX forecast layered on top of the congestion time series.

## Airflow orchestration (added)

`orchestration/dags/portpulse_enrichment_dag.py` is an Apache Airflow DAG
for the hourly enrichment run: `validate_job_spec -> start_emr_job ->
wait_for_emr_job -> check_freshness`. It validates the YAML/JSON job spec
first (a bad config fails at task one, not mid-Spark-job), starts the EMR
Serverless job with the spec shipped via `--files`/`--config`, polls with
a reschedule-mode sensor that fails on a FAILED/CANCELLED run, then
reuses the freshness handler as a final data-quality gate. Retries,
exponential backoff, `catchup=False` and `max_active_runs=1` are set.
Task logic lives in `orchestration/airflow_tasks.py` (Airflow-free, so it
is unit-tested with injected fake clients).

**What's real vs. not:** `tests/test_airflow_dag.py` (6 tests) imports the
DAG under real Airflow 2.10 via `DagBag` and checks structure, ordering,
retries and sensor mode, and exercises the task callables. It has **not**
been deployed to MWAA or run against a live EMR Serverless application --
consistent with the rest of this project's "validated, not deployed"
status.

## Real AWS deployment (2026-10-06, torn down)

The ingestion CloudFormation stack (`infra/cloudformation/ingestion.yaml`) was deployed to a real
account (us-east-1) and exercised end to end, then fully torn down (~25 min, estimated cost a few cents):

- **Replayed 2,717 real captured AIS records** through Kinesis -> Firehose -> S3. Firehose accepted
  JSON->Parquet conversion **plus** dynamic partitioning in one delivery stream (previously unverified).
- Result: 4 Parquet files / **2,717 rows** (exact match to the replay count) under
  `raw/port_zone=<zone>/date=/hour=/`, correct schema (int64 mmsi, float64 lat/lon/speed/course, int32
  nav_status), `raw-errors/` empty.
- Two real findings fixed in the templates: the Glue crawler is denied at account level, so
  `CreateGlueCrawler` is now an optional parameter (default off; the table is declared explicitly anyway),
  and `pipeline.py` alone cannot run on EMR (imports local packages and openlineage), so
  `scripts/build_emr_bundle.sh` builds a `--py-files` bundle (openlineage-python pinned to 1.45.0 because
  EMR 7.1.0 runs Python 3.9). `processing.yaml` gained `HourlyTriggerState` for manual test deploys.
- **Not completed:** the EMR Serverless job run. The account's EMR Serverless "Max concurrent vCPUs"
  quota is 0, so the application could not be created; an increase was requested. The Redshift
  warehouse and the Airflow DAG have still not been run on real infrastructure.
- Also open: the scheduled Lambda trigger in `processing.yaml` does not pass `--py-files`, so it would fail
  on EMR as written.

## Build status

**What's actually built and tested (Phase 1):**
- `ingestion/schema.py` — maps AISStream's raw `PositionReport` JSON to a
  normalized record (MMSI, lat, lon, speed, course, timestamp, port_zone).
  Unit tested (valid messages, malformed messages, non-position message
  types, out-of-zone positions).
- `ingestion/geofence.py` — coarse bounding-box port-zone assignment.
- `ingestion/sinks.py` — pluggable output sinks: stdout, local JSONL file,
  and Kinesis (`PutRecord`/`PutRecords`, partitioned by MMSI so a vessel's
  updates stay ordered within a shard). The Kinesis sink is unit tested
  against a mocked AWS API (`moto`) — **no real Kinesis stream has been
  created and no AWS costs have been incurred.**
- `ingestion/aisstream_client.py` — live WebSocket client for
  AISStream.io, built against their documented subscription/message
  format. **Run live and verified**: connected to `wss://stream.aisstream.io/v0/stream`
  with a real API key and captured a 10-minute sample
  (`ingestion/sample_data/captured_live.jsonl`, committed to the repo) —
  **2,717 real `PositionReport` messages from 1,167 distinct vessels.**
  Per-port breakdown from that specific 10-minute window: NLRTM (Rotterdam)
  1,933, USLAX (LA/Long Beach) 352, USNYC (NY/NJ) 284, SGSIN (Singapore)
  148, **CNSHA (Shanghai) 0.** The Shanghai bounding box didn't see a
  single position report in this window — worth watching once ingestion
  runs continuously; could be terrestrial AIS receiver coverage gaps in
  AISStream's free tier near Yangshan, or just this specific 10-minute
  slice, not yet root-caused.
- `ingestion/replay.py` — replays a recorded/sample JSONL file of raw
  AISStream messages through the same normalize → sink pipeline the live
  client uses. **Verified working on both**: the 240-message synthetic
  fixture (240/240 normalized, all 5 ports present, since it's synthetic)
  and, more importantly, the real captured sample above (2,717/2,717
  raw messages replayed to normalized records with no data loss).
  **Correction (found in Phase 10):** "no data loss" was true of the
  record count but not of the timestamps. 2,713 of those 2,717 records
  were silently stamped with the *replay* time instead of the real AIS
  time, because AISStream sends nanosecond-precision fractions that the
  parser rejected. Fixed in Phase 10; see there for the details and for
  what it changed downstream.
- Along the way, replaying the *first* live capture attempt caught a real
  bug: the capture path was writing already-normalized records instead of
  raw AISStream JSON, so `replay.py`'s normalize step silently produced
  zero records. Fixed with an explicit `--raw` flag
  (`ingestion/aisstream_client.py`) and covered by 3 new tests before
  re-capturing.
- 19 unit tests, all passing, all offline (no network or AWS calls) —
  run with `pytest tests/`.

**What's built and locally tested (Phase 2, not yet deployed to real AWS):**
- `streaming/table_schema.py` — single source of truth for the raw table
  schema (mmsi, lat, lon, speed_knots, course_deg, timestamp, port_zone,
  ship_name, nav_status), consumed by both the Glue table definition and
  the local Parquet writer, so they can't drift apart.
- `streaming/local_preview.py` — a local stand-in for what Firehose+Glue
  do in production: reads a JSONL sample, partitions by
  `port_zone/date/hour`, writes real Parquet files. **Verified against
  the real captured sample**: all 2,717 records converted and
  partitioned correctly (6 partitions, e.g. `port_zone=NLRTM/date=2026-08-17/hour=15`
  with 1,930 rows), schema round-trips cleanly through pyarrow.
  (*Correction, Phase 10:* the `hour=15` partitions came from the
  replay-time timestamp bug. The real capture ran 14:44–14:54 UTC, so
  after the fix every record lands in `hour=14`.)
- `streaming/firehose_config.py`, `streaming/glue_catalog_config.py` —
  the real Firehose delivery stream and Glue database/table/crawler
  configs. **Open question, documented in the code, not hidden**: AWS's
  docs don't clearly confirm whether a single Firehose delivery stream
  can combine Parquet format conversion with dynamic (content-based)
  partitioning — checked multiple doc pages, found no definitive answer
  either way. A tested fallback config (dynamic partitioning only, raw
  gzip JSON, Parquet conversion deferred to Phase 3's EMR job) is ready
  if AWS's API rejects the combined config on first real deploy.
- `streaming/deploy.py` — the boto3 script that would create the real
  resources (Kinesis stream, S3 bucket, IAM roles, Glue database/table/
  crawler, Firehose delivery stream). **Verified two ways**: a full
  mocked deployment (via `moto`) exercises every function end-to-end
  including idempotent re-runs, and a real dry-run (`--region us-east-1`,
  no `--confirm`) against the actual AWS account confirmed the plan
  resolves correctly and **created nothing** (every check is a read-only
  `describe`/`head`/`get` call). Actually creating these resources
  (the `--confirm` flag) has not been run — see Cost below.
- 31 unit tests total, all passing, all offline (no real AWS calls).

**What's built and tested (Phase 3 — EMR/Spark enrichment, run locally,
not yet pointed at EMR Serverless):**
- `batch/emr_jobs/geofence.py` — radius-based anchorage geofencing
  (haversine distance, 20km radius from each port's center point — see
  the module docstring for why 20km and its limitations).
- `batch/emr_jobs/dwell.py` — PySpark transformation that groups each
  vessel's consecutive in-zone position reports into "visits" (a
  standard gaps-and-islands technique over a window function) and
  computes dwell time per visit, correctly distinguishing *completed*
  visits (vessel later observed leaving) from *still-open* ones (vessel's
  last known position is still inside the zone).
- `batch/emr_jobs/congestion.py` — hourly curated aggregation
  (vessel_count, queue_length, avg_dwell_time_hours) and the
  congestion_index formula, **explicitly documented** in that module's
  docstring: a weighted sum of three capped/normalized components
  (0.4 weight on dwell time, 0.3 each on vessel count and queue length),
  with the caps stated as reasonable starting assumptions rather than
  values derived from historical data (there isn't any yet).
- 15 new unit tests (synthetic multi-vessel, multi-hour scenarios run
  against a real local Spark session) verifying: geofencing distances,
  correct completed-vs-open visit classification, correct hour-bucket
  assignment for dwell time, and the congestion_index formula matching
  its documented definition exactly. 44 tests total, all passing.
- **Run against the real captured sample** (`ingestion/sample_data/captured_live.jsonl`,
  replayed and fed through `batch.emr_jobs.pipeline`) and it produced
  real, honest, and slightly humbling results:
  - `avg_dwell_time_hours` came back essentially null/zero for every
    port. This is expected and correct, not a bug: the capture window
    is 10 minutes, and no real vessel completes an anchorage visit that
    fast. A couple of near-zero values (~1-10 microseconds) showed up
    from vessels with two AIS reports milliseconds apart flipping zone
    status right at the 20km boundary — a real data-quality nuance in
    raw AIS, not something the pipeline mishandled.
    **Correction (Phase 10): that explanation was wrong.** The
    microsecond gaps were the timestamp bug: nearly every record carried
    its replay time, milliseconds apart, instead of its real AIS time.
    Re-run with corrected timestamps, the same job gives NLRTM
    `avg_dwell_time_hours` = 0.0056 (~20 s) and USNYC = 0.033 (~2 min),
    with SGSIN/USLAX still null. That's still "no completed anchorage
    visit in a 10-minute window," so the conclusion stands, but the
    explanation was wrong. `vessel_count`/`queue_length` were unchanged
    (NLRTM still 401/339), and `congestion_index` still pins at 60.0.
  - `vessel_count` and `queue_length` came back **far higher than
    expected** for busy ports — Rotterdam alone showed 401 distinct
    vessels and 339 of them under 1 knot within a single hour. High
    enough to saturate both component caps (VESSEL_COUNT_CAP=40,
    QUEUE_LENGTH_CAP=25), which is why `congestion_index` pinned at
    60.0 for 4 of the 5 ports rather than showing meaningful spread.
    Documented rather than quietly adjusted: this is a real signal that
    (a) the caps need real historical data to calibrate properly, and
    (b) a single 20km-radius circle around one port center point
    doesn't distinguish vessels genuinely queued at anchor from vessels
    already docked/moored at a berth inside a busy harbor complex like
    Rotterdam's — both look like "in zone, low speed." Worth revisiting
    with tighter, port-specific anchorage boundaries if real port
    authority data becomes available.
  - **Bottom line: the computation logic is verified correct (via unit
    tests); the specific congestion_index numbers produced from this
    10-minute real sample are not meaningful port congestion
    measurements** — they're a byproduct of too short a window and
    uncalibrated caps, not a finished analytical result.

**What's built and tested (Phase 4 — Redshift warehouse, not yet deployed
to real AWS):**
- `warehouse/redshift_ddl/schema.sql` — star schema: `dim_port` (real
  data, generated straight from `ingestion/config.py` so it can't drift
  from the ports actually tracked — see `generate_dim_port_seed.py`),
  `dim_vessel_type` (seeded from the standardized AIS/ITU-R M.1371 ship
  type classification — real external reference data, grouped at the
  tens-digit level; **not yet linked to any fact data**, since ingestion
  currently only requests AISStream's `PositionReport` message type, not
  `ShipStaticData` where AIS actually reports vessel type — flagged in
  the DDL, not hidden), and `fact_port_congestion_hourly` (one row per
  port per hour, matching Phase 3's curated output columns).
- **Validated against a real local Postgres container** (via Docker),
  not just eyeballed — this caught two real mistakes before they'd have
  hit a billable Redshift endpoint: a `VARCHAR(100)` column too narrow
  for the longer AIS category descriptions, and an earlier draft of
  `load.sql` using PostgreSQL's `ON CONFLICT ... DO UPDATE` syntax,
  which **Redshift does not support** — confirmed against AWS's own
  MERGE documentation and rewritten to use Redshift's actual `MERGE`
  statement. Also documented rather than left implicit: **Redshift does
  not enforce PRIMARY KEY / FOREIGN KEY / UNIQUE constraints** (only
  `NOT NULL`) — confirmed against AWS docs — so `load.sql`'s `MERGE`
  keyed on `(port_id, hour_start)` is what actually prevents duplicate
  rows in production, not the declared primary key.
- `warehouse/deploy.py` — creates the Redshift Serverless namespace +
  workgroup and applies the schema via the Redshift Data API (no VPC
  network path needed). **Could not be verified via `moto`** — checked
  directly, `moto` doesn't implement `redshift-serverless` yet
  (`CreateNamespace` returns "Not yet implemented"), unlike Phase 2's
  Kinesis/S3/Glue/Firehose, which did have moto support. Instead: the
  exact request shapes were checked against AWS's own API reference
  (camelCase parameters, confirmed against real docs rather than
  guessed), and a real dry-run against the actual AWS account exercised
  the read-only existence checks and confirmed the plan resolves
  correctly, creating nothing.
- A real bug caught along the way: the first version of the SQL-file
  statement splitter broke on semicolons that appear naturally inside
  comment *prose* (e.g. "...enforced; only NOT NULL is") — before
  stripping comments first, it silently mis-parsed `schema.sql` into a
  single giant garbage statement. Fixed and covered by 8 new tests.
- 53 tests total, all passing (Docker-dependent warehouse tests skip
  gracefully if Docker isn't running).
- **Not yet deployed for real** — `publiclyAccessible=False` by design
  (querying it should go through the Redshift Data API, not direct
  psql/JDBC, to avoid needing VPC/security-group setup this project
  doesn't have yet).

**What's built and tested (Phase 5 — SARIMAX forecasting):**
- `forecasting/sarimax_forecast.py` — fits a SARIMAX(1,1,1) model (no
  seasonal component yet — see below) per port on the hourly
  `congestion_index` series and forecasts a configurable horizon (default
  6 hours) with 80% confidence intervals.
- **Honest about data sufficiency by design, not by afterthought**: it
  refuses to fit and returns an explicit `insufficient_data` result (with
  a stated reason) unless there are at least 48 hourly observations
  *and* at least 80% hourly coverage over their time span (so a handful
  of points scattered across weeks doesn't pass just because the raw
  count is high enough). Two separate failure modes, both tested.
- **Run against the real curated data** from Phase 3
  (`python -m forecasting.run_forecast --curated /tmp/portpulse_curated`)
  and it correctly reported `insufficient_data` for all 5 ports — real
  history is currently 1-2 hourly observations per port (from the single
  10-minute capture; *exactly 1* per port after Phase 10's timestamp fix,
  re-run and still `insufficient_data`), nowhere near the 48-hour minimum. **No fake-looking
  chart was produced**, exactly per the project brief. This is asserted
  by an automated test, not just eyeballed once.
- **Also run against a labeled-synthetic 96-hour series** (daily sine
  pattern + noise, clearly not real data) to prove the "ok" forecast path
  actually works end-to-end, not just in isolated unit tests. Real,
  observed limitation worth noting: the forecast came back essentially
  flat (~32 ± widening intervals) rather than tracking the injected daily
  cycle — expected, since `order=(1,1,1)` has no seasonal term. Capturing
  daily/weekly seasonality reliably needs several weeks of real history
  (order of 336-504+ hourly points) to fit a seasonal order without
  overfitting; upgrading the model once that much real data exists is a
  documented next step, not something faked now.
- 5 new unit tests, 58 total, all passing.

**What's built and tested (Phase 6 — Dashboard):**
- `dashboard/app.py` — a Streamlit dashboard (chosen over React: no build
  step, fast to make "real and demo-able" for a Python-heavy data
  project like this). Shows live congestion_index per port with a
  forecast overlay, vessel_count/queue_length/dwell-time metrics, and an
  explicit banner stating which data source is active — **never
  ambiguous about whether you're looking at demo data or a live
  warehouse.**
- Two data sources, never silently falling back from one to the other:
  `dashboard/data_source.py`'s `load_local()` (reads Phase 3's curated
  Parquet directly — works today) and `load_redshift()` (Redshift Data
  API — no VPC network path needed, but genuinely untested since
  Redshift isn't deployed).
- **Actually launched and verified in a browser against the real
  captured data**, not just written and assumed to work. Caught and
  fixed a real bug this way: Streamlit raised `StreamlitDuplicateElementId`
  on the per-port chart loop (needed an explicit unique `key` per port,
  since auto-generated IDs collided). After the fix, verified all 5
  ports render correctly — 4 with real metrics from the captured sample,
  and **Shanghai correctly showing "No data for this port yet"** rather
  than a blank or broken chart (consistent with the Phase 3 finding that
  the real capture had zero Shanghai messages).
- Also verified the Redshift ("live") mode by actually switching to it
  in the browser: with real AWS credentials present, it made a genuine
  (harmless, read-only) `ExecuteStatement` call against `redshift-data`,
  got back `ValidationException: Redshift endpoint doesn't exist in this
  region` (expected — nothing's deployed), and displayed that error
  cleanly instead of crashing the app.
- 2 new unit tests for `data_source.py` (58 → 60 total).

Run it: `streamlit run dashboard/app.py` (needs the Phase 3 curated
Parquet at `/tmp/portpulse_curated` for demo mode — see Setup).

**What's built and tested (Phase 7 — orchestration + monitoring, not
deployed):**
- `orchestration/trigger_emr_job.py` — Lambda handler, triggered hourly,
  starts an EMR Serverless job run of `batch/emr_jobs/pipeline.py`.
  **Not runnable for real yet** — no EMR Serverless application has been
  created (Phase 3 only runs the same pipeline locally). Application ID
  and role ARN come from environment variables so the same handler code
  works once Phase 8's CloudFormation creates the real application.
- `orchestration/check_freshness.py` — Lambda handler, triggered every 15
  minutes, queries Redshift for the newest curated row and publishes a
  `PortPulse/CuratedFreshnessMinutes` custom CloudWatch metric. Handles
  the empty-table case explicitly (reports a large finite staleness
  value rather than skipping the metric, so the alarm can actually
  evaluate it instead of silently having no data point).
- `orchestration/deploy.py` — creates both Lambda functions, an IAM role,
  two EventBridge schedule rules, and two CloudWatch alarms:
  - `FailedConversion.Records >= 1` (Firehose format-conversion
    failures) — checked against AWS's real metric docs and chosen over
    `DeliveryToS3.Success` as the primary signal because that one is
    ambiguous (AWS's own docs call it a "sum" but it's commonly alarmed
    on as a 0-1 ratio in AWS's examples — not resolved without a real
    running stream to observe, so not relied on here, documented rather
    than guessed).
  - `CuratedFreshnessMinutes > 90` for 2 consecutive periods (the "no new
    hourly row in >90 min" alarm from the project brief).
  - **Caught a real bug writing this**: the alarm's Firehose dimension
    defaulted to the *Kinesis* stream name (`portpulse-ais-positions`)
    instead of the *Firehose delivery stream* name (`portpulse-ais-raw`)
    — two different real resources with similar names. Fixed before it
    shipped, not after being wrong in production.
- **Verified two ways**, same pattern as every other deploy script here:
  a full mocked deployment (`moto`) exercises Lambda/EventBridge/
  CloudWatch end-to-end including idempotent re-runs, and a real dry-run
  against the actual AWS account confirmed the plan resolves correctly,
  creating nothing.
- 6 new tests (Lambda handler logic tested via dependency-injected mock
  clients, not a real Lambda runtime), 66 total, all passing.

**What's built and tested (Phase 8 — CloudFormation, not deployed):**
- Three split stacks in `infra/cloudformation/` (`ingestion.yaml`,
  `processing.yaml`, `warehouse.yaml`), consolidating everything built
  across Phases 1-7 into proper IaC rather than the ad hoc boto3 deploy
  scripts each phase used to get built and validated. See
  `infra/cloudformation/DEPLOY.md` for deployment order and commands.
- **Verified two ways, neither of which is "actually deployed"**: all
  three pass `cfn-lint` with zero warnings (caught and fixed one real
  issue — a redundant `DependsOn` cfn-lint correctly flagged), and all
  three pass `aws cloudformation validate-template` against the real AWS
  account (read-only, confirms YAML/resource/parameter structure,
  creates nothing).
- **Important, honestly-documented limitation found while writing
  `DEPLOY.md`**: `ingestion.yaml` cannot actually be deployed as a new
  stack right now without conflicts — the Kinesis stream, both IAM
  roles, and the Glue database/table it defines have the exact same
  names as the resources `streaming/deploy.py` already created for real
  in Phase 2 (see Cost above). Deploying this stack as-is today would
  hit `AlreadyExists` errors on four separate resources. Recommended
  fix, not yet done: run Phase 9's `teardown.sh` first.
- Verified two AWS API/CFN facts by checking real docs rather than
  guessing while writing these templates: CloudFormation's inline Lambda
  `ZipFile` always names the file `index.py` (so `Handler` must be
  `index.handler`, not e.g. `trigger_emr_job.handler` — different from
  how `orchestration/deploy.py` names it when zip-packaging the same
  source), and the exact required/optional properties for
  `AWS::EMRServerless::Application` and `AWS::RedshiftServerless::Namespace`/`Workgroup`
  (newer resource types, checked rather than assumed).
- 6 new tests, 72 total, all passing.

**What's built and tested (Phase 9 — cost control + teardown):**
- `teardown.sh` (thin wrapper) / `infra/teardown.py` (the real logic) —
  deletes every billable resource this project's deploy scripts could
  have created: Kinesis, Firehose, Glue (database/table/crawler), EMR
  Serverless, Redshift Serverless, Lambda functions, EventBridge rules,
  CloudWatch alarms, IAM roles, and (opt-in only, via `--delete-bucket`)
  the S3 bucket. Every delete checks existence first and no-ops if the
  resource isn't there — safe to run at any point, repeatedly.
- **Verified two ways**: a full mocked lifecycle (moto) for everything
  moto actually implements — deploys real-shaped fixtures, tears them
  down, asserts they're gone, and separately asserts tearing down an
  already-empty account doesn't raise. Redshift Serverless is excluded
  from that mocked test (checked directly: moto's mock doesn't
  implement `GetWorkgroup`/`GetNamespace`, so the not-found-is-a-no-op
  path can't be exercised through it) and instead verified with a real
  dry-run against the actual AWS account.
- **Then actually run for real, twice**: first a dry-run against this
  project's own live leftover Phase 2 resources (correctly found exactly
  the 6 that existed and nothing else), then `--confirm` for real —
  deleting the Kinesis stream, both IAM roles, and the Glue database/
  table, verified afterward by directly querying AWS and confirming
  `ResourceNotFoundException`/`NoSuchEntity` for all four. The S3 bucket
  was deliberately left alone (`--delete-bucket` not passed) — see Cost
  below for the full before/after.
- 4 new tests, 76 total, all passing.

**What's built and tested (Phase 10 — DynamoDB watermark/dedup table,
deployed for real, verified, and torn down):**
- **The gap it fills, checked first:** nothing in `ingestion/` or
  `streaming/` did dedup or tracked progress. Replaying the same sample
  twice into Kinesis would have published every record twice, and
  Phase 3's dwell job would have counted them twice.
- `streaming/watermark_store.py` — `get_watermark(mmsi, port_zone)` /
  `update_watermark(mmsi, port_zone, ts)` over a DynamoDB table (partition
  key `mmsi`, sort key `port_zone`, on-demand billing). The update is a
  **server-side conditional write**
  (`attribute_not_exists(last_processed_ts) OR last_processed_ts < :ts`),
  so the watermark only moves forward. A stale or equal timestamp is
  rejected by DynamoDB itself, not by a read-then-write race in Python.
  `last_processed_ts` is stored as epoch **microseconds (a number)**, not
  the pipeline's ISO string. DynamoDB compares strings byte-wise, and
  `isoformat()` drops the fraction entirely on whole seconds, so string
  comparison would be right only by accident of ASCII ordering. A
  dedicated test pins that case. Records outside every port zone are
  keyed under `OUT_OF_ZONE`, because DynamoDB key attributes can't be
  empty.
- **Wired into real pipeline behavior:** `ingestion/replay.py
  --watermark-table portpulse-watermarks` skips any record at or before
  its (vessel, zone) watermark, and advances the watermark only *after*
  the sink write succeeds. A crash between the two re-sends one record
  (at-least-once) rather than losing it. Watermarks are read once per
  (vessel, zone) per run and cached, so a replay costs one read per
  distinct vessel/zone, not one per message. Without the flag, replay
  behaves exactly as before.
- **Real bug found by this phase: 99.85% of real AIS timestamps were
  wrong.** The first moto run over the real capture looked broken: a
  second replay re-emitted 2,713 of 2,717 records instead of ~0, and 2
  records looked "late" by 42 days, which is exactly the gap between
  the capture date and the day of the test. Root cause: AISStream's
  `time_utc` has **nanosecond** precision with trailing zeros trimmed
  (`14:44:51.690367294`, 7-9 fractional digits in 2,713 of 2,717 real
  messages). Python's `%f` accepts at most 6, so `_parse_aisstream_time`
  hit its fallback and silently stamped each record with
  `datetime.now()`. The synthetic fixture uses 6-digit fractions, which
  is why Phases 1-9's tests never caught it. Fixed in `ingestion/schema.py`
  (truncate to microseconds before parsing; the now() fallback kept but
  made loud with a warning), covered by 2 new tests, one of which asserts
  every real captured record parses to its real 14:xx capture hour. The
  corrections this forced in earlier phases' write-ups are noted inline
  above (Phases 1, 2, 3, 5). Phase 3 was re-run locally to measure the
  impact rather than guess it.
- **After the fix, measured on the real capture (moto, offline):** 2,717
  records across 1,167 distinct (vessel, zone) pairs, 0 exact duplicates,
  and **0 out-of-order records** per vessel in file order. The strict
  watermark's known cost is dropping a late arrival older than something
  already sent, which is tested and documented in the module. It didn't
  bite at all on this real sample. Run 1: 2,717 emitted, 0 skipped.
  Run 2: 0 emitted, 2,717 skipped.
- `streaming/deploy_watermark.py` (`--confirm`-gated, same pattern as
  `streaming/deploy.py`), `infra/cloudformation/state.yaml` (a 4th,
  independent stack; passes `cfn-lint` with zero warnings and
  `aws cloudformation validate-template` against the real account, and a
  test asserts it matches the boto3 table definition exactly), and
  `infra/teardown.py`'s new `delete_dynamodb_tables` (waits for
  `table_not_exists`, since `DeleteTable` returns while the table is
  still `DELETING`).
- **Actually deployed for real, 2026-09-28**, then verified and torn down:
  - Dry run first: `python -m streaming.deploy_watermark` found no table
    and created nothing. Then `--confirm`: table `ACTIVE` in ~20 s
    (created 20:26:31 UTC, `PAY_PER_REQUEST`, keys `mmsi` HASH /
    `port_zone` RANGE).
  - `python -m streaming.watermark_smoke_test` ran real
    `get_watermark`/`update_watermark` calls on dummy MMSIs 1 and 2. First
    get → `None`. Forward writes → `True`. A stale write (15:01 after
    15:05:00.25) → **`False`, rejected by real DynamoDB**. An equal
    timestamp → `False`. Read-back → `15:05:00.250000` (microseconds
    preserved). Other zone → `None`. Out-of-zone key round-trips.
  - Then the **full real capture through real DynamoDB, twice**:
    `python -m ingestion.replay --sample ingestion/sample_data/captured_live.jsonl --sink file --out … --watermark-table portpulse-watermarks`.
    Run 1: **2,717 emitted, 0 skipped, 77 s**. Run 2: **0 emitted, 2,717
    skipped, 23 s**, with nothing written to the sink. A `Scan` counted
    1,169 items (1,167 vessel/zone pairs plus the 2 smoke-test items).
  - **Surprises worth knowing:** (1) `describe-table`'s `ItemCount` read
    **0** while a scan found 1,169. AWS refreshes that field only
    periodically (roughly every 6 hours), so it's not a way to check a
    fresh table. (2) Run 1's ~28 ms per message is one synchronous
    conditional `UpdateItem` per record from a laptop. That's fine for
    replay, but a live Kinesis producer at real volume would want batched
    or async watermark writes. Not built, just noted. (3) The prompt that
    kicked off this phase assumed on-demand mode is covered by the
    always-free 25 RCU/WCU tier. **It isn't**: AWS's pricing page ties
    that free tier to provisioned capacity. On-demand requests are billed
    from the first one; see Cost for the real number, which is still a
    fraction of a cent.
  - Torn down with `./teardown.sh` (dry run listed exactly one resource,
    the table, and nothing else, which also confirms nothing else from
    this project is deployed) and then `./teardown.sh --confirm`
    (finished 20:29:11 UTC, after waiting out `DELETING`). **Verified by a
    direct AWS call afterward**: `aws dynamodb describe-table --table-name portpulse-watermarks`
    → `ResourceNotFoundException`, and `list-tables` → `[]`. Total
    lifetime ~2 min 40 s.
- 24 new tests (20 watermark/deploy/replay/teardown against moto's
  DynamoDB, 2 for the timestamp fix, 2 new CloudFormation lint/validate
  cases for `state.yaml`), 100 total, all passing.

**What's built and tested (Phase 11 — classical hypothesis testing,
applied to real data):**
- `analysis/hypothesis_testing.py` — `compare_groups(a, b)` picks the
  test from the outcome type (continuous → two-sample t-test, categorical
  → chi-squared) and checks that test's assumptions. For the t-test that
  means Shapiro-Wilk normality and Brown-Forsythe equal variance, falling
  back to Welch's t or Mann-Whitney U. For chi-squared it means
  Cochran's expected-count rule, falling back to Fisher's exact test
  (2×2) or a permutation chi-squared test (larger tables). **It runs a
  power analysis from the sample sizes before interpreting the result**:
  the minimum detectable effect at 80% power, and the power to detect a
  stated smallest effect of interest (default Cohen's "small," d = 0.2 /
  w = 0.1). A non-significant result is then reported as
  `no_effect_detected` only if the test was adequately powered, and as
  `inconclusive_underpowered` otherwise. Too little data gives
  `insufficient_data`, and no test is run. Deliberately *not* "observed
  power," which is a known fallacy; the module docstring says why.
- **Applied to PortPulse's own real data first, and found degenerate.**
  After the Phase 10 timestamp fix, the 10-minute capture yields
  **exactly one** hourly `congestion_index` per port (NLRTM, SGSIN,
  USLAX, USNYC; none for CNSHA). The module reports NLRTM vs. SGSIN as
  `insufficient_data` (n = 1 vs. 1) instead of running a meaningless test.
  (All four values are 60.0 anyway; see Phase 3's cap saturation.)
- **So applied to the real 206-product Amazon-Reviews-2023 dataset from
  the sibling causal-price-lab project** instead, as a companion to its
  PSM analysis. It uses the same naive above-/below-median-price groups
  (102 vs. 104). The computed mean gap, +3.30 reviews, matches that
  project's reported naive ATE exactly. Results
  (`python -m analysis.run_real_hypothesis_tests`, full output in
  [`analysis/reports/real_hypothesis_tests.json`](analysis/reports/real_hypothesis_tests.json)):
  - Review growth: normality rejected (Shapiro p ≈ 1e-17, heavily
    right-skewed counts) → Mann-Whitney U, **p = 0.68**, d = 0.07.
  - Placebo (prior review growth): Mann-Whitney U, p = 0.98. The naive
    placebo gap (+3.61) is as large as the outcome gap. That's the
    confounding PSM exists to remove.
  - Price tier × review-growth tertile: expected counts fine (min 29.7)
    → χ²(2) = 0.71, **p = 0.70**, w = 0.06.
  - **All three: `inconclusive_underpowered`.** With about 100 per group,
    the minimum detectable effect is d ≈ 0.39 (w ≈ 0.22), and power to
    detect a small effect is only 0.23-0.30. That's reported as "can't
    tell small from none," not "no effect." causal-price-lab's README has
    a new section explaining why PSM was still needed on top of this.
- Two of my own test mistakes caught on first run, both fixed in the
  tests rather than the module. (1) A "clear effect is adequately
  powered" test assumed 200 per group gives 80% power at d = 0.2. It
  gives 0.51, and 500 per group is needed. (2) A test-selection test
  drew random normal data, and Shapiro-Wilk rejected it, which it does
  5% of the time by design. Selection tests now use deterministic
  normal-quantile data. Power calculations are checked against Cohen's
  (1988) published tables (64/group → d = 0.50; N = 88, 1 df → w = 0.30).
- 20 new tests, **120 total, all passing** (0 skipped, with Phase 3's
  curated output regenerated at `/tmp/portpulse_curated`; the 2
  real-curated-data tests skip without it, as before).

**About the sample data:** two files live in `ingestion/sample_data/`,
and they are not interchangeable —
- `synthetic_fixture.jsonl` is **synthetically generated** (see
  [`generate_synthetic_fixture.py`](ingestion/sample_data/generate_synthetic_fixture.py)),
  not real AIS traffic. It existed so the pipeline could be built and
  tested before a live feed/API key were available, and it's kept around
  because it deterministically covers all 5 ports (the real capture, by
  chance of timing, didn't).
- `captured_live.jsonl` is a **real** ~10-minute capture from the live
  AISStream.io feed (2,717 messages, 1,167 vessels, captured 2026-08-17
  — see Build status above for the exact per-port breakdown). The exact
  command used is in Setup below.

## Setup

```bash
cd ~/PortPulse
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env   # then add your AISSTREAM_API_KEY
```

Run the tests (fully offline):

```bash
pytest tests/ -v
```

Replay the synthetic fixture through the pipeline (stdout sink, no AWS needed):

```bash
python -m ingestion.replay --sample ingestion/sample_data/synthetic_fixture.jsonl --sink stdout
```

Capture a real ~10-minute sample from the live feed once your API key is
in `.env` (ask for confirmation before running this against the live feed):

```bash
python -m ingestion.aisstream_client --sink file --raw \
  --out ingestion/sample_data/captured_live.jsonl --max-seconds 600
```

(`--raw` writes AISStream's raw JSON instead of normalized records --
required because `replay.py` re-runs `normalize_message()` on read, so a
recorded sample has to look like what the live feed actually sends, not
the already-normalized output.)

Then replay that real sample the same way:

```bash
python -m ingestion.replay --sample ingestion/sample_data/captured_live.jsonl --sink stdout
```

Replay with watermark dedup (Phase 10). This needs the DynamoDB table
deployed, which is billable though tiny; see Cost. Deploy it, replay
twice, and the second run skips everything:

```bash
python -m streaming.deploy_watermark             # dry run: plan only
python -m streaming.deploy_watermark --confirm   # actually creates the table
python -m streaming.watermark_smoke_test         # real get/update calls, incl. a rejected stale write
python -m ingestion.replay --sample ingestion/sample_data/captured_live.jsonl \
  --sink file --out /tmp/replayed.jsonl --watermark-table portpulse-watermarks
./teardown.sh --confirm                          # deletes the table again
```

Run the classical hypothesis tests on real data (Phase 11; needs the
curated Parquet from the Spark step below, plus the sibling
causal-price-lab dataset):

```bash
python -m analysis.run_real_hypothesis_tests --curated /tmp/portpulse_curated \
  --causal-data ~/Downloads/causal-price-lab/data/processed/real_dataset_combined.parquet
```

Run the local Parquet + port/date/hour partitioning preview (stands in
for Firehose+Glue, no AWS needed):

```bash
python -m streaming.local_preview --sample ingestion/sample_data/captured_live.jsonl \
  --out streaming/local_preview_output/raw
```

Run the Spark enrichment job locally (needs a JDK — see note below —
and the normalized JSONL from a replay run above, not the raw capture):

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17   # or wherever your JDK 17 lives
python -m ingestion.replay --sample ingestion/sample_data/captured_live.jsonl \
  --sink file --out /tmp/normalized_live.jsonl
python -m batch.emr_jobs.pipeline --input /tmp/normalized_live.jsonl \
  --output /tmp/portpulse_curated --input-format json
```

PySpark needs a real JDK (8, 11, or 17) on the machine — it's not a pip
install away. On macOS: `brew install openjdk@17`, then point
`JAVA_HOME` at it as shown above (no need to symlink it system-wide).

Run the dashboard (after the Spark pipeline step above has produced
`/tmp/portpulse_curated`):

```bash
streamlit run dashboard/app.py
```

Opens at `http://localhost:8501` in "Local Parquet (demo)" mode by
default. Switching the sidebar to "Redshift (live)" only works once
Phase 4 is actually deployed.

## Cost

**Currently deployed for real (as of 2026-08-17): nothing except one
empty S3 bucket.** The Kinesis stream, 2 IAM roles, and Glue database/
table created while testing Phase 2 ran for about an hour (total cost:
roughly 3-4 cents, since no data ever actually flowed through the
stream — Phase 1's live capture wrote to a local file, not Kinesis) and
have since been **torn down for real** with `./teardown.sh --confirm`,
verified by directly querying AWS afterward and confirming
`ResourceNotFoundException`/`NoSuchEntity` for all four. The S3 bucket
(`portpulse-raw-<account-id>`) was deliberately left alone (opt-in via
`--delete-bucket`, not passed) — it's empty, holds nothing.

**Phase 10's DynamoDB watermark table** (`portpulse-watermarks`) was
deployed for real on 2026-09-28 and existed for ~2 min 40 s (created
20:26:31 UTC, deletion finished 20:29:11 UTC). It was torn down with
`./teardown.sh --confirm`, and the deletion was verified by a direct
`describe-table` → `ResourceNotFoundException` and an empty
`list-tables`. Real usage, counted from what was actually run:
~2,722 write request units (2,717 replay writes, plus 3 accepted and 2
rejected smoke-test writes; a rejected conditional write is still
billed) and ~2,350 read request units (2 × 1,167 strongly-consistent
replay reads, 4 smoke-test reads, plus a ~15.5-RCU verification scan).
At the published on-demand rates that's **≈ $0.0017 + $0.0003 ≈ $0.002
total**. Storage (~1,169 small items, well under 1 MB, for under 3
minutes) rounds to $0.

Everything else in this project (Firehose delivery stream, Glue
crawler, EMR Serverless, Redshift namespace/workgroup, Lambda/
EventBridge/CloudWatch) was never deployed at all — config/code only.

Rates sourced from AWS's current published pricing (not estimated from
memory):

| Resource | Rate | Notes |
|---|---|---|
| Kinesis Data Stream (on-demand) | **$0.04/stream-hour** (~$28.80/mo) + $0.08/GB ingested + $0.04/GB retrieved | **Currently running.** This is the one fixed cost that accrues just for the stream existing, independent of traffic. |
| Firehose ingestion | $0.029/GB | rounded up to 5KB/record; not deployed yet |
| Firehose format conversion (Parquet) | $0.018/GB | if the combined config works (see Build status) |
| Firehose dynamic partitioning | $0.02/GB + $0.005/1,000 objects, +$0.07/JQ-processing-hour | usage-based |
| S3 storage | ~$0.023/GB/month | negligible — our raw data is megabytes, not gigabytes |
| Glue crawler | $0.44/DPU-hour, 1-min minimum | a few cents per manual run; not scheduled yet (that's Phase 7); currently blocked (see Build status) |
| Glue Data Catalog | free under 1M objects/1M requests | nowhere close at this scale |
| DynamoDB (on-demand, Standard table class, us-east-1) | **$0.625/million write request units, $0.125/million read request units**, $0.25/GB-month storage | Phase 10's watermark table. Deployed for ~3 minutes, then torn down (see above). **Not covered by the always-free 25 RCU/WCU tier**, which AWS's pricing page ties to *provisioned* capacity. On-demand requests are billed from the first one. A strongly consistent read ≤ 4 KB = 1 RRU; a write ≤ 1 KB = 1 WRU. If run continuously, a live replay writes one WRU per message: ~$0.63 per million messages, plus one read per new vessel/zone per run. |
| **Redshift Serverless** | **$0.375/RPU-hour, 4 RPU minimum = $1.50/hour while active**, billed per-second with a 60-second minimum per activation, **$0 while auto-paused/idle** | Not deployed yet. This is the single biggest per-hour rate in the whole project, but Serverless auto-pauses when nothing is querying it — a few minutes of testing a day stays well under $1. Leaving something actively querying it continuously would be the expensive mistake to avoid. |

**Bottom line:** what's deployed so far (Phase 1+2, partial) costs well
under $2-3 for a day or two of testing, dominated by the Kinesis
stream's fixed hourly charge. Redshift, if/when deployed, adds up fast
only if left continuously active — auto-pause keeps occasional testing
cheap.

**Tear it all down:**

```bash
./teardown.sh                    # dry run: lists what would be deleted
./teardown.sh --confirm          # actually deletes it
./teardown.sh --confirm --delete-bucket --bucket-name portpulse-raw-<account-id>  # also empties + deletes the S3 bucket
```

The S3 bucket is opt-in (`--delete-bucket`) since it may hold real
captured data worth keeping even after everything else is torn down.
Every other resource, including Phase 10's DynamoDB watermark table, is
deleted by default when `--confirm` is passed.

**Suggested AWS Budget alert: $10/month**, generous enough not to
false-alarm on normal testing but tight enough to catch a forgotten
resource (e.g., a stream left running for days, a crawler put on an
hourly schedule, or a Redshift workgroup that never pauses) well before
it becomes a real bill. Not created by this project — Budgets require
billing-account-level permissions this project's IAM user may not have,
and creating one wasn't asked for, just suggested as the non-negotiables
in the project brief call for.

Sources: [DynamoDB on-demand pricing](https://aws.amazon.com/dynamodb/pricing/on-demand/)
and [free tier terms](https://aws.amazon.com/dynamodb/pricing/provisioned/),
[Kinesis Data Streams pricing](https://aws.amazon.com/kinesis/data-streams/pricing/),
[Firehose pricing](https://aws.amazon.com/kinesis/data-firehose/pricing/),
[Glue pricing](https://aws.amazon.com/glue/pricing/),
[Redshift pricing](https://aws.amazon.com/redshift/pricing/).

## Congestion index formula

Computed per port, per hour, in [`batch/emr_jobs/congestion.py`](batch/emr_jobs/congestion.py)
(full rationale in that file's docstring). `congestion_index` ranges 0-100:

```
congestion_index = 100 * (
    0.3 * min(vessel_count / 40, 1.0) +
    0.4 * min(avg_dwell_time_hours / 48, 1.0) +
    0.3 * min(queue_length / 25, 1.0)
)
```

- **vessel_count**: distinct vessels observed inside the port's 20km
  anchorage radius during that hour.
- **queue_length**: of those, how many were moving under 1 knot (i.e.
  actually queued/stationary, not just transiting through).
- **avg_dwell_time_hours**: average duration of anchorage *visits that
  concluded* during that hour (entry time to last-observed-in-zone time,
  for vessels later seen leaving — see `batch/emr_jobs/dwell.py`).
- **Weights** (0.4 dwell / 0.3 vessel count / 0.3 queue length): dwell
  time gets the highest weight because it's the most direct signal of
  actual delay; the other two are weighted equally as volume/backlog
  signals.
- **Caps** (40 vessels / 48 hours / 25 queued vessels) are the values
  each component saturates at (contributes its full weight). They're
  **documented starting assumptions, not derived from historical data**
  — there isn't enough yet. Running the real captured sample through
  this formula showed the vessel/queue caps are likely too low for busy
  ports (see Build status above) — a concrete, honest reason to
  recalibrate once real hourly history accumulates, not a hidden flaw.

## What this project does not claim
: there is no continuously-running production
deployment — a Glue crawler permissions issue (root cause outside this
project's control, still under investigation) blocked completing the
real AWS deployment beyond a partial Phase 2 (Kinesis stream, S3 bucket,
IAM roles, Glue database/table). Every phase is built and verified
either locally, against `moto`, or via real-but-read-only checks against
an actual AWS account — see Build status for exactly which is which per
component. No result or metric anywhere in this README was fabricated;
where real data wasn't sufficient to produce a meaningful number (the
forecast, the congestion-index calibration), that's stated outright
instead of shown as if it were.

## AI assistance

This project was built with the help of Claude (Anthropic) via Claude Code. I specified the design, reviewed the code and ran the tests and the live AIS capture.
