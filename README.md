# PortPulse

Real-time global port congestion analytics. Tracks live cargo-ship traffic
at 5 major container ports via AIS and computes a per-port congestion
index (vessel count, dwell time, queue length) with a short-horizon
forecast.

**Status: Phases 1-4 of 9 built and tested locally.** This is a
portfolio project built incrementally and validated at each phase — see
[Build status](#build-status) for exactly what's working today versus
what's still ahead. **Partial real AWS deployment exists** (Kinesis
stream, S3 bucket, IAM roles, Glue database/table — see Cost below); the
Glue crawler and Firehose delivery stream are blocked on an
account-level issue currently being investigated, not yet resolved.

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

**Not yet built:** forecasting (Phase 5), dashboard (Phase 6),
orchestration/monitoring (Phase 7), CloudFormation (Phase 8), teardown
tooling (Phase 9).

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

## Cost

**Currently deployed for real** (as of 2026-08-17): a Kinesis stream, an
S3 bucket, 2 IAM roles, and a Glue database/table — created while
testing Phase 2, then blocked on the Glue crawler step (see Build
status). These **are accruing real cost right now**, dominated by the
Kinesis stream's fixed hourly charge below. Everything else (Firehose
delivery stream, Glue crawler, Redshift namespace/workgroup) is
config/code only, not yet deployed.

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
| **Redshift Serverless** | **$0.375/RPU-hour, 4 RPU minimum = $1.50/hour while active**, billed per-second with a 60-second minimum per activation, **$0 while auto-paused/idle** | Not deployed yet. This is the single biggest per-hour rate in the whole project, but Serverless auto-pauses when nothing is querying it — a few minutes of testing a day stays well under $1. Leaving something actively querying it continuously would be the expensive mistake to avoid. |

**Bottom line:** what's deployed so far (Phase 1+2, partial) costs well
under $2-3 for a day or two of testing, dominated by the Kinesis
stream's fixed hourly charge. Redshift, if/when deployed, adds up fast
only if left continuously active — auto-pause keeps occasional testing
cheap. Phase 9 will add an automated `teardown.sh`; until it exists,
anything deployed here should be **manually deleted right after
testing** rather than left running. **Suggested AWS Budget alert:
$10/month**, generous enough not to false-alarm on normal testing but
tight enough to catch a forgotten resource (e.g., a stream left running
for days, a crawler put on an hourly schedule, or a Redshift workgroup
that never pauses) well before it becomes a real bill.

Sources: [Kinesis Data Streams pricing](https://aws.amazon.com/kinesis/data-streams/pricing/),
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

## Resume bullet

To be written in Phase 9, and only for the parts that are genuinely
built, tested, and (if deployed) verified running.
