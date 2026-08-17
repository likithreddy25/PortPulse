# PortPulse

Real-time global port congestion analytics. Tracks live cargo-ship traffic
at 5 major container ports via AIS and computes a per-port congestion
index (vessel count, dwell time, queue length) with a short-horizon
forecast.

**Status: Phase 1 of 9 complete.** This is a portfolio project built
incrementally and validated at each phase — see [Build status](#build-status)
for exactly what's working today versus what's still ahead. Nothing here
is deployed to AWS yet.

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

**Not yet built:** EMR enrichment (Phase 3), Redshift (Phase 4),
forecasting (Phase 5), dashboard (Phase 6), orchestration/monitoring
(Phase 7), CloudFormation (Phase 8), teardown tooling (Phase 9).

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

## Cost

Nothing has been deployed to real AWS yet — everything above runs
locally or against `moto`. Here's what deploying Phase 1+2
(`streaming/deploy.py --confirm`) would actually cost, sourced from
AWS's current published pricing (not estimated from memory):

| Resource | Rate | Notes |
|---|---|---|
| Kinesis Data Stream (on-demand) | **$0.04/stream-hour** (~$28.80/mo) + $0.08/GB ingested + $0.04/GB retrieved | **This is the one fixed cost that accrues just for the stream existing**, independent of traffic. Everything else below is usage-based and negligible at our data volume (a few thousand small messages/hour). |
| Firehose ingestion | $0.029/GB | rounded up to 5KB/record |
| Firehose format conversion (Parquet) | $0.018/GB | if the combined config works (see Build status) |
| Firehose dynamic partitioning | $0.02/GB + $0.005/1,000 objects, +$0.07/JQ-processing-hour | usage-based |
| S3 storage | ~$0.023/GB/month | negligible — our raw data is megabytes, not gigabytes |
| Glue crawler | $0.44/DPU-hour, 1-min minimum | a few cents per manual run; not scheduled yet (that's Phase 7) |
| Glue Data Catalog | free under 1M objects/1M requests | nowhere close at this scale |

**Bottom line:** running Phase 1+2 for a day or two of testing is well
under $2-3 total, dominated by the Kinesis stream's fixed hourly charge.
Phase 9 will add an automated `teardown.sh`; until it exists, anything
deployed here should be **manually deleted right after testing** rather
than left running. **Suggested AWS Budget alert: $10/month**, generous
enough not to false-alarm on normal testing but tight enough to catch a
forgotten resource (e.g., a stream left running for days, or a crawler
accidentally put on an hourly schedule) well before it becomes a real
bill.

Sources: [Kinesis Data Streams pricing](https://aws.amazon.com/kinesis/data-streams/pricing/),
[Firehose pricing](https://aws.amazon.com/kinesis/data-firehose/pricing/),
[Glue pricing](https://aws.amazon.com/glue/pricing/).

## Congestion index formula

To be defined and documented here in Phase 3, once the EMR enrichment
job that computes it exists.

## Resume bullet

To be written in Phase 9, and only for the parts that are genuinely
built, tested, and (if deployed) verified running.
