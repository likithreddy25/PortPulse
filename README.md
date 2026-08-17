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
  format. **Not yet run against the live feed** — that requires an
  `AISSTREAM_API_KEY`, which is the user's to provide (see Setup below).
- `ingestion/replay.py` — replays a recorded/sample JSONL file of raw
  AISStream messages through the same normalize → sink pipeline the live
  client uses. **Verified working**: replaying the 240-message synthetic
  fixture below produces 240 normalized records, correctly geofenced
  across all 5 ports.
- 16 unit tests, all passing, all offline (no network or AWS calls) —
  run with `pytest tests/`.

**Not yet built:** Firehose/S3/Glue (Phase 2), EMR enrichment (Phase 3),
Redshift (Phase 4), forecasting (Phase 5), dashboard (Phase 6),
orchestration/monitoring (Phase 7), CloudFormation (Phase 8), teardown
tooling (Phase 9).

**About the sample data:** `ingestion/sample_data/synthetic_fixture.jsonl`
is **synthetically generated** (see
[`generate_synthetic_fixture.py`](ingestion/sample_data/generate_synthetic_fixture.py)),
not real captured AIS traffic — it exists so the pipeline can be built
and tested before a live feed is available. Once `AISSTREAM_API_KEY` is
set, a real ~10-minute capture can be taken with the command in Setup
below, and the replay mode works identically against it.

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
python -m ingestion.aisstream_client --sink file \
  --out ingestion/sample_data/captured_live.jsonl --max-seconds 600
```

Then replay that real sample the same way:

```bash
python -m ingestion.replay --sample ingestion/sample_data/captured_live.jsonl --sink stdout
```

## Cost

Phase 1 has zero AWS cost — everything above runs locally. A cost
estimate for the full deployed pipeline (Kinesis, Firehose, EMR
Serverless, Redshift Serverless, S3) will be documented here once later
phases are built and a teardown script exists (Phase 9), before anything
is actually deployed.

## Congestion index formula

To be defined and documented here in Phase 3, once the EMR enrichment
job that computes it exists.

## Resume bullet

To be written in Phase 9, and only for the parts that are genuinely
built, tested, and (if deployed) verified running.
