# PortPulse Architecture

Status: Phases 1-2 built and locally validated, not yet deployed to real
AWS (see [README.md](../README.md#build-status)). This doc describes the
target end-state architecture; components not yet built are marked
accordingly.

## Data flow

```
AISStream.io (WebSocket)
      |
      v
[Phase 1 - BUILT] Python WebSocket client (ingestion/aisstream_client.py)
      | normalizes: MMSI, lat, lon, speed, course, timestamp, port_zone
      v
[Phase 1 - config/code built, NOT deployed] Kinesis Data Stream (on-demand, single stream)
      |
      v
[Phase 2 - config/code built + locally validated, NOT deployed] Kinesis Data Firehose
      | batches, converts to Parquet, dynamic-partitions by port_zone/date/hour
      | (see README Build status: the Parquet+dynamic-partitioning combo is
      | unverified against real AWS; tested fallback config exists)
      v
[Phase 2 - locally proven via streaming/local_preview.py] S3 raw zone (partitioned by port/date/hour)
      |
      v
[Phase 2 - config built, NOT deployed] Glue Crawler -> Glue Data Catalog
      |
      v
[Phase 3 - NOT BUILT] EMR Serverless PySpark job (hourly, via EventBridge)
      | geofencing (anchorage radius), dwell time, congestion_index
      v
[Phase 3 - NOT BUILT] S3 curated zone (Parquet)
      |
      v
[Phase 4 - NOT BUILT] Redshift Serverless (COPY load)
      | star schema: fact_port_congestion_hourly, dim_port, dim_vessel_type
      v
[Phase 5 - NOT BUILT] SARIMAX forecast per port, written back to Redshift
      |
      v
[Phase 6 - NOT BUILT] Dashboard (React or Streamlit)
```

## Local development path (Phase 1)

Everything above the Firehose line can run entirely locally, with no AWS
account needed, via `ingestion/replay.py` reading a recorded or synthetic
JSONL sample and a `stdout`/`file` sink instead of Kinesis. This is what
lets the rest of the pipeline be developed without live-feed dependency
or AWS cost. See the Setup section in the README.

## Port zone assignment

Two layers, deliberately different levels of precision:
1. **Ingestion-time (Phase 1, built):** coarse bounding-box check
   (`ingestion/geofence.py`) — just tags which of the 5 port boxes a
   position falls in, using the same boxes AISStream is subscribed to.
2. **Enrichment-time (Phase 3, not built):** radius-based distance from
   each port's anchorage center, used to detect zone-entry/zone-exit
   events for dwell-time computation. This is the layer the
   congestion_index formula will actually depend on.

## IAM / security model

Deferred to Phase 8 (CloudFormation). Will follow least-privilege: the
ingestion client's IAM identity gets `kinesis:PutRecord`/`PutRecords` on
exactly the one stream, nothing else.
