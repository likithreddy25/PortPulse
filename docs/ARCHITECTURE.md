# PortPulse Architecture

Status: Phases 1-8 built and tested (locally, plus a partial real AWS
deployment — Kinesis stream, S3 bucket, IAM roles, Glue database/table;
see [README.md](../README.md#build-status) and #cost for exactly what's
real vs. local-only). This doc describes the target end-state
architecture; components not yet built are marked accordingly.

## Data flow

```
AISStream.io (WebSocket)
      |
      v
[Phase 1 - BUILT] Python WebSocket client (ingestion/aisstream_client.py)
      | normalizes: MMSI, lat, lon, speed, course, timestamp, port_zone
      v
[Phase 1 - REAL: Kinesis stream "portpulse-ais-positions" exists in AWS, empty] Kinesis Data Stream (on-demand, single stream)
      |
      v
[Phase 2 - config/code built + locally validated, delivery stream NOT created] Kinesis Data Firehose
      | batches, converts to Parquet, dynamic-partitions by port_zone/date/hour
      | (see README Build status: the Parquet+dynamic-partitioning combo is
      | unverified against real AWS; tested fallback config exists; also
      | blocked behind the Glue crawler AccessDenied issue below)
      v
[Phase 2 - locally proven via streaming/local_preview.py; real S3 bucket exists, empty] S3 raw zone (partitioned by port/date/hour)
      |
      v
[Phase 2 - REAL: Glue database + table exist; Crawler creation blocked by an account-level AccessDeniedException, under investigation] Glue Crawler -> Glue Data Catalog
      |
      v
[Phase 3 - BUILT + unit tested + run locally against real captured data] EMR Serverless PySpark job (hourly, via EventBridge)
      | geofencing (20km anchorage radius), dwell time, congestion_index
      | -- not yet pointed at real EMR Serverless, only local Spark
      v
[Phase 3 - BUILT: writes local Parquet partitioned by port_id, not yet S3] S3 curated zone (Parquet)
      |
      v
[Phase 4 - schema + load scripts BUILT + validated against real Postgres, NOT deployed] Redshift Serverless (COPY/MERGE load)
      | star schema: fact_port_congestion_hourly, dim_port (real), dim_vessel_type
      | (real standardized AIS ship-type reference data, not yet joined to any fact)
      v
[Phase 5 - BUILT + tested against real data (correctly refuses to forecast, insufficient history) and synthetic data (forecast path works)] SARIMAX forecast per port, written back to Redshift
      |
      v
[Phase 6 - BUILT + verified live in a browser against real data] Dashboard (Streamlit)
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
2. **Enrichment-time (Phase 3, built):** radius-based distance
   (`batch/emr_jobs/geofence.py`, 20km from each port's center) from
   each port's anchorage center, used to detect zone-entry/zone-exit
   events for dwell-time computation. This is the layer the
   congestion_index formula actually depends on. **Known limitation,
   found by running against real data**: a single 20km circle around one
   center point doesn't distinguish vessels genuinely queued at anchor
   from vessels already docked at a berth inside a busy harbor complex
   (Rotterdam showed 401 vessels/hour, 339 of them "queued" by this
   definition) — see README Build status for the full writeup. Worth
   revisiting with tighter, port-specific anchorage boundaries if real
   port authority geometry becomes available.

## Deployment blocker (as of 2026-08-17)

`streaming/deploy.py --confirm` successfully created the Kinesis stream,
S3 bucket, both IAM roles, and the Glue database/table, then failed on
`glue:CreateCrawler` with `AccessDeniedException: Account ... is denied
access`. Ruled out: the deploying IAM user has `AdministratorAccess`
(not a permissions problem), and the account isn't part of an AWS
Organization (not an SCP). Likely an account-level restriction (the AWS
account is under a month old) requiring either an AWS Support case or a
Console-side check — being investigated by the project owner. Firehose
delivery stream creation hasn't been attempted yet since it comes after
the crawler step in the deploy sequence.

## Redshift schema validation notes

`warehouse/redshift_ddl/schema.sql` was checked against a real local
Postgres container (Docker), not just read over -- this caught a column
width bug and, more importantly, surfaced two real Postgres-vs-Redshift
dialect differences that would otherwise have only shown up against a
live, billable Redshift endpoint:
1. Redshift has no `ON CONFLICT ... DO UPDATE` -- upserts use `MERGE`
   (confirmed against AWS's MERGE docs, `load.sql` rewritten accordingly).
2. Redshift does not enforce PK/FK/UNIQUE constraints (only `NOT NULL`)
   -- Postgres does, so a bad `port_id` or duplicate `(port_id,
   hour_start)` that Postgres would reject would load into real Redshift
   silently. `load.sql`'s `MERGE` is what actually prevents duplicates in
   production, not the declared primary key.

`dim_vessel_type` is seeded from the standardized AIS/ITU-R M.1371 ship
type classification (real external reference data) but isn't joined to
any fact data yet -- `ingestion/aisstream_client.py` only requests
AISStream's `PositionReport` message type, not `ShipStaticData` (where
AIS actually reports vessel type). Capturing that and mapping MMSI ->
vessel_type_id would be the natural next step to make the dimension
actually used rather than a populated-but-unreferenced reference table.

## Infrastructure as Code (Phase 8)

`infra/cloudformation/` has three split stacks (`ingestion.yaml`,
`processing.yaml`, `warehouse.yaml`) consolidating everything above.
Verified via `cfn-lint` (zero warnings) and `aws cloudformation
validate-template` against the real account (read-only). **Not
deployed** -- and `ingestion.yaml` specifically can't be deployed as a
new stack without conflicts right now, since it defines resources
(Kinesis stream, 2 IAM roles, Glue database/table) with the exact same
names as what `streaming/deploy.py` already created for real in Phase 2.
See `infra/cloudformation/DEPLOY.md` for deployment order, commands, and
this conflict in full.

## Orchestration and monitoring

Not shown in the linear data-flow diagram above since these are
cross-cutting rather than part of the pipeline itself:
- **EventBridge (hourly)** -> Lambda (`orchestration/trigger_emr_job.py`)
  -> EMR Serverless `StartJobRun`. Not runnable for real yet -- no EMR
  Serverless application exists.
- **EventBridge (every 15 min)** -> Lambda
  (`orchestration/check_freshness.py`) -> queries Redshift for the
  newest curated row -> publishes `PortPulse/CuratedFreshnessMinutes` to
  CloudWatch. Not runnable for real yet -- Redshift isn't deployed.
- **CloudWatch alarms**: Firehose `FailedConversion.Records >= 1`, and
  `CuratedFreshnessMinutes > 90` for 2 consecutive periods.

`orchestration/deploy.py` creates all of the above, verified via a full
mocked deployment (moto) and a real read-only dry-run against the actual
AWS account -- see README Build status.

## IAM / security model

Deferred to Phase 8 (CloudFormation). Will follow least-privilege: the
ingestion client's IAM identity gets `kinesis:PutRecord`/`PutRecords` on
exactly the one stream, nothing else.
