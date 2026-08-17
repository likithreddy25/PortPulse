-- Load scripts for the PortPulse Redshift warehouse. Run schema.sql
-- first, then seed_dim_port.sql and seed_dim_vessel_type.sql (both
-- static/reference data, no COPY needed), then this file for the fact
-- table.
--
-- NOT YET RUNNABLE FOR REAL: this COPY reads from
-- s3://<raw-bucket>/curated/, but Phase 3's curated output only exists
-- locally right now (see README Build status) -- nothing has been
-- uploaded to S3 yet. The command is correct and ready for when that
-- path exists; :bucket and :iam_role are psql variables, substitute
-- with actual values (or a real deploy script) rather than hardcoding
-- an account-specific ARN here.
--
-- Redshift's Parquet COPY infers the source schema from the Parquet
-- file itself and matches columns by name to the target table, so this
-- works as-is against batch/emr_jobs/pipeline.py's output schema
-- (port_id, date, hour, vessel_count, avg_dwell_time_hours,
-- queue_length, congestion_index) once hour_start is derived -- see the
-- staging-table approach below, since the curated Parquet has separate
-- `date`/`hour` string columns rather than a single timestamp column.

-- Note on upsert syntax: Redshift does NOT support PostgreSQL's
-- "ON CONFLICT ... DO UPDATE" (an earlier draft of this file used it,
-- caught during review by checking AWS's own MERGE documentation --
-- ON CONFLICT would have validated fine against the local Postgres
-- container used to sanity-check this schema, since Postgres does
-- support it, but would have failed against real Redshift). Redshift's
-- upsert primitive is the MERGE statement (GA since 2023), used below.

BEGIN;

CREATE TEMP TABLE stg_congestion_hourly_raw (
    port_id             VARCHAR(10),
    vessel_count        INTEGER,
    queue_length        INTEGER,
    avg_dwell_time_hours DECIMAL(10,4),
    date                VARCHAR(10),
    hour                VARCHAR(2),
    congestion_index    DECIMAL(5,1)
);

COPY stg_congestion_hourly_raw
FROM 's3://:bucket/curated/'
IAM_ROLE ':iam_role'
FORMAT AS PARQUET;

-- MERGE requires source and target to share a matching join key and
-- comparable columns; reshape date+hour into hour_start here so the
-- staging table's shape lines up with fact_port_congestion_hourly.
CREATE TEMP TABLE stg_congestion_hourly (
    port_id             VARCHAR(10),
    hour_start          TIMESTAMP,
    vessel_count        INTEGER,
    avg_dwell_time_hours DECIMAL(10,4),
    queue_length        INTEGER,
    congestion_index    DECIMAL(5,1)
);

INSERT INTO stg_congestion_hourly
SELECT
    port_id,
    (date || ' ' || hour || ':00:00')::TIMESTAMP AS hour_start,
    vessel_count,
    avg_dwell_time_hours,
    queue_length,
    congestion_index
FROM stg_congestion_hourly_raw;

MERGE INTO portpulse.fact_port_congestion_hourly
USING stg_congestion_hourly AS src
ON portpulse.fact_port_congestion_hourly.port_id = src.port_id
   AND portpulse.fact_port_congestion_hourly.hour_start = src.hour_start
WHEN MATCHED THEN UPDATE SET
    vessel_count = src.vessel_count,
    avg_dwell_time_hours = src.avg_dwell_time_hours,
    queue_length = src.queue_length,
    congestion_index = src.congestion_index
WHEN NOT MATCHED THEN INSERT
    (port_id, hour_start, vessel_count, avg_dwell_time_hours, queue_length, congestion_index)
    VALUES (src.port_id, src.hour_start, src.vessel_count, src.avg_dwell_time_hours, src.queue_length, src.congestion_index);

COMMIT;
