-- PortPulse Redshift Serverless star schema.
--
-- Redshift-specific syntax used below (DISTSTYLE/DISTKEY/SORTKEY) only
-- validates against a real Redshift/Redshift Serverless endpoint, which
-- hasn't been deployed yet (see README Cost/Build status) -- deploying
-- it is the next AWS-cost gate, same pattern as Phases 1-2. The
-- ANSI-portable parts of this schema (column definitions, PK/FK
-- declarations, data types) were validated against a real local
-- Postgres container as a sanity check that the SQL itself is
-- well-formed; Postgres doesn't understand DISTKEY/SORTKEY, so those
-- clauses are stripped in warehouse/redshift_ddl/postgres_compat.sql
-- (used only for that local validation, not for real deployment).
--
-- IMPORTANT discrepancy found during that validation, worth stating
-- plainly rather than leaving implicit: Redshift does NOT enforce
-- PRIMARY KEY, FOREIGN KEY, or UNIQUE constraints at load/insert time --
-- they're informational only, used by the query planner (Redshift only
-- enforces NOT NULL). Postgres, which was used to validate this schema
-- locally, DOES enforce them -- so a bad port_id or a duplicate
-- (port_id, hour_start) row that Postgres would reject will load into
-- real Redshift silently. That's exactly why load.sql uses a MERGE
-- statement keyed on (port_id, hour_start) to prevent duplicates at the
-- load-script level, rather than relying on the declared PK to do it --
-- because in Redshift, it won't.

CREATE SCHEMA IF NOT EXISTS portpulse;

-- dim_port: real reference data, one row per tracked port, generated
-- directly from ingestion/config.py (see seed_dim_port.sql) so it can't
-- drift from the ports the ingestion pipeline actually tracks.
CREATE TABLE IF NOT EXISTS portpulse.dim_port (
    port_id       VARCHAR(10)   NOT NULL PRIMARY KEY,
    port_name     VARCHAR(100)  NOT NULL,
    center_lat    DECIMAL(9,6)  NOT NULL,
    center_lon    DECIMAL(9,6)  NOT NULL,
    anchorage_radius_km DECIMAL(6,2) NOT NULL
)
DISTSTYLE ALL;  -- tiny (5-row) dimension: replicate to every node rather than distribute

-- dim_vessel_type: seeded from the standardized AIS/ITU-R M.1371 "Type
-- of ship and cargo" classification (grouped at the tens-digit level --
-- 10 categories rather than the full 100 specific codes -- for a
-- manageable reference table). This is real, standardized reference
-- data (like a country-code table), NOT data captured by this
-- pipeline: PortPulse's ingestion currently only requests AISStream's
-- PositionReport message type (see ingestion/aisstream_client.py), not
-- ShipStaticData, which is where AIS actually reports a vessel's type.
-- No fact rows are joined to this table yet -- capturing ShipStaticData
-- and mapping MMSI -> vessel_type_id would be the natural next step to
-- make that connection real instead of just a reference table sitting
-- unused. Flagged here rather than left implicit.
CREATE TABLE IF NOT EXISTS portpulse.dim_vessel_type (
    vessel_type_id   SMALLINT     NOT NULL PRIMARY KEY,  -- tens digit of the AIS ship type code (0-9)
    category_name    VARCHAR(150) NOT NULL,
    code_range       VARCHAR(20)  NOT NULL  -- e.g. '70-79'
)
DISTSTYLE ALL;

-- fact_port_congestion_hourly: the grain is one row per (port, hour).
-- Loaded via COPY from the Phase 3 curated S3 zone (Parquet), which
-- isn't uploaded to S3 yet -- Phase 3 currently writes curated output
-- locally only (see README Build status). load.sql's COPY command is
-- ready for when that S3 path exists.
CREATE TABLE IF NOT EXISTS portpulse.fact_port_congestion_hourly (
    port_id             VARCHAR(10)   NOT NULL REFERENCES portpulse.dim_port(port_id),
    hour_start          TIMESTAMP     NOT NULL,  -- UTC, truncated to the hour (date + hour from the curated table)
    vessel_count        INTEGER       NOT NULL,
    avg_dwell_time_hours DECIMAL(10,4),           -- NULL when no visits completed that hour (see batch/emr_jobs/congestion.py)
    queue_length        INTEGER       NOT NULL,
    congestion_index    DECIMAL(5,1)  NOT NULL,   -- 0-100, see batch/emr_jobs/congestion.py for the formula
    PRIMARY KEY (port_id, hour_start)
)
DISTSTYLE KEY
DISTKEY (port_id)
SORTKEY (port_id, hour_start);
