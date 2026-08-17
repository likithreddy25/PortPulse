-- Postgres-compatible stripped copy of schema.sql, used ONLY to
-- validate the ANSI-portable parts of the schema locally (Postgres
-- doesn't understand Redshift's DISTSTYLE/DISTKEY/SORTKEY). NOT used
-- for real deployment -- schema.sql is. Keep in sync by hand; this file
-- is small (3 tables) and changes rarely.

CREATE SCHEMA IF NOT EXISTS portpulse;

CREATE TABLE IF NOT EXISTS portpulse.dim_port (
    port_id       VARCHAR(10)   NOT NULL PRIMARY KEY,
    port_name     VARCHAR(100)  NOT NULL,
    center_lat    DECIMAL(9,6)  NOT NULL,
    center_lon    DECIMAL(9,6)  NOT NULL,
    anchorage_radius_km DECIMAL(6,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS portpulse.dim_vessel_type (
    vessel_type_id   SMALLINT     NOT NULL PRIMARY KEY,
    category_name    VARCHAR(150) NOT NULL,
    code_range       VARCHAR(20)  NOT NULL
);

CREATE TABLE IF NOT EXISTS portpulse.fact_port_congestion_hourly (
    port_id             VARCHAR(10)   NOT NULL REFERENCES portpulse.dim_port(port_id),
    hour_start          TIMESTAMP     NOT NULL,
    vessel_count        INTEGER       NOT NULL,
    avg_dwell_time_hours DECIMAL(10,4),
    queue_length        INTEGER       NOT NULL,
    congestion_index    DECIMAL(5,1)  NOT NULL,
    PRIMARY KEY (port_id, hour_start)
);
