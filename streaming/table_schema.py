"""Single source of truth for the raw vessel-position table schema.

Used by: the local Parquet preview (pyarrow), the Glue table/crawler
config, and the Firehose format-conversion config (Glue-backed schema).
Column names/types here must match `ingestion.schema.VesselPosition`.
"""

from __future__ import annotations

# (name, glue_type, pyarrow_type_str) -- kept as plain tuples rather than
# a heavier schema library since this only needs to feed two consumers.
COLUMNS = [
    ("mmsi", "bigint", "int64"),
    ("lat", "double", "float64"),
    ("lon", "double", "float64"),
    ("speed_knots", "double", "float64"),
    ("course_deg", "double", "float64"),
    # Kept as string (ISO-8601 with UTC offset) rather than Glue `timestamp`
    # because Hive's timestamp type doesn't reliably parse the offset
    # suffix; Phase 3's EMR job does the real timestamp parsing/typing.
    ("timestamp", "string", "string"),
    ("port_zone", "string", "string"),
    ("ship_name", "string", "string"),
    ("nav_status", "int", "int32"),
]

# Physical S3 partitioning: port_zone/date/hour, per the project spec.
# port_zone is intentionally kept in `COLUMNS` above too, even though it's
# also a partition key -- Firehose's dynamic partitioning doesn't strip
# extracted fields from the record body, and duplicating it as a regular
# column costs a few bytes but makes ad-hoc querying of a single Parquet
# file (without going through the partitioned table) still work.
PARTITION_COLUMNS = [
    ("port_zone", "string"),
    ("date", "string"),   # YYYY-MM-DD, derived from `timestamp`
    ("hour", "string"),   # HH, derived from `timestamp`
]

DATABASE_NAME = "portpulse"
RAW_TABLE_NAME = "ais_positions_raw"


def glue_columns() -> list[dict]:
    return [{"Name": name, "Type": glue_type} for name, glue_type, _ in COLUMNS]


def glue_partition_keys() -> list[dict]:
    return [{"Name": name, "Type": glue_type} for name, glue_type in PARTITION_COLUMNS]


def pyarrow_schema():
    import pyarrow as pa

    type_map = {
        "int64": pa.int64(),
        "float64": pa.float64(),
        "int32": pa.int32(),
        "string": pa.string(),
    }
    fields = [pa.field(name, type_map[pa_type]) for name, _, pa_type in COLUMNS]
    return pa.schema(fields)


def derive_partition_values(record: dict) -> tuple[str, str, str]:
    """Given a normalized record (as produced by ingestion.schema.normalize_message),
    returns (port_zone, date, hour) for partitioning. Records with no
    port_zone are bucketed under 'UNKNOWN' rather than dropped."""
    port_zone = record.get("port_zone") or "UNKNOWN"
    timestamp = record["timestamp"]  # e.g. "2026-08-17T14:32:43.137643+00:00"
    date = timestamp[:10]
    hour = timestamp[11:13]
    return port_zone, date, hour
