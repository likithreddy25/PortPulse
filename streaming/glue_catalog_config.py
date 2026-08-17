"""Glue Data Catalog config: database, raw table definition (used both by
the crawler and by Firehose's format-conversion schema reference), and
the crawler that keeps the table's partitions up to date.
"""

from __future__ import annotations

from typing import Any

from streaming.table_schema import DATABASE_NAME, RAW_TABLE_NAME, glue_columns, glue_partition_keys

CRAWLER_NAME = "portpulse-raw-crawler"


def database_input() -> dict[str, Any]:
    return {"Name": DATABASE_NAME, "Description": "PortPulse port congestion analytics"}


def raw_table_input(*, s3_location: str) -> dict[str, Any]:
    """Table definition for ais_positions_raw. Firehose's
    DataFormatConversionConfiguration.SchemaConfiguration references this
    exact database/table to know how to serialize records to Parquet, so
    schema drift here directly breaks ingestion -- this is the single
    place both the crawler and Firehose read from (via table_schema.py)."""
    return {
        "Name": RAW_TABLE_NAME,
        "StorageDescriptor": {
            "Columns": glue_columns(),
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
        },
        "PartitionKeys": glue_partition_keys(),
        "TableType": "EXTERNAL_TABLE",
    }


def crawler_config(*, role_arn: str, s3_target_path: str, schedule_cron: str | None = None) -> dict[str, Any]:
    """schedule_cron: e.g. 'cron(5 * * * ? *)' for hourly, 5 min past the
    hour. None means on-demand only -- keep it that way until Phase 7
    (EventBridge orchestration) is actually built, so a stray crawler
    doesn't accrue cost on a schedule nobody's watching yet."""
    config: dict[str, Any] = {
        "Name": CRAWLER_NAME,
        "Role": role_arn,
        "DatabaseName": DATABASE_NAME,
        "Targets": {"S3Targets": [{"Path": s3_target_path}]},
        # New partitions only add/update the table; never drop columns
        # or the table itself out from under Firehose's schema reference.
        "SchemaChangePolicy": {"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "LOG"},
    }
    if schedule_cron:
        config["Schedule"] = schedule_cron
    return config
