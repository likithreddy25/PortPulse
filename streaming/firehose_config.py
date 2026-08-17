"""Builds the Kinesis Data Firehose delivery stream config for the S3 raw
zone.

IMPORTANT CAVEAT (documented rather than hidden): AWS's own docs don't
give a clear yes/no on whether a single Firehose delivery stream can
combine record format conversion (JSON -> Parquet, via a Glue table
schema) *and* dynamic partitioning (content-based S3 prefixes via a JQ
query) at the same time. Multiple doc pages were checked and neither
confirms nor rules it out. `build_primary_config()` below builds the
combined config exactly as specified (Parquet + port/date/hour S3
partitioning in one delivery stream); if AWS's CreateDeliveryStream API
rejects that combination on first real deploy, `build_fallback_config()`
is the tested alternative: dynamic partitioning writes raw
gzip-compressed JSON to the same port/date/hour layout, and Parquet
conversion happens downstream in the Phase 3 EMR job instead (which was
already going to write Parquet for the curated zone). Neither config has
been deployed against real AWS yet -- moto's mock accepts both, which
proves the config *shape* is well-formed but not the semantic
restriction question.
"""

from __future__ import annotations

from typing import Any

from streaming.table_schema import DATABASE_NAME, RAW_TABLE_NAME

DELIVERY_STREAM_NAME = "portpulse-ais-raw"

# jq query: extract the fields Firehose's dynamic partitioning uses to
# build the S3 prefix. date/hour are sliced out of the ISO-8601 timestamp
# the same way streaming.table_schema.derive_partition_values() does it
# locally, so the local preview and the real pipeline agree on layout.
METADATA_EXTRACTION_QUERY = (
    '{port_zone: (.port_zone // "UNKNOWN"), '
    "date: (.timestamp[0:10]), "
    "hour: (.timestamp[11:13])}"
)

S3_PREFIX = (
    "raw/port_zone=!{partitionKeyFromQuery:port_zone}/"
    "date=!{partitionKeyFromQuery:date}/"
    "hour=!{partitionKeyFromQuery:hour}/"
)
S3_ERROR_PREFIX = "raw-errors/!{firehose:error-output-type}/"

BUFFERING_HINTS = {"SizeInMBs": 64, "IntervalInSeconds": 300}


def _processing_configuration() -> dict[str, Any]:
    return {
        "Enabled": True,
        "Processors": [
            {
                "Type": "MetadataExtraction",
                "Parameters": [
                    {"ParameterName": "MetadataExtractionQuery", "ParameterValue": METADATA_EXTRACTION_QUERY},
                    {"ParameterName": "JsonParsingEngine", "ParameterValue": "JQ-1.6"},
                ],
            },
        ],
    }


def build_primary_config(
    *, bucket_arn: str, firehose_role_arn: str, kinesis_stream_arn: str, region: str, account_id: str
) -> dict[str, Any]:
    """Parquet conversion + dynamic partitioning in one delivery stream,
    exactly as specified. Unverified against real AWS -- see module docstring."""
    return {
        "DeliveryStreamName": DELIVERY_STREAM_NAME,
        "DeliveryStreamType": "KinesisStreamAsSource",
        "KinesisStreamSourceConfiguration": {
            "KinesisStreamARN": kinesis_stream_arn,
            "RoleARN": firehose_role_arn,
        },
        "ExtendedS3DestinationConfiguration": {
            "RoleARN": firehose_role_arn,
            "BucketARN": bucket_arn,
            "Prefix": S3_PREFIX,
            "ErrorOutputPrefix": S3_ERROR_PREFIX,
            "BufferingHints": BUFFERING_HINTS,
            "DynamicPartitioningConfiguration": {"Enabled": True, "RetryOptions": {"DurationInSeconds": 300}},
            "ProcessingConfiguration": _processing_configuration(),
            "DataFormatConversionConfiguration": {
                "Enabled": True,
                "SchemaConfiguration": {
                    "RoleARN": firehose_role_arn,
                    "DatabaseName": DATABASE_NAME,
                    "TableName": RAW_TABLE_NAME,
                    "Region": region,
                    "CatalogId": account_id,
                    "VersionId": "LATEST",
                },
                "InputFormatConfiguration": {"Deserializer": {"OpenXJsonSerDe": {}}},
                "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {"Compression": "SNAPPY"}}},
            },
        },
    }


def build_fallback_config(
    *, bucket_arn: str, firehose_role_arn: str, kinesis_stream_arn: str
) -> dict[str, Any]:
    """Dynamic partitioning only, writing gzip-compressed raw JSON. Used
    if AWS rejects the primary (Parquet + dynamic partitioning) config.
    Phase 3's EMR job would then be responsible for the JSON -> Parquet
    conversion for anything read out of this raw zone."""
    return {
        "DeliveryStreamName": DELIVERY_STREAM_NAME,
        "DeliveryStreamType": "KinesisStreamAsSource",
        "KinesisStreamSourceConfiguration": {
            "KinesisStreamARN": kinesis_stream_arn,
            "RoleARN": firehose_role_arn,
        },
        "ExtendedS3DestinationConfiguration": {
            "RoleARN": firehose_role_arn,
            "BucketARN": bucket_arn,
            "Prefix": S3_PREFIX,
            "ErrorOutputPrefix": S3_ERROR_PREFIX,
            "BufferingHints": BUFFERING_HINTS,
            "CompressionFormat": "GZIP",
            "DynamicPartitioningConfiguration": {"Enabled": True, "RetryOptions": {"DurationInSeconds": 300}},
            "ProcessingConfiguration": _processing_configuration(),
        },
    }
