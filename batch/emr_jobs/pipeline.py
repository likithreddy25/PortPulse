"""Orchestrates the full enrichment pipeline: read normalized position
records -> geofence -> compute visits/dwell time -> hourly curated
aggregation -> write Parquet. Same entrypoint runs locally (small sample,
local[*] master) or on EMR Serverless (S3 paths, cluster master) --
nothing here is environment-specific except the paths passed in.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from batch.emr_jobs.congestion import compute_hourly_curated
from batch.emr_jobs.dwell import add_geofence_columns, compute_visits


def read_positions(spark: SparkSession, input_path: str, input_format: str = "json") -> DataFrame:
    if input_format == "json":
        return spark.read.json(input_path)
    if input_format == "parquet":
        return spark.read.parquet(input_path)
    raise ValueError(f"unsupported input_format: {input_format!r}")


def run(spark: SparkSession, input_path: str, output_path: str, input_format: str = "json") -> DataFrame:
    positions = read_positions(spark, input_path, input_format)
    geofenced = add_geofence_columns(positions)
    visits = compute_visits(geofenced)
    curated = compute_hourly_curated(geofenced, visits)

    curated.write.mode("overwrite").partitionBy("port_id").parquet(output_path)
    return curated


def _main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="PortPulse EMR enrichment job")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--input-format", choices=["json", "parquet"], default="json")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("portpulse-enrichment").getOrCreate()
    try:
        curated = run(spark, args.input, args.output, args.input_format)
        curated.show(50, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    _main()
