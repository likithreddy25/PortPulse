"""Spark transformations: geofence each position, then group each
vessel's consecutive in-zone position reports into "visits" (zone-entry
to zone-exit) using a standard gaps-and-islands technique, and compute
dwell time per visit.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from batch.emr_jobs.geofence import in_anchorage

_geofence_schema = StructType([
    StructField("anchorage_port", StringType(), True),
    StructField("distance_km", DoubleType(), False),
])


@F.udf(returnType=_geofence_schema)
def _geofence_udf(lat: float, lon: float):
    return in_anchorage(lat, lon)


def add_geofence_columns(df: DataFrame) -> DataFrame:
    """Adds anchorage_port (null if not within ANCHORAGE_RADIUS_KM of any
    port) and distance_km (to the nearest port regardless)."""
    geo = _geofence_udf(F.col("lat"), F.col("lon"))
    return df.withColumn("anchorage_port", geo["anchorage_port"]).withColumn("distance_km", geo["distance_km"])


def compute_visits(df: DataFrame) -> DataFrame:
    """Input: geofenced position reports (must have mmsi, timestamp,
    anchorage_port). Output: one row per (mmsi, port) visit with
    entry_time, exit_time, dwell_hours, and exit_observed.

    exit_observed=False means the vessel was still in the zone at the
    last position report we have for it in this dataset -- the visit
    hasn't actually ended yet as far as we know, so dwell_hours for it
    is a lower bound, not a completed duration. These are excluded from
    avg_dwell_time_hours in the hourly aggregation (see congestion.py)
    since including them would understate real dwell times.
    """
    w_by_mmsi_time = Window.partitionBy("mmsi").orderBy("timestamp")

    with_prev = df.withColumn("prev_port", F.lag("anchorage_port").over(w_by_mmsi_time))
    with_change = with_prev.withColumn(
        "zone_changed",
        F.when(F.col("anchorage_port") != F.col("prev_port"), 1)
         .when(F.col("anchorage_port").isNull() != F.col("prev_port").isNull(), 1)
         .otherwise(0),
    )
    with_group = with_change.withColumn(
        "visit_id",
        F.sum("zone_changed").over(w_by_mmsi_time.rowsBetween(Window.unboundedPreceding, 0)),
    )

    in_zone = with_group.filter(F.col("anchorage_port").isNotNull())

    vessel_max_ts = df.groupBy("mmsi").agg(F.max("timestamp").alias("vessel_max_ts"))

    visits = (
        in_zone.groupBy("mmsi", "anchorage_port", "visit_id")
        .agg(
            F.min("timestamp").alias("entry_time"),
            F.max("timestamp").alias("exit_time"),
            F.count("*").alias("num_positions"),
        )
        .withColumnRenamed("anchorage_port", "port_zone")
        .join(vessel_max_ts, "mmsi")
        .withColumn(
            "dwell_hours",
            (F.col("exit_time").cast("timestamp").cast("double")
             - F.col("entry_time").cast("timestamp").cast("double")) / 3600.0,
        )
        .withColumn("exit_observed", F.col("exit_time") < F.col("vessel_max_ts"))
        .drop("vessel_max_ts", "visit_id")
    )
    return visits
