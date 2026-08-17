"""Hourly curated aggregation per port and the congestion_index formula.

## The congestion_index formula

congestion_index (0-100, higher = more congested) is a weighted sum of
three normalized components:

    congestion_index = 100 * (
        0.3 * min(vessel_count / VESSEL_COUNT_CAP, 1.0) +
        0.4 * min(avg_dwell_time_hours / DWELL_HOURS_CAP, 1.0) +
        0.3 * min(queue_length / QUEUE_LENGTH_CAP, 1.0)
    )

Each component is capped and scaled to [0, 1] against a fixed reference
value (the "cap" -- the point at which that component alone would
represent maximum congestion), then combined with weights:

- **avg_dwell_time_hours gets the highest weight (0.4)** because it's
  the most direct signal of actual delay experienced by ships -- a port
  with few vessels but long waits is genuinely more congested than one
  with many vessels moving through quickly.
- **vessel_count and queue_length each get 0.3.** vessel_count captures
  overall traffic volume; queue_length (vessels observed stationary,
  i.e. actively waiting rather than transiting through the anchorage)
  captures backlog specifically.

The caps (VESSEL_COUNT_CAP=40, DWELL_HOURS_CAP=48, QUEUE_LENGTH_CAP=25)
are **reasonable starting assumptions, not derived from historical data**
-- there isn't enough historical data yet (see README) to calibrate them
empirically. They should be revisited once enough real hourly history
accumulates per port to know what "typical" vs "severe" actually looks
like for each one specifically. Treating this as a documented, adjustable
assumption rather than a black box is the point.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

STATIONARY_SPEED_KNOTS = 1.0

VESSEL_COUNT_CAP = 40.0
DWELL_HOURS_CAP = 48.0
QUEUE_LENGTH_CAP = 25.0

WEIGHT_VESSEL_COUNT = 0.3
WEIGHT_DWELL_TIME = 0.4
WEIGHT_QUEUE_LENGTH = 0.3


def congestion_index_expr(vessel_count_col: str, avg_dwell_col: str, queue_length_col: str):
    """Returns a Spark Column implementing the formula above, usable in
    a .withColumn() call. avg_dwell_col may be null (no completed visits
    that hour) -- treated as 0 contribution, not penalized or rewarded."""
    vessel_component = F.least(F.col(vessel_count_col) / F.lit(VESSEL_COUNT_CAP), F.lit(1.0))
    dwell_component = F.least(F.coalesce(F.col(avg_dwell_col), F.lit(0.0)) / F.lit(DWELL_HOURS_CAP), F.lit(1.0))
    queue_component = F.least(F.col(queue_length_col) / F.lit(QUEUE_LENGTH_CAP), F.lit(1.0))

    return F.round(
        100 * (
            WEIGHT_VESSEL_COUNT * vessel_component
            + WEIGHT_DWELL_TIME * dwell_component
            + WEIGHT_QUEUE_LENGTH * queue_component
        ),
        1,
    )


def compute_hourly_curated(positions: DataFrame, visits: DataFrame) -> DataFrame:
    """positions: geofenced position reports (mmsi, timestamp, anchorage_port,
    speed_knots, ...). visits: output of dwell.compute_visits().

    Produces one row per (port_zone, date, hour):
    vessel_count, avg_dwell_time_hours, queue_length, congestion_index.
    """
    in_zone = positions.filter(F.col("anchorage_port").isNotNull()).withColumn(
        "hour_bucket", F.substring(F.col("timestamp"), 1, 13)  # "2026-08-17T15"
    )

    per_vessel_hour = in_zone.groupBy("anchorage_port", "hour_bucket", "mmsi").agg(
        F.min("speed_knots").alias("min_speed_knots")
    )

    vessel_and_queue = per_vessel_hour.groupBy("anchorage_port", "hour_bucket").agg(
        F.countDistinct("mmsi").alias("vessel_count"),
        F.sum(F.when(F.col("min_speed_knots") < STATIONARY_SPEED_KNOTS, 1).otherwise(0)).alias("queue_length"),
    )

    completed_visits = visits.filter(F.col("exit_observed")).withColumn(
        "hour_bucket", F.substring(F.col("exit_time"), 1, 13)
    )
    dwell_by_hour = completed_visits.groupBy("port_zone", "hour_bucket").agg(
        F.avg("dwell_hours").alias("avg_dwell_time_hours")
    )

    curated = (
        vessel_and_queue.join(
            dwell_by_hour,
            (vessel_and_queue.anchorage_port == dwell_by_hour.port_zone)
            & (vessel_and_queue.hour_bucket == dwell_by_hour.hour_bucket),
            "left",
        )
        .select(
            vessel_and_queue.anchorage_port.alias("port_id"),
            vessel_and_queue.hour_bucket,
            "vessel_count",
            "queue_length",
            "avg_dwell_time_hours",
        )
        .withColumn("date", F.substring(F.col("hour_bucket"), 1, 10))
        .withColumn("hour", F.substring(F.col("hour_bucket"), 12, 2))
        .drop("hour_bucket")
    )

    return curated.withColumn(
        "congestion_index",
        congestion_index_expr("vessel_count", "avg_dwell_time_hours", "queue_length"),
    )
