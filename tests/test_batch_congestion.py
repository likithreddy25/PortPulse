from batch.emr_jobs.congestion import (
    DWELL_HOURS_CAP,
    QUEUE_LENGTH_CAP,
    VESSEL_COUNT_CAP,
    compute_hourly_curated,
)
from batch.emr_jobs.dwell import add_geofence_columns, compute_visits

POSITIONS = [
    {"mmsi": 1001, "lat": 33.71, "lon": -118.21, "speed_knots": 0.2, "timestamp": "2026-01-01T10:00:00+00:00"},
    {"mmsi": 1001, "lat": 33.70, "lon": -118.20, "speed_knots": 0.1, "timestamp": "2026-01-01T11:00:00+00:00"},
    {"mmsi": 1001, "lat": 33.70, "lon": -118.19, "speed_knots": 0.3, "timestamp": "2026-01-01T12:50:00+00:00"},
    {"mmsi": 1001, "lat": 34.50, "lon": -118.20, "speed_knots": 12.0, "timestamp": "2026-01-01T14:00:00+00:00"},
    {"mmsi": 1002, "lat": 33.72, "lon": -118.22, "speed_knots": 5.0, "timestamp": "2026-01-01T11:00:00+00:00"},
    {"mmsi": 1002, "lat": 33.71, "lon": -118.20, "speed_knots": 0.5, "timestamp": "2026-01-01T13:00:00+00:00"},
]


def _curated(spark):
    df = spark.createDataFrame(POSITIONS)
    geo = add_geofence_columns(df)
    visits = compute_visits(geo)
    return compute_hourly_curated(geo, visits).orderBy("date", "hour").collect()


def test_vessel_count_and_queue_length_per_hour(spark):
    rows = {(r["date"], r["hour"]): r for r in _curated(spark)}

    # hour 10: only vessel A, queued (speed 0.2)
    r10 = rows[("2026-01-01", "10")]
    assert r10["vessel_count"] == 1
    assert r10["queue_length"] == 1

    # hour 11: A (queued, speed 0.1) and B (moving, speed 5.0)
    r11 = rows[("2026-01-01", "11")]
    assert r11["vessel_count"] == 2
    assert r11["queue_length"] == 1

    # hour 13: only B, queued (speed 0.5)
    r13 = rows[("2026-01-01", "13")]
    assert r13["vessel_count"] == 1
    assert r13["queue_length"] == 1


def test_avg_dwell_time_assigned_to_exit_hour_only_for_completed_visits(spark):
    rows = {(r["date"], r["hour"]): r for r in _curated(spark)}

    # Vessel A's visit completes at 12:50 -> dwell shows up in hour 12.
    r12 = rows[("2026-01-01", "12")]
    assert r12["avg_dwell_time_hours"] is not None
    assert abs(r12["avg_dwell_time_hours"] - (2 + 50 / 60)) < 0.01

    # Vessel B's visit never completes in this dataset -> no hour gets
    # a dwell time contribution from it.
    r13 = rows[("2026-01-01", "13")]
    assert r13["avg_dwell_time_hours"] is None

    r10 = rows[("2026-01-01", "10")]
    assert r10["avg_dwell_time_hours"] is None


def test_congestion_index_matches_documented_formula_by_hand(spark):
    rows = {(r["date"], r["hour"]): r for r in _curated(spark)}
    r12 = rows[("2026-01-01", "12")]

    vessel_component = min(r12["vessel_count"] / VESSEL_COUNT_CAP, 1.0)
    dwell_component = min(r12["avg_dwell_time_hours"] / DWELL_HOURS_CAP, 1.0)
    queue_component = min(r12["queue_length"] / QUEUE_LENGTH_CAP, 1.0)
    expected = round(100 * (0.3 * vessel_component + 0.4 * dwell_component + 0.3 * queue_component), 1)

    assert r12["congestion_index"] == expected


def test_congestion_index_is_zero_to_low_when_no_activity_component_present(spark):
    rows = {(r["date"], r["hour"]): r for r in _curated(spark)}
    r10 = rows[("2026-01-01", "10")]
    # vessel_count=1, queue_length=1, avg_dwell=None(->0 contribution).
    # Compared with a tolerance rather than exact float equality: Python's
    # and Spark's ROUND() disagree at exact .5-cent boundaries (1.95 -> 1.9
    # vs 2.0) due to binary float representation, not a pipeline bug --
    # test_congestion_index_matches_documented_formula_by_hand already
    # verifies exact formula agreement using Spark's own rounded output.
    expected_unrounded = 100 * (0.3 * (1 / VESSEL_COUNT_CAP) + 0.3 * (1 / QUEUE_LENGTH_CAP))
    assert abs(r10["congestion_index"] - expected_unrounded) < 0.1
