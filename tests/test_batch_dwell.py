from batch.emr_jobs.dwell import add_geofence_columns, compute_visits

# Vessel A: enters USLAX anchorage at 10:00, last seen inside at 12:50,
# clearly outside by 14:00 -> a completed visit, dwell ~2.833h.
# Vessel B: enters at 11:00, last position at 13:00 still inside -> visit
# with no observed exit (still "in zone" as far as this dataset shows).
POSITIONS = [
    {"mmsi": 1001, "lat": 33.71, "lon": -118.21, "speed_knots": 0.2, "timestamp": "2026-01-01T10:00:00+00:00"},
    {"mmsi": 1001, "lat": 33.70, "lon": -118.20, "speed_knots": 0.1, "timestamp": "2026-01-01T11:00:00+00:00"},
    {"mmsi": 1001, "lat": 33.70, "lon": -118.19, "speed_knots": 0.3, "timestamp": "2026-01-01T12:50:00+00:00"},
    {"mmsi": 1001, "lat": 34.50, "lon": -118.20, "speed_knots": 12.0, "timestamp": "2026-01-01T14:00:00+00:00"},
    {"mmsi": 1002, "lat": 33.72, "lon": -118.22, "speed_knots": 5.0, "timestamp": "2026-01-01T11:00:00+00:00"},
    {"mmsi": 1002, "lat": 33.71, "lon": -118.20, "speed_knots": 0.5, "timestamp": "2026-01-01T13:00:00+00:00"},
]


def _geofenced(spark):
    df = spark.createDataFrame(POSITIONS)
    return add_geofence_columns(df)


def test_add_geofence_columns_flags_in_and_out_of_zone(spark):
    geo = _geofenced(spark).orderBy("mmsi", "timestamp").collect()
    zones = [r["anchorage_port"] for r in geo]
    assert zones == ["USLAX", "USLAX", "USLAX", None, "USLAX", "USLAX"]


def test_compute_visits_vessel_a_has_completed_visit(spark):
    geo = _geofenced(spark)
    visits = compute_visits(geo).filter("mmsi = 1001").collect()

    assert len(visits) == 1
    v = visits[0]
    assert v["port_zone"] == "USLAX"
    assert v["entry_time"] == "2026-01-01T10:00:00+00:00"
    assert v["exit_time"] == "2026-01-01T12:50:00+00:00"
    assert v["exit_observed"] is True
    assert abs(v["dwell_hours"] - (2 + 50 / 60)) < 0.01


def test_compute_visits_vessel_b_visit_is_still_open(spark):
    geo = _geofenced(spark)
    visits = compute_visits(geo).filter("mmsi = 1002").collect()

    assert len(visits) == 1
    v = visits[0]
    assert v["exit_observed"] is False
    assert abs(v["dwell_hours"] - 2.0) < 0.01
