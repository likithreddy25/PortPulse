import json

from batch.emr_jobs.pipeline import run

POSITIONS = [
    {"mmsi": 1001, "lat": 33.71, "lon": -118.21, "speed_knots": 0.2, "course_deg": 10.0,
     "timestamp": "2026-01-01T10:00:00+00:00", "port_zone": "USLAX", "ship_name": "A", "nav_status": 1},
    {"mmsi": 1001, "lat": 34.50, "lon": -118.20, "speed_knots": 12.0, "course_deg": 90.0,
     "timestamp": "2026-01-01T14:00:00+00:00", "port_zone": None, "ship_name": "A", "nav_status": 0},
]


def test_full_pipeline_writes_partitioned_curated_parquet(spark, tmp_path):
    input_path = tmp_path / "input.jsonl"
    with open(input_path, "w") as fh:
        for row in POSITIONS:
            fh.write(json.dumps(row) + "\n")

    output_path = tmp_path / "curated"
    curated = run(spark, str(input_path), str(output_path), input_format="json")

    rows = curated.collect()
    assert len(rows) == 1
    assert rows[0]["port_id"] == "USLAX"
    assert rows[0]["vessel_count"] == 1

    # Written output is readable back and partitioned by port_id.
    written = spark.read.parquet(str(output_path))
    assert written.count() == 1
    assert (output_path / "port_id=USLAX").exists()
