import json

import pyarrow.parquet as pq

from streaming.local_preview import read_records, write_partitioned_parquet

NORMALIZED_RECORD = {
    "mmsi": 244690454, "lat": 51.88755, "lon": 4.40572, "speed_knots": 0.0,
    "course_deg": 0.0, "timestamp": "2026-08-17T15:02:45.321601+00:00",
    "port_zone": "NLRTM", "ship_name": "REGGEPLUS", "nav_status": 0,
}

RAW_MESSAGE = {
    "MessageType": "PositionReport",
    "MetaData": {
        "MMSI": 259000420, "ShipName": "AUGUSTSON", "latitude": 33.7,
        "longitude": -118.2, "time_utc": "2026-08-17 14:22:32.318353 +0000 UTC",
    },
    "Message": {"PositionReport": {"Sog": 5.2, "Cog": 308, "TrueHeading": 235, "NavigationalStatus": 0}},
}


def test_read_records_normalizes_raw_messages(tmp_path):
    path = tmp_path / "sample.jsonl"
    path.write_text(json.dumps(RAW_MESSAGE) + "\n")
    records = list(read_records(str(path)))
    assert len(records) == 1
    assert records[0]["mmsi"] == 259000420
    assert records[0]["port_zone"] == "USLAX"


def test_read_records_passes_through_already_normalized(tmp_path):
    path = tmp_path / "sample.jsonl"
    path.write_text(json.dumps(NORMALIZED_RECORD) + "\n")
    records = list(read_records(str(path)))
    assert records == [NORMALIZED_RECORD]


def test_write_partitioned_parquet_groups_by_port_date_hour(tmp_path):
    records = [
        NORMALIZED_RECORD,
        {**NORMALIZED_RECORD, "mmsi": 111, "timestamp": "2026-08-17T15:45:00+00:00"},  # same partition
        {**NORMALIZED_RECORD, "mmsi": 222, "port_zone": "USLAX", "timestamp": "2026-08-17T16:00:00+00:00"},
    ]
    out_dir = tmp_path / "raw"
    counts = write_partitioned_parquet(iter(records), str(out_dir))

    assert counts == {
        "port_zone=NLRTM/date=2026-08-17/hour=15": 2,
        "port_zone=USLAX/date=2026-08-17/hour=16": 1,
    }

    table = pq.read_table(out_dir / "port_zone=NLRTM" / "date=2026-08-17" / "hour=15")
    assert table.num_rows == 2
    assert set(table.column("mmsi").to_pylist()) == {244690454, 111}


def test_write_partitioned_parquet_schema_matches_table_schema(tmp_path):
    from streaming.table_schema import pyarrow_schema

    out_dir = tmp_path / "raw"
    write_partitioned_parquet(iter([NORMALIZED_RECORD]), str(out_dir))
    table = pq.read_table(out_dir / "port_zone=NLRTM" / "date=2026-08-17" / "hour=15")
    assert table.schema.equals(pyarrow_schema())
