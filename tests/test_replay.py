import json

from ingestion.replay import read_raw_messages, replay
from ingestion.sinks import JsonlFileSink


def test_replay_writes_normalized_records_from_sample(tmp_path):
    sample_path = tmp_path / "sample.jsonl"
    raw = {
        "MessageType": "PositionReport",
        "MetaData": {
            "MMSI": 111222333,
            "ShipName": "TEST SHIP",
            "latitude": 33.7,
            "longitude": -118.2,
            "time_utc": "2026-01-15 00:00:00.000000 +0000 UTC",
        },
        "Message": {
            "PositionReport": {
                "Sog": 1.0, "Cog": 90, "TrueHeading": 90, "NavigationalStatus": 1,
                "Latitude": 33.7, "Longitude": -118.2,
            }
        },
    }
    with open(sample_path, "w") as fh:
        fh.write(json.dumps(raw) + "\n")
        fh.write(json.dumps({**raw, "MessageType": "ShipStaticData"}) + "\n")  # should be skipped

    out_path = tmp_path / "out.jsonl"
    sink = JsonlFileSink(str(out_path))
    written = replay(str(sample_path), sink)

    assert written == 1
    lines = out_path.read_text().strip().split("\n")
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["mmsi"] == 111222333
    assert record["port_zone"] == "USLAX"


def test_read_raw_messages_skips_blank_lines(tmp_path):
    path = tmp_path / "sample.jsonl"
    path.write_text('{"a": 1}\n\n{"a": 2}\n')
    messages = list(read_raw_messages(str(path)))
    assert messages == [{"a": 1}, {"a": 2}]


def test_replay_against_synthetic_fixture_produces_records_for_all_ports():
    from pathlib import Path

    fixture = Path("ingestion/sample_data/synthetic_fixture.jsonl")
    if not fixture.exists():
        import pytest
        pytest.skip("synthetic fixture not generated; run generate_synthetic_fixture.py first")

    seen_zones = set()
    for raw in read_raw_messages(str(fixture)):
        from ingestion.schema import normalize_message
        record = normalize_message(raw)
        if record and record.port_zone:
            seen_zones.add(record.port_zone)

    assert seen_zones == {"USLAX", "CNSHA", "SGSIN", "NLRTM", "USNYC"}
