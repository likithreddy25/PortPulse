from ingestion.schema import normalize_message

VALID_RAW = {
    "MessageType": "PositionReport",
    "MetaData": {
        "MMSI": 259000420,
        "ShipName": "AUGUSTSON",
        "latitude": 33.7,
        "longitude": -118.2,
        "time_utc": "2022-12-29 18:22:32.318353 +0000 UTC",
    },
    "Message": {
        "PositionReport": {
            "MessageID": 1,
            "UserID": 259000420,
            "Latitude": 33.7,
            "Longitude": -118.2,
            "Sog": 5.2,
            "Cog": 308,
            "TrueHeading": 235,
            "NavigationalStatus": 0,
        }
    },
}


def test_normalize_valid_position_report():
    record = normalize_message(VALID_RAW)
    assert record is not None
    assert record.mmsi == 259000420
    assert record.lat == 33.7
    assert record.lon == -118.2
    assert record.speed_knots == 5.2
    assert record.course_deg == 308
    assert record.port_zone == "USLAX"
    assert record.ship_name == "AUGUSTSON"
    assert record.timestamp.startswith("2022-12-29T18:22:32")


def test_non_position_report_message_types_are_ignored():
    raw = {**VALID_RAW, "MessageType": "ShipStaticData"}
    assert normalize_message(raw) is None


def test_missing_mmsi_returns_none():
    raw = {
        "MessageType": "PositionReport",
        "MetaData": {"latitude": 1.0, "longitude": 2.0, "time_utc": "bad"},
        "Message": {"PositionReport": {}},
    }
    assert normalize_message(raw) is None


def test_position_outside_all_ports_has_null_zone_but_still_normalizes():
    raw = {**VALID_RAW}
    raw["MetaData"] = {**raw["MetaData"], "latitude": 0.0, "longitude": 0.0}
    record = normalize_message(raw)
    assert record is not None
    assert record.port_zone is None


def test_to_dict_round_trips_all_fields():
    record = normalize_message(VALID_RAW)
    d = record.to_dict()
    assert set(d.keys()) == {
        "mmsi", "lat", "lon", "speed_knots", "course_deg",
        "timestamp", "port_zone", "ship_name", "nav_status",
    }
