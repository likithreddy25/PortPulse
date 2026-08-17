"""Generates a SYNTHETIC fixture of raw AISStream-shaped messages for
offline pipeline development and unit tests.

This is NOT real captured AIS traffic -- it's deterministically generated
so the rest of PortPulse (replay, Kinesis producer, and later the Phase 3
geofencing/dwell-time logic) can be built and tested before a real ~10min
capture exists. Once AISSTREAM_API_KEY is set, capture a real sample with:

    python -m ingestion.aisstream_client --sink file \\
        --out ingestion/sample_data/captured_live.jsonl --max-seconds 600

Regenerate this fixture with:
    python -m ingestion.sample_data.generate_synthetic_fixture
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ingestion.config import PORTS

OUT_PATH = Path(__file__).parent / "synthetic_fixture.jsonl"

SHIP_NAME_PREFIXES = ["EVER", "MSC", "MAERSK", "COSCO", "CMA CGM", "OOCL", "HAPAG", "ONE"]
SHIP_NAME_SUFFIXES = ["GIVEN", "HORIZON", "PIONEER", "VOYAGER", "TRADER", "SPIRIT", "STAR", "GLORY"]


def _random_ship_name(rng: random.Random) -> str:
    return f"{rng.choice(SHIP_NAME_PREFIXES)} {rng.choice(SHIP_NAME_SUFFIXES)}"


def _fmt_time(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f") + " +0000 UTC"


def generate(seed: int = 42, vessels_per_port: int = 4, points_per_vessel: int = 12) -> list[dict]:
    rng = random.Random(seed)
    messages: list[dict] = []
    base_time = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

    for port in PORTS:
        for v in range(vessels_per_port):
            mmsi = 200000000 + hash((port.port_id, v)) % 99999999
            ship_name = _random_ship_name(rng)

            # Vessel enters near the edge of the box, drifts toward the
            # center (anchorage-ish), dwells a bit, then some exit again --
            # gives Phase 3's dwell-time logic something non-trivial to chew on.
            (lat_min, lon_min), (lat_max, lon_max) = port.bbox
            start_lat = rng.uniform(lat_min, lat_max)
            start_lon = rng.uniform(lon_min, lon_max)
            lat, lon = start_lat, start_lon

            will_exit = v % 2 == 0
            t = base_time + timedelta(minutes=rng.randint(0, 30))

            for i in range(points_per_vessel):
                # small random walk, biased toward the port center while "dwelling"
                drift_lat = (port.center_lat - lat) * 0.05 + rng.uniform(-0.01, 0.01)
                drift_lon = (port.center_lon - lon) * 0.05 + rng.uniform(-0.01, 0.01)
                lat += drift_lat
                lon += drift_lon

                if will_exit and i >= points_per_vessel - 3:
                    # peel off toward open water in the last few points
                    lat += 0.15
                    lon += 0.15

                speed = 0.2 if not (will_exit and i >= points_per_vessel - 3) else rng.uniform(8, 14)
                course = rng.uniform(0, 359)
                t += timedelta(minutes=rng.randint(3, 8))

                messages.append({
                    "MessageType": "PositionReport",
                    "MetaData": {
                        "MMSI": mmsi,
                        "ShipName": ship_name,
                        "latitude": round(lat, 6),
                        "longitude": round(lon, 6),
                        "time_utc": _fmt_time(t),
                    },
                    "Message": {
                        "PositionReport": {
                            "MessageID": 1,
                            "UserID": mmsi,
                            "Latitude": round(lat, 6),
                            "Longitude": round(lon, 6),
                            "Sog": round(speed, 1),
                            "Cog": round(course, 1),
                            "TrueHeading": int(course),
                            "NavigationalStatus": 1 if speed < 1 else 0,
                        }
                    },
                })

    # Interleave by timestamp so it reads like a real multi-vessel stream.
    messages.sort(key=lambda m: m["MetaData"]["time_utc"])
    return messages


def main() -> None:
    messages = generate()
    with open(OUT_PATH, "w") as fh:
        for m in messages:
            fh.write(json.dumps(m) + "\n")
    print(f"Wrote {len(messages)} synthetic messages to {OUT_PATH}")


if __name__ == "__main__":
    main()
