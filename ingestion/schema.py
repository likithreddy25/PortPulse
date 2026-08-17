"""Normalized vessel-position record and the raw-AISStream-message -> record mapping."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from ingestion.geofence import assign_port_zone

# AISStream's time_utc looks like "2022-12-29 18:22:32.318353 +0000 UTC"
_AISSTREAM_TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f %z UTC",
    "%Y-%m-%d %H:%M:%S %z UTC",
)


@dataclass
class VesselPosition:
    mmsi: int
    lat: float
    lon: float
    speed_knots: float
    course_deg: float
    timestamp: str  # ISO-8601 UTC
    port_zone: Optional[str]
    ship_name: Optional[str] = None
    nav_status: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_aisstream_time(raw: str) -> str:
    for fmt in _AISSTREAM_TIME_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            continue
    # Fall back to "now" rather than dropping the message if AISStream ever
    # changes its timestamp formatting.
    return datetime.now(timezone.utc).isoformat()


def normalize_message(raw: dict[str, Any]) -> Optional[VesselPosition]:
    """Map a raw AISStream PositionReport message to our normalized schema.

    Returns None for message types we don't handle (e.g. ShipStaticData)
    or malformed payloads, so callers can just skip Nones.
    """
    if raw.get("MessageType") != "PositionReport":
        return None

    meta = raw.get("MetaData") or {}
    report = (raw.get("Message") or {}).get("PositionReport") or {}

    try:
        mmsi = int(meta["MMSI"])
        lat = float(meta["latitude"])
        lon = float(meta["longitude"])
    except (KeyError, TypeError, ValueError):
        return None

    speed = float(report.get("Sog", 0.0) or 0.0)
    course = float(report.get("Cog", 0.0) or 0.0)
    timestamp = _parse_aisstream_time(meta.get("time_utc", ""))
    port_zone = assign_port_zone(lat, lon)

    return VesselPosition(
        mmsi=mmsi,
        lat=lat,
        lon=lon,
        speed_knots=speed,
        course_deg=course,
        timestamp=timestamp,
        port_zone=port_zone,
        ship_name=meta.get("ShipName"),
        nav_status=report.get("NavigationalStatus"),
    )
