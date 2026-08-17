"""Coarse port-zone assignment used at ingestion time.

This just tags each position with the bounding box it falls in, purely so
downstream consumers (Firehose partitioning, dashboards) can filter by
port without re-deriving it. Phase 3's EMR job does the real anchorage
geofencing with radius-based distance math for dwell-time computation --
this is deliberately cheap and approximate.
"""

from typing import Optional

from ingestion.config import PORTS


def assign_port_zone(lat: float, lon: float) -> Optional[str]:
    for port in PORTS:
        if port.contains(lat, lon):
            return port.port_id
    return None
