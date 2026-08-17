"""Radius-based anchorage geofencing -- the precise layer described in
docs/ARCHITECTURE.md, as opposed to Phase 1's coarse bounding-box check.

A single anchorage radius is used for all 5 ports as a starting
assumption (not derived from real port-authority anchorage boundaries,
which vary a lot by port). ~20km (~10.8 nautical miles) is a reasonable
middle ground for "vessels waiting to call at this port" used informally
in maritime traffic analyses -- refine per-port with real anchorage
boundaries if/when they're available, same caveat as the Phase 1
bounding boxes in ingestion/config.py.
"""

from __future__ import annotations

import math

from ingestion.config import PORTS

ANCHORAGE_RADIUS_KM = 20.0
EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def nearest_port_and_distance(lat: float, lon: float) -> tuple[str, float]:
    """Returns (port_id, distance_km) for the closest of the 5 ports."""
    best_port_id, best_distance = None, float("inf")
    for port in PORTS:
        d = haversine_km(lat, lon, port.center_lat, port.center_lon)
        if d < best_distance:
            best_port_id, best_distance = port.port_id, d
    return best_port_id, best_distance


def in_anchorage(lat: float, lon: float) -> tuple[str | None, float]:
    """Returns (port_id, distance_km) if the position is within
    ANCHORAGE_RADIUS_KM of that port's center, else (None, distance_km)
    to the nearest port anyway (useful for debugging near-misses)."""
    port_id, distance = nearest_port_and_distance(lat, lon)
    if distance <= ANCHORAGE_RADIUS_KM:
        return port_id, distance
    return None, distance
