"""Port definitions and shared ingestion config.

Bounding boxes are approximate rectangles around each port's anchorage /
approach area, derived from the center points given in the project brief
with a ~0.35 lat / ~0.45 lon degree buffer (roughly 40-50km). They are
intentionally generous at this stage: AISStream uses them to filter which
vessel positions get streamed to us at all, and Phase 3's geofencing logic
narrows further within them using radius-based distance math. If real
port-authority anchorage boundaries are found later, swap the values here
without touching any downstream code.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Port:
    port_id: str  # short UN/LOCODE-style identifier
    name: str
    center_lat: float
    center_lon: float
    lat_buffer: float = 0.35
    lon_buffer: float = 0.45

    @property
    def bbox(self) -> tuple[tuple[float, float], tuple[float, float]]:
        """((lat_min, lon_min), (lat_max, lon_max))"""
        return (
            (self.center_lat - self.lat_buffer, self.center_lon - self.lon_buffer),
            (self.center_lat + self.lat_buffer, self.center_lon + self.lon_buffer),
        )

    def contains(self, lat: float, lon: float) -> bool:
        (lat_min, lon_min), (lat_max, lon_max) = self.bbox
        return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


PORTS: list[Port] = [
    Port("USLAX", "Los Angeles / Long Beach, USA", 33.7, -118.2),
    Port("CNSHA", "Shanghai, China", 30.6, 122.0),
    Port("SGSIN", "Singapore", 1.26, 103.8),
    Port("NLRTM", "Rotterdam, Netherlands", 51.9, 4.0),
    Port("USNYC", "New York / New Jersey, USA", 40.6, -74.0),
]

PORTS_BY_ID: dict[str, Port] = {p.port_id: p for p in PORTS}


def aisstream_bounding_boxes() -> list[list[list[float]]]:
    """BoundingBoxes payload for the AISStream subscription request:
    one [[lat_min, lon_min], [lat_max, lon_max]] box per port."""
    boxes = []
    for p in PORTS:
        (lat_min, lon_min), (lat_max, lon_max) = p.bbox
        boxes.append([[lat_min, lon_min], [lat_max, lon_max]])
    return boxes


AISSTREAM_WS_URL = "wss://stream.aisstream.io/v0/stream"
KINESIS_STREAM_NAME_DEFAULT = "portpulse-ais-positions"
AWS_REGION_DEFAULT = "us-east-1"
