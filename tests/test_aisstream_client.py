import json

import pytest

from ingestion import aisstream_client as client_module
from ingestion.aisstream_client import AISStreamClient
from ingestion.sinks import JsonlFileSink

POSITION_REPORT = {
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
            "Sog": 5.2, "Cog": 308, "TrueHeading": 235, "NavigationalStatus": 0,
            "Latitude": 33.7, "Longitude": -118.2,
        }
    },
}
SHIP_STATIC_DATA = {"MessageType": "ShipStaticData", "MetaData": {}, "Message": {}}


class FakeWebSocket:
    def __init__(self, messages):
        self._messages = [json.dumps(m) for m in messages]
        self.sent = []

    async def send(self, payload):
        self.sent.append(payload)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_write_raw_captures_raw_position_reports_and_skips_others(monkeypatch, tmp_path):
    fake_ws = FakeWebSocket([POSITION_REPORT, SHIP_STATIC_DATA])
    monkeypatch.setattr(client_module.websockets, "connect", lambda url: fake_ws)

    out_path = tmp_path / "captured.jsonl"
    sink = JsonlFileSink(str(out_path))
    c = AISStreamClient(api_key="fake", sink=sink, write_raw=True)
    await c.run()

    lines = out_path.read_text().strip().split("\n")
    assert len(lines) == 1
    assert json.loads(lines[0]) == POSITION_REPORT


@pytest.mark.asyncio
async def test_default_mode_writes_normalized_records(monkeypatch, tmp_path):
    fake_ws = FakeWebSocket([POSITION_REPORT])
    monkeypatch.setattr(client_module.websockets, "connect", lambda url: fake_ws)

    out_path = tmp_path / "captured.jsonl"
    sink = JsonlFileSink(str(out_path))
    c = AISStreamClient(api_key="fake", sink=sink, write_raw=False)
    await c.run()

    record = json.loads(out_path.read_text().strip())
    assert record["mmsi"] == 259000420
    assert record["port_zone"] == "USLAX"


@pytest.mark.asyncio
async def test_raw_capture_can_be_replayed(monkeypatch, tmp_path):
    """The whole point of --raw: what gets captured must be replayable
    through the same normalize step the live client itself uses."""
    from ingestion.replay import replay
    from ingestion.sinks import StdoutSink

    fake_ws = FakeWebSocket([POSITION_REPORT])
    monkeypatch.setattr(client_module.websockets, "connect", lambda url: fake_ws)

    captured_path = tmp_path / "captured.jsonl"
    sink = JsonlFileSink(str(captured_path))
    c = AISStreamClient(api_key="fake", sink=sink, write_raw=True)
    await c.run()

    written = replay(str(captured_path), StdoutSink())
    assert written == 1
