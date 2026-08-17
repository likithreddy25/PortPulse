"""Live WebSocket client for AISStream.io.

Subscribes to the 5 port bounding boxes, normalizes each PositionReport,
and writes it to whatever sink is configured. Requires AISSTREAM_API_KEY
in the environment (see .env.example) -- never hardcode the key.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Optional

import websockets

from ingestion.config import AISSTREAM_WS_URL, aisstream_bounding_boxes
from ingestion.schema import normalize_message
from ingestion.sinks import Sink

logger = logging.getLogger(__name__)


class AISStreamClient:
    def __init__(self, api_key: str, sink: Sink, max_messages: Optional[int] = None,
                 max_seconds: Optional[float] = None, write_raw: bool = False):
        self.api_key = api_key
        self.sink = sink
        self.max_messages = max_messages
        self.max_seconds = max_seconds
        # When True, writes AISStream's raw JSON to the sink instead of the
        # normalized record. Used for --sink file captures that feed
        # replay.py, which itself calls normalize_message() on read -- so
        # the recorded sample needs to be raw, the same as what the live
        # feed actually sends. Live production ingestion (sink=kinesis)
        # should leave this False so Kinesis/Firehose/S3 downstream get
        # the normalized schema directly.
        self.write_raw = write_raw
        self._message_count = 0

    def _subscribe_payload(self) -> str:
        return json.dumps({
            "APIKey": self.api_key,
            "BoundingBoxes": aisstream_bounding_boxes(),
            "FilterMessageTypes": ["PositionReport"],
        })

    async def run(self) -> None:
        start = time.monotonic()
        async with websockets.connect(AISSTREAM_WS_URL) as ws:
            await ws.send(self._subscribe_payload())
            logger.info("Subscribed to AISStream for %d ports", len(aisstream_bounding_boxes()))

            async for raw_message in ws:
                if self.max_seconds is not None and (time.monotonic() - start) >= self.max_seconds:
                    logger.info("max_seconds reached, stopping")
                    break

                try:
                    raw = json.loads(raw_message)
                except json.JSONDecodeError:
                    logger.warning("Skipping non-JSON message")
                    continue

                if self.write_raw:
                    if raw.get("MessageType") != "PositionReport":
                        continue
                    self.sink.write(raw)
                else:
                    record = normalize_message(raw)
                    if record is None:
                        continue
                    self.sink.write(record.to_dict())

                self._message_count += 1

                if self.max_messages is not None and self._message_count >= self.max_messages:
                    logger.info("max_messages reached, stopping")
                    break

        self.sink.close()


def load_api_key() -> str:
    """Loads AISSTREAM_API_KEY from the environment / .env file. Raises
    with a clear message if it's missing rather than silently proceeding
    with an empty key."""
    from dotenv import load_dotenv

    load_dotenv()
    key = os.environ.get("AISSTREAM_API_KEY")
    if not key:
        raise RuntimeError(
            "AISSTREAM_API_KEY is not set. Copy .env.example to .env and add your "
            "AISStream.io API key before running live ingestion."
        )
    return key


async def _main() -> None:
    import argparse

    from ingestion.sinks import build_sink

    parser = argparse.ArgumentParser(description="Live AISStream.io ingestion client")
    parser.add_argument("--sink", choices=["stdout", "file", "kinesis"], default="stdout")
    parser.add_argument("--out", default="ingestion/sample_data/captured_live.jsonl",
                         help="output path for --sink file")
    parser.add_argument("--stream-name", default=os.environ.get("KINESIS_STREAM_NAME", "portpulse-ais-positions"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--max-seconds", type=float, default=None)
    parser.add_argument("--raw", action="store_true",
                         help="write AISStream's raw JSON instead of normalized records "
                              "(use this for --sink file captures meant to feed replay.py)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    api_key = load_api_key()
    if args.sink == "stdout":
        sink = build_sink("stdout")
    elif args.sink == "file":
        sink = build_sink("file", path=args.out)
    else:
        sink = build_sink("kinesis", stream_name=args.stream_name, region_name=args.region)

    client = AISStreamClient(api_key, sink, max_messages=args.max_messages, max_seconds=args.max_seconds,
                              write_raw=args.raw)
    await client.run()


if __name__ == "__main__":
    asyncio.run(_main())
