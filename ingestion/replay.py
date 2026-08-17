"""Replay a recorded (or synthetic) JSONL sample of raw AISStream messages
through the same normalize -> sink pipeline the live client uses.

This is what lets the rest of PortPulse be developed and tested without
the live feed running or touching AWS: point it at a sample file and any
sink (stdout for a smoke test, file for a fixture, kinesis once you're
ready to test the real pipeline end-to-end).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Iterator

from ingestion.schema import normalize_message
from ingestion.sinks import Sink, build_sink

logger = logging.getLogger(__name__)


def read_raw_messages(path: str) -> Iterator[dict]:
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def replay(path: str, sink: Sink, speed: float = 0.0, loop: bool = False) -> int:
    """Replays raw messages from `path` through normalize() into `sink`.

    speed: seconds to sleep between messages (0 = as fast as possible).
    loop: if True, replay the file repeatedly (useful for a long-running
    local demo of the downstream pipeline).
    Returns the number of normalized records written.
    """
    written = 0
    while True:
        for raw in read_raw_messages(path):
            record = normalize_message(raw)
            if record is not None:
                sink.write(record.to_dict())
                written += 1
            if speed > 0:
                time.sleep(speed)
        if not loop:
            break
    sink.close()
    return written


def _main() -> None:
    parser = argparse.ArgumentParser(description="Replay a recorded AISStream sample")
    parser.add_argument("--sample", default="ingestion/sample_data/synthetic_fixture.jsonl")
    parser.add_argument("--sink", choices=["stdout", "file", "kinesis"], default="stdout")
    parser.add_argument("--out", default="ingestion/sample_data/replayed_output.jsonl")
    parser.add_argument("--stream-name", default=os.environ.get("KINESIS_STREAM_NAME", "portpulse-ais-positions"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument("--speed", type=float, default=0.0, help="seconds between messages")
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.sink == "stdout":
        sink = build_sink("stdout")
    elif args.sink == "file":
        sink = build_sink("file", path=args.out)
    else:
        sink = build_sink("kinesis", stream_name=args.stream_name, region_name=args.region)

    count = replay(args.sample, sink, speed=args.speed, loop=args.loop)
    logger.info("Replayed %d normalized records from %s", count, args.sample)


if __name__ == "__main__":
    _main()
