"""Output destinations for normalized vessel positions.

All sinks implement the same tiny interface (`write(record: dict)` and
`close()`) so the client and replay driver don't need to know whether
records are headed to stdout, a local file, or Kinesis.
"""

from __future__ import annotations

import json
import logging
import sys
from abc import ABC, abstractmethod
from typing import Any, Optional

logger = logging.getLogger(__name__)


class Sink(ABC):
    @abstractmethod
    def write(self, record: dict[str, Any]) -> None: ...

    def close(self) -> None:
        pass


class StdoutSink(Sink):
    """Prints one JSON record per line. Useful for `--sink stdout` smoke tests."""

    def write(self, record: dict[str, Any]) -> None:
        print(json.dumps(record), file=sys.stdout, flush=True)


class JsonlFileSink(Sink):
    """Appends one JSON record per line to a local file."""

    def __init__(self, path: str):
        self.path = path
        self._fh = open(path, "a")

    def write(self, record: dict[str, Any]) -> None:
        self._fh.write(json.dumps(record) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class KinesisSink(Sink):
    """Publishes each record to a Kinesis Data Stream, partitioned by MMSI
    so a given vessel's position updates stay in relative order within a
    shard -- Phase 3's dwell-time computation depends on that ordering.
    """

    def __init__(self, stream_name: str, region_name: str, client: Optional[Any] = None):
        self.stream_name = stream_name
        if client is not None:
            self._client = client
        else:
            import boto3

            self._client = boto3.client("kinesis", region_name=region_name)

    def write(self, record: dict[str, Any]) -> None:
        partition_key = str(record.get("mmsi", "unknown"))
        self._client.put_record(
            StreamName=self.stream_name,
            Data=json.dumps(record).encode("utf-8"),
            PartitionKey=partition_key,
        )

    def write_batch(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """PutRecords is far cheaper than one PutRecord call per message
        once real traffic volume shows up; batches of up to 500 per AWS
        limits. Returns the raw PutRecords response so callers can check
        FailedRecordCount."""
        entries = [
            {
                "Data": json.dumps(r).encode("utf-8"),
                "PartitionKey": str(r.get("mmsi", "unknown")),
            }
            for r in records
        ]
        return self._client.put_records(StreamName=self.stream_name, Records=entries)


def build_sink(kind: str, **kwargs: Any) -> Sink:
    if kind == "stdout":
        return StdoutSink()
    if kind == "file":
        return JsonlFileSink(kwargs["path"])
    if kind == "kinesis":
        return KinesisSink(kwargs["stream_name"], kwargs["region_name"])
    raise ValueError(f"unknown sink kind: {kind!r}")
