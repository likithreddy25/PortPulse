"""Local stand-in for what Firehose + Glue will do in Phase 2, without
touching AWS: reads a JSONL sample (raw AISStream messages or already-
normalized records -- auto-detected), groups records by (port_zone,
date, hour), and writes real Parquet files to a local directory using
that exact partition layout.

This exists so the Parquet schema and port/date/hour partitioning logic
can be proven correct against real captured data before any S3 bucket,
Firehose delivery stream, or Glue crawler is actually created.
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

from ingestion.schema import normalize_message
from streaming.table_schema import derive_partition_values, pyarrow_schema

logger = logging.getLogger(__name__)


def read_records(path: str) -> Iterator[dict[str, Any]]:
    """Yields normalized records, transparently normalizing raw AISStream
    JSON lines if that's what the file contains."""
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)
            if "MessageType" in raw:
                record = normalize_message(raw)
                if record is not None:
                    yield record.to_dict()
            else:
                yield raw


def write_partitioned_parquet(records: Iterator[dict[str, Any]], out_dir: str) -> dict[str, int]:
    """Groups records by (port_zone, date, hour) and writes one Parquet
    file per partition under out_dir/port_zone=X/date=Y/hour=Z/part.parquet.
    Returns {partition_path: row_count} for reporting."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pyarrow_schema()
    column_names = [f.name for f in schema]
    buckets: dict[tuple[str, str, str], list[dict]] = defaultdict(list)

    for record in records:
        key = derive_partition_values(record)
        buckets[key].append(record)

    out_base = Path(out_dir)
    counts: dict[str, int] = {}

    for (port_zone, date, hour), rows in buckets.items():
        partition_dir = out_base / f"port_zone={port_zone}" / f"date={date}" / f"hour={hour}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        columns = {name: [row.get(name) for row in rows] for name in column_names}
        table = pa.table(columns, schema=schema)
        out_path = partition_dir / "part-0000.parquet"
        pq.write_table(table, out_path, compression="snappy")

        counts[str(partition_dir.relative_to(out_base))] = len(rows)

    return counts


def _main() -> None:
    parser = argparse.ArgumentParser(description="Local Parquet + port/date/hour partitioning preview")
    parser.add_argument("--sample", required=True, help="JSONL file: raw AISStream or normalized records")
    parser.add_argument("--out", default="streaming/local_preview_output/raw")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    counts = write_partitioned_parquet(read_records(args.sample), args.out)
    total = sum(counts.values())
    logger.info("Wrote %d records across %d partitions to %s", total, len(counts), args.out)
    for partition, count in sorted(counts.items()):
        logger.info("  %s: %d rows", partition, count)


if __name__ == "__main__":
    _main()
