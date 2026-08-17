"""Two ways to get curated congestion data into the dashboard: a local
Parquet reader (works right now, no AWS needed) and a Redshift Data API
reader (works once Phase 4 is actually deployed -- untested against a
real warehouse since none exists yet, see README). The dashboard always
shows which one is active; never silently falls back from one to the
other.
"""

from __future__ import annotations

import pandas as pd
import pyarrow.parquet as pq

from ingestion.config import PORTS


def load_local(curated_parquet_path: str) -> pd.DataFrame:
    df = pq.read_table(curated_parquet_path).to_pandas()
    df["hour_start"] = pd.to_datetime(df["date"] + " " + df["hour"] + ":00:00", utc=True)
    return _with_port_name(df)


def load_redshift(region: str, workgroup_name: str, database: str = "portpulse") -> pd.DataFrame:
    """Queries fact_port_congestion_hourly via the Redshift Data API
    (IAM-authenticated, no VPC network path needed). Not runnable until
    Phase 4 is actually deployed -- see warehouse/deploy.py."""
    import time

    import boto3

    client = boto3.client("redshift-data", region_name=region)
    resp = client.execute_statement(
        WorkgroupName=workgroup_name, Database=database,
        Sql=(
            "SELECT port_id, hour_start, vessel_count, avg_dwell_time_hours, "
            "queue_length, congestion_index FROM portpulse.fact_port_congestion_hourly "
            "ORDER BY port_id, hour_start"
        ),
    )
    statement_id = resp["Id"]
    while True:
        desc = client.describe_statement(Id=statement_id)
        if desc["Status"] == "FINISHED":
            break
        if desc["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift query failed: {desc.get('Error')}")
        time.sleep(1)

    result = client.get_statement_result(Id=statement_id)
    columns = [c["name"] for c in result["ColumnMetadata"]]
    rows = []
    for record in result["Records"]:
        row = {}
        for col, field in zip(columns, record):
            row[col] = next(iter(field.values())) if field else None
        rows.append(row)

    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        df["hour_start"] = pd.to_datetime(df["hour_start"], utc=True)
    return _with_port_name(df)


def _with_port_name(df: pd.DataFrame) -> pd.DataFrame:
    names = {p.port_id: p.name for p in PORTS}
    df = df.copy()
    df["port_name"] = df["port_id"].map(names)
    return df
