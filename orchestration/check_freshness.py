"""Lambda handler triggered every 15 minutes by EventBridge: checks how
long it's been since the curated table's newest row, and publishes that
as a CloudWatch custom metric (PortPulse/CuratedFreshnessMinutes) for a
CloudWatch alarm to watch (see orchestration/deploy.py: alarms if this
exceeds 90 minutes, per the project brief).

NOT YET RUNNABLE FOR REAL: queries Redshift via the Data API, which
isn't deployed (see README Build status). Handler logic is unit tested
with injected mock clients rather than a real endpoint.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

FRESHNESS_METRIC_NAMESPACE = "PortPulse"
FRESHNESS_METRIC_NAME = "CuratedFreshnessMinutes"


def _latest_curated_hour(data_client, workgroup_name: str, database: str) -> Optional[datetime]:
    resp = data_client.execute_statement(
        WorkgroupName=workgroup_name, Database=database,
        Sql="SELECT MAX(hour_start) AS latest FROM portpulse.fact_port_congestion_hourly",
    )
    statement_id = resp["Id"]
    while True:
        desc = data_client.describe_statement(Id=statement_id)
        if desc["Status"] == "FINISHED":
            break
        if desc["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Freshness query failed: {desc.get('Error')}")
        time.sleep(1)

    result = data_client.get_statement_result(Id=statement_id)
    records = result["Records"]
    if not records or "stringValue" not in records[0][0]:
        return None  # empty table -- no curated data has ever landed
    return datetime.fromisoformat(records[0][0]["stringValue"]).replace(tzinfo=timezone.utc)


def handler(event: dict, context: Any, data_client: Optional[Any] = None, cw_client: Optional[Any] = None) -> dict:
    if data_client is None or cw_client is None:
        import boto3
        data_client = data_client or boto3.client("redshift-data")
        cw_client = cw_client or boto3.client("cloudwatch")

    workgroup_name = os.environ.get("REDSHIFT_WORKGROUP", "portpulse-wg")
    database = os.environ.get("REDSHIFT_DATABASE", "portpulse")

    latest_hour = _latest_curated_hour(data_client, workgroup_name, database)

    if latest_hour is None:
        # No curated data has ever landed -- report a large staleness
        # value rather than skipping the metric, so the alarm fires
        # instead of silently having no data point to evaluate.
        staleness_minutes = float("inf")
        reported_value = 99999.0
    else:
        staleness_minutes = (datetime.now(timezone.utc) - latest_hour).total_seconds() / 60.0
        reported_value = staleness_minutes

    cw_client.put_metric_data(
        Namespace=FRESHNESS_METRIC_NAMESPACE,
        MetricData=[{"MetricName": FRESHNESS_METRIC_NAME, "Value": reported_value, "Unit": "None"}],
    )
    return {"staleness_minutes": staleness_minutes, "latest_hour": str(latest_hour)}
