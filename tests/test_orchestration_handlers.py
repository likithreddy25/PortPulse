import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from orchestration.check_freshness import handler as freshness_handler
from orchestration.trigger_emr_job import handler as trigger_handler


def test_trigger_emr_job_starts_job_run_with_env_config(monkeypatch):
    monkeypatch.setenv("EMR_SERVERLESS_APPLICATION_ID", "app-123")
    monkeypatch.setenv("EMR_EXECUTION_ROLE_ARN", "arn:aws:iam::123:role/emr-exec")
    monkeypatch.setenv("CURATED_INPUT_PATH", "s3://bucket/raw/")
    monkeypatch.setenv("CURATED_OUTPUT_PATH", "s3://bucket/curated/")
    monkeypatch.setenv("PIPELINE_SCRIPT_S3_PATH", "s3://bucket/pipeline.py")

    mock_emr = MagicMock()
    mock_emr.start_job_run.return_value = {"jobRunId": "jr-abc"}

    result = trigger_handler({}, None, emr_client=mock_emr)

    assert result == {"jobRunId": "jr-abc", "applicationId": "app-123"}
    call_kwargs = mock_emr.start_job_run.call_args.kwargs
    assert call_kwargs["applicationId"] == "app-123"
    assert call_kwargs["executionRoleArn"] == "arn:aws:iam::123:role/emr-exec"
    assert "s3://bucket/pipeline.py" == call_kwargs["jobDriver"]["sparkSubmit"]["entryPoint"]
    assert "--input" in call_kwargs["jobDriver"]["sparkSubmit"]["entryPointArguments"]


def _mock_redshift_data_client(latest_hour_iso: str | None):
    client = MagicMock()
    client.execute_statement.return_value = {"Id": "stmt-1"}
    client.describe_statement.return_value = {"Status": "FINISHED"}
    if latest_hour_iso is None:
        client.get_statement_result.return_value = {"Records": [[{"isNull": True}]]}
    else:
        client.get_statement_result.return_value = {"Records": [[{"stringValue": latest_hour_iso}]]}
    return client


def test_check_freshness_computes_staleness_and_publishes_metric():
    latest = (datetime.now(timezone.utc) - timedelta(minutes=45)).isoformat()
    data_client = _mock_redshift_data_client(latest)
    cw_client = MagicMock()

    result = freshness_handler({}, None, data_client=data_client, cw_client=cw_client)

    assert 44 <= result["staleness_minutes"] <= 46
    put_call = cw_client.put_metric_data.call_args.kwargs
    assert put_call["Namespace"] == "PortPulse"
    metric = put_call["MetricData"][0]
    assert metric["MetricName"] == "CuratedFreshnessMinutes"
    assert 44 <= metric["Value"] <= 46


def test_check_freshness_handles_empty_table_as_maximally_stale():
    data_client = _mock_redshift_data_client(None)
    cw_client = MagicMock()

    result = freshness_handler({}, None, data_client=data_client, cw_client=cw_client)

    assert result["staleness_minutes"] == float("inf")
    metric = cw_client.put_metric_data.call_args.kwargs["MetricData"][0]
    assert metric["Value"] == 99999.0  # large finite value so the alarm can actually evaluate it
