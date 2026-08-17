import boto3
from moto import mock_aws

from orchestration.deploy import (
    CHECK_FRESHNESS_FUNCTION,
    FIREHOSE_ALARM,
    FRESHNESS_ALARM,
    FRESHNESS_RULE,
    HOURLY_RULE,
    TRIGGER_EMR_FUNCTION,
    run,
)

REGION = "us-east-1"


@mock_aws
def test_dry_run_creates_nothing():
    run(region=REGION, firehose_stream_name="portpulse-ais-raw", sns_topic_arn=None, confirm=False)

    lambda_client = boto3.client("lambda", region_name=REGION)
    assert lambda_client.list_functions()["Functions"] == []

    events_client = boto3.client("events", region_name=REGION)
    assert events_client.list_rules()["Rules"] == []


@mock_aws
def test_confirmed_run_creates_full_orchestration_pipeline():
    run(region=REGION, firehose_stream_name="portpulse-ais-raw", sns_topic_arn=None, confirm=True)

    lambda_client = boto3.client("lambda", region_name=REGION)
    function_names = [f["FunctionName"] for f in lambda_client.list_functions()["Functions"]]
    assert TRIGGER_EMR_FUNCTION in function_names
    assert CHECK_FRESHNESS_FUNCTION in function_names

    events_client = boto3.client("events", region_name=REGION)
    rule_names = [r["Name"] for r in events_client.list_rules()["Rules"]]
    assert HOURLY_RULE in rule_names
    assert FRESHNESS_RULE in rule_names

    hourly_targets = events_client.list_targets_by_rule(Rule=HOURLY_RULE)["Targets"]
    assert len(hourly_targets) == 1

    cw_client = boto3.client("cloudwatch", region_name=REGION)
    alarm_names = [a["AlarmName"] for a in cw_client.describe_alarms()["MetricAlarms"]]
    assert FIREHOSE_ALARM in alarm_names
    assert FRESHNESS_ALARM in alarm_names


@mock_aws
def test_confirmed_run_is_idempotent_on_rerun():
    run(region=REGION, firehose_stream_name="portpulse-ais-raw", sns_topic_arn=None, confirm=True)
    run(region=REGION, firehose_stream_name="portpulse-ais-raw", sns_topic_arn=None, confirm=True)

    lambda_client = boto3.client("lambda", region_name=REGION)
    function_names = [f["FunctionName"] for f in lambda_client.list_functions()["Functions"]]
    assert function_names.count(TRIGGER_EMR_FUNCTION) == 1
