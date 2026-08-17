"""Tests infra/teardown.py two ways:
1. A full mocked lifecycle (moto) for everything moto actually
   implements: Kinesis, Firehose, Glue, IAM, EventBridge, Lambda,
   CloudWatch, S3. Deploys real-shaped resources via the same create
   calls the deploy scripts use, then tears them down, then asserts
   they're gone.
2. Redshift Serverless is excluded from the moto test -- checked
   directly, moto's redshift-serverless mock doesn't implement
   GetWorkgroup/GetNamespace (raises a generic 404 "Not yet
   implemented" instead of the real ResourceNotFoundException these
   AWS APIs actually return for a missing resource), so the
   not-found-is-a-no-op path can't be exercised through moto without
   the test asserting moto's own limitation rather than real behavior.
   Verified instead via a real dry-run against the actual AWS account
   (skips if credentials aren't configured) -- read-only, deletes
   nothing, but exercises the real GetWorkgroup/GetNamespace calls and
   their real ResourceNotFoundException handling.
"""

from __future__ import annotations

import json
import shutil
import subprocess

import boto3
import pytest
from moto import mock_aws

from infra.teardown import (
    CLOUDWATCH_ALARM_NAMES,
    EVENTBRIDGE_RULE_NAMES,
    IAM_ROLE_NAMES,
    LAMBDA_FUNCTION_NAMES,
    delete_cloudwatch_alarms,
    delete_eventbridge_rules,
    delete_firehose_stream,
    delete_glue_resources,
    delete_iam_roles,
    delete_kinesis_stream,
    delete_lambda_functions,
    delete_s3_bucket,
)

REGION = "us-east-1"


def _create_test_fixture(region: str) -> dict:
    """Creates one real-shaped resource of each moto-supported type this
    project's deploy scripts create, so teardown has something to find
    and delete."""
    kinesis = boto3.client("kinesis", region_name=region)
    kinesis.create_stream(StreamName="portpulse-ais-positions", StreamModeDetails={"StreamMode": "ON_DEMAND"})
    stream_arn = kinesis.describe_stream_summary(StreamName="portpulse-ais-positions")["StreamDescriptionSummary"]["StreamARN"]

    s3 = boto3.client("s3", region_name=region)
    s3.create_bucket(Bucket="portpulse-test-teardown-bucket")
    s3.put_object(Bucket="portpulse-test-teardown-bucket", Key="raw/sample.parquet", Body=b"fake-parquet-bytes")

    firehose = boto3.client("firehose", region_name=region)
    firehose.create_delivery_stream(
        DeliveryStreamName="portpulse-ais-raw", DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "RoleARN": "arn:aws:iam::123456789012:role/firehose-role",
            "BucketARN": "arn:aws:s3:::portpulse-test-teardown-bucket",
            "Prefix": "raw/",
        },
    )

    glue = boto3.client("glue", region_name=region)
    glue.create_database(DatabaseInput={"Name": "portpulse"})
    glue.create_table(DatabaseName="portpulse", TableInput={"Name": "ais_positions_raw"})
    glue.create_crawler(
        Name="portpulse-raw-crawler", Role="arn:aws:iam::123456789012:role/glue-role",
        DatabaseName="portpulse", Targets={"S3Targets": [{"Path": "s3://portpulse-test-teardown-bucket/raw/"}]},
    )

    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    })
    inline_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": "*"}],
    })
    iam = boto3.client("iam", region_name=region)
    for role_name in IAM_ROLE_NAMES[:2]:  # just a couple, not all -- keeps the fixture fast
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust_policy)
        iam.put_role_policy(RoleName=role_name, PolicyName="inline", PolicyDocument=inline_policy)

    lambda_client = boto3.client("lambda", region_name=region)
    for fn_name in LAMBDA_FUNCTION_NAMES:
        lambda_client.create_function(
            FunctionName=fn_name, Runtime="python3.12", Role=f"arn:aws:iam::123456789012:role/{IAM_ROLE_NAMES[0]}",
            Handler="index.handler",
            Code={"ZipFile": b"def handler(e,c): pass"},
        )

    events = boto3.client("events", region_name=region)
    for rule_name in EVENTBRIDGE_RULE_NAMES:
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)", State="ENABLED")
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": f"arn:aws:lambda:{region}:123456789012:function:{LAMBDA_FUNCTION_NAMES[0]}"}])

    cw = boto3.client("cloudwatch", region_name=region)
    for alarm_name in CLOUDWATCH_ALARM_NAMES:
        cw.put_metric_alarm(
            AlarmName=alarm_name, MetricName="FailedConversion.Records", Namespace="AWS/Firehose",
            Statistic="Sum", Period=300, EvaluationPeriods=1, Threshold=1,
            ComparisonOperator="GreaterThanOrEqualToThreshold",
        )

    return {"stream_arn": stream_arn}


@mock_aws
def test_dry_run_deletes_nothing():
    _create_test_fixture(REGION)

    delete_kinesis_stream(boto3.client("kinesis", region_name=REGION), confirm=False)
    delete_firehose_stream(boto3.client("firehose", region_name=REGION), confirm=False)
    delete_glue_resources(boto3.client("glue", region_name=REGION), confirm=False)
    delete_iam_roles(boto3.client("iam", region_name=REGION), confirm=False)
    delete_lambda_functions(boto3.client("lambda", region_name=REGION), confirm=False)
    delete_eventbridge_rules(boto3.client("events", region_name=REGION), confirm=False)
    delete_cloudwatch_alarms(boto3.client("cloudwatch", region_name=REGION), confirm=False)

    kinesis = boto3.client("kinesis", region_name=REGION)
    assert "portpulse-ais-positions" in kinesis.list_streams()["StreamNames"]


@mock_aws
def test_confirmed_teardown_removes_everything_moto_supports():
    _create_test_fixture(REGION)

    kinesis = boto3.client("kinesis", region_name=REGION)
    firehose = boto3.client("firehose", region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)
    iam = boto3.client("iam", region_name=REGION)
    lambda_client = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    cw = boto3.client("cloudwatch", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    delete_kinesis_stream(kinesis, confirm=True)
    delete_firehose_stream(firehose, confirm=True)
    delete_glue_resources(glue, confirm=True)
    delete_iam_roles(iam, confirm=True)
    delete_lambda_functions(lambda_client, confirm=True)
    delete_eventbridge_rules(events, confirm=True)
    delete_cloudwatch_alarms(cw, confirm=True)
    delete_s3_bucket(s3, "portpulse-test-teardown-bucket", confirm=True)

    assert "portpulse-ais-positions" not in kinesis.list_streams()["StreamNames"]
    assert "portpulse-ais-raw" not in firehose.list_delivery_streams()["DeliveryStreamNames"]
    with pytest.raises(Exception):
        glue.get_database(Name="portpulse")
    for role_name in IAM_ROLE_NAMES[:2]:
        with pytest.raises(Exception):
            iam.get_role(RoleName=role_name)
    assert lambda_client.list_functions()["Functions"] == []
    assert events.list_rules()["Rules"] == []
    assert cw.describe_alarms()["MetricAlarms"] == []
    with pytest.raises(Exception):
        s3.head_bucket(Bucket="portpulse-test-teardown-bucket")


@mock_aws
def test_teardown_is_idempotent_on_already_empty_account():
    # Nothing created -- every delete function should just no-op, not raise.
    delete_kinesis_stream(boto3.client("kinesis", region_name=REGION), confirm=True)
    delete_firehose_stream(boto3.client("firehose", region_name=REGION), confirm=True)
    delete_glue_resources(boto3.client("glue", region_name=REGION), confirm=True)
    delete_iam_roles(boto3.client("iam", region_name=REGION), confirm=True)
    delete_lambda_functions(boto3.client("lambda", region_name=REGION), confirm=True)
    delete_eventbridge_rules(boto3.client("events", region_name=REGION), confirm=True)
    delete_cloudwatch_alarms(boto3.client("cloudwatch", region_name=REGION), confirm=True)
    delete_s3_bucket(boto3.client("s3", region_name=REGION), "does-not-exist-bucket", confirm=True)


def _aws_credentials_available() -> bool:
    if shutil.which("aws") is None:
        return False
    result = subprocess.run(["aws", "sts", "get-caller-identity"], capture_output=True, timeout=15)
    return result.returncode == 0


def test_real_dry_run_against_actual_account_including_redshift_and_emr():
    if not _aws_credentials_available():
        pytest.skip("AWS credentials not configured in this environment")

    from infra.teardown import run

    # confirm=False -- every call in here is a read-only describe/get/list,
    # this creates and deletes nothing for real.
    run(region=REGION, bucket_name=None, delete_bucket=False, confirm=False)
