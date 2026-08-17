"""Creates the real EventBridge rules, Lambda functions, IAM roles, and
CloudWatch alarms for Phase 7 orchestration/monitoring.

SAFETY: mutating calls gated behind --confirm, same pattern as every
other deploy script in this project. Without it, only prints the plan.

Two schedules:
- Hourly: triggers orchestration/trigger_emr_job.py (starts the EMR
  Serverless enrichment job run). Not runnable for real yet -- no EMR
  Serverless application exists (see README Build status).
- Every 15 minutes: triggers orchestration/check_freshness.py (publishes
  the PortPulse/CuratedFreshnessMinutes custom metric). Not runnable for
  real yet -- Redshift isn't deployed.

Two CloudWatch alarms:
- Firehose format-conversion failures: FailedConversion.Records >= 1
  over any 5-minute period. Chosen over DeliveryToS3.Success as the
  primary failure signal because it's an unambiguous count of actual
  failures (checked against AWS's own metric docs); DeliveryToS3.Success
  is documented as "sum of successful S3 put commands" but commonly
  alarmed on as a 0-1 ratio in AWS's own examples -- that ambiguity
  wasn't worth resolving without a real running stream to observe, so
  it's not relied on here.
- Curated-table freshness: CuratedFreshnessMinutes > 90 for 2
  consecutive 15-minute periods (avoids alarming on a single missed
  Lambda invocation).
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import zipfile

import boto3
from botocore.exceptions import ClientError

from ingestion.config import AWS_REGION_DEFAULT
from streaming.firehose_config import DELIVERY_STREAM_NAME

logger = logging.getLogger(__name__)

LAMBDA_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
}
EVENTS_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "events.amazonaws.com"}, "Action": "sts:AssumeRole"}],
}

TRIGGER_EMR_FUNCTION = "portpulse-trigger-emr-job"
CHECK_FRESHNESS_FUNCTION = "portpulse-check-freshness"
HOURLY_RULE = "portpulse-hourly-emr-trigger"
FRESHNESS_RULE = "portpulse-freshness-check"
FIREHOSE_ALARM = "portpulse-firehose-conversion-failures"
FRESHNESS_ALARM = "portpulse-curated-data-stale"


def _zip_single_file(path: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(path, arcname=os.path.basename(path))
    return buf.getvalue()


def create_lambda_role(iam_client, confirm: bool) -> str:
    role_name = "portpulse-lambda-role"
    try:
        role = iam_client.get_role(RoleName=role_name)
        return role["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise

    logger.info("[plan] create IAM role %s", role_name)
    if not confirm:
        return f"arn:aws:iam::PLANNED:role/{role_name}"

    role = iam_client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(LAMBDA_TRUST_POLICY))
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], "Resource": "*"},
            {"Effect": "Allow", "Action": ["emr-serverless:StartJobRun"], "Resource": "*"},
            {"Effect": "Allow", "Action": ["redshift-data:ExecuteStatement", "redshift-data:DescribeStatement", "redshift-data:GetStatementResult"], "Resource": "*"},
            {"Effect": "Allow", "Action": ["cloudwatch:PutMetricData"], "Resource": "*"},
        ],
    }
    iam_client.put_role_policy(RoleName=role_name, PolicyName="portpulse-lambda-inline", PolicyDocument=json.dumps(policy))
    return role["Role"]["Arn"]


def create_lambda_function(lambda_client, function_name: str, source_path: str, handler: str, role_arn: str,
                            env_vars: dict, confirm: bool) -> str:
    try:
        fn = lambda_client.get_function(FunctionName=function_name)
        return fn["Configuration"]["FunctionArn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    logger.info("[plan] create Lambda function %s from %s", function_name, source_path)
    if not confirm:
        return f"arn:aws:lambda:PLANNED:{function_name}"

    zip_bytes = _zip_single_file(source_path)
    fn = lambda_client.create_function(
        FunctionName=function_name, Runtime="python3.12", Role=role_arn, Handler=handler,
        Code={"ZipFile": zip_bytes}, Timeout=120, MemorySize=256,
        Environment={"Variables": env_vars},
    )
    return fn["FunctionArn"]


def create_schedule_rule(events_client, lambda_client, rule_name: str, schedule_expression: str,
                          target_function_name: str, target_function_arn: str, confirm: bool) -> None:
    logger.info("[plan] create EventBridge rule %s (%s) -> %s", rule_name, schedule_expression, target_function_name)
    if not confirm:
        return

    events_client.put_rule(Name=rule_name, ScheduleExpression=schedule_expression, State="ENABLED")
    events_client.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": target_function_arn}])

    try:
        lambda_client.add_permission(
            FunctionName=target_function_name, StatementId=f"{rule_name}-invoke",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":  # permission already added
            raise


def create_alarms(cw_client, stream_name: str, sns_topic_arn: str | None, confirm: bool) -> None:
    alarm_actions = [sns_topic_arn] if sns_topic_arn else []

    logger.info("[plan] create CloudWatch alarm %s (FailedConversion.Records >= 1)", FIREHOSE_ALARM)
    if confirm:
        cw_client.put_metric_alarm(
            AlarmName=FIREHOSE_ALARM, Namespace="AWS/Firehose", MetricName="FailedConversion.Records",
            Dimensions=[{"Name": "DeliveryStreamName", "Value": stream_name}],
            Statistic="Sum", Period=300, EvaluationPeriods=1, Threshold=1,
            ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="notBreaching",
            AlarmActions=alarm_actions,
        )

    logger.info("[plan] create CloudWatch alarm %s (CuratedFreshnessMinutes > 90)", FRESHNESS_ALARM)
    if confirm:
        cw_client.put_metric_alarm(
            AlarmName=FRESHNESS_ALARM, Namespace="PortPulse", MetricName="CuratedFreshnessMinutes",
            Statistic="Maximum", Period=900, EvaluationPeriods=2, Threshold=90,
            ComparisonOperator="GreaterThanThreshold", TreatMissingData="breaching",
            AlarmActions=alarm_actions,
        )


def run(*, region: str, firehose_stream_name: str, sns_topic_arn: str | None, confirm: bool) -> None:
    iam_client = boto3.client("iam", region_name=region)
    lambda_client = boto3.client("lambda", region_name=region)
    events_client = boto3.client("events", region_name=region)
    cw_client = boto3.client("cloudwatch", region_name=region)

    role_arn = create_lambda_role(iam_client, confirm)

    orchestration_dir = os.path.dirname(__file__)
    trigger_arn = create_lambda_function(
        lambda_client, TRIGGER_EMR_FUNCTION, os.path.join(orchestration_dir, "trigger_emr_job.py"),
        "trigger_emr_job.handler", role_arn,
        {"EMR_SERVERLESS_APPLICATION_ID": "PLACEHOLDER", "EMR_EXECUTION_ROLE_ARN": "PLACEHOLDER",
         "CURATED_INPUT_PATH": "PLACEHOLDER", "CURATED_OUTPUT_PATH": "PLACEHOLDER",
         "PIPELINE_SCRIPT_S3_PATH": "PLACEHOLDER"},
        confirm,
    )
    freshness_arn = create_lambda_function(
        lambda_client, CHECK_FRESHNESS_FUNCTION, os.path.join(orchestration_dir, "check_freshness.py"),
        "check_freshness.handler", role_arn,
        {"REDSHIFT_WORKGROUP": "portpulse-wg", "REDSHIFT_DATABASE": "portpulse"},
        confirm,
    )

    create_schedule_rule(events_client, lambda_client, HOURLY_RULE, "rate(1 hour)", TRIGGER_EMR_FUNCTION, trigger_arn, confirm)
    create_schedule_rule(events_client, lambda_client, FRESHNESS_RULE, "rate(15 minutes)", CHECK_FRESHNESS_FUNCTION, freshness_arn, confirm)

    create_alarms(cw_client, firehose_stream_name, sns_topic_arn, confirm)

    if not confirm:
        logger.info("Dry run complete -- nothing was created. Re-run with --confirm to actually deploy.")


def _main() -> None:
    parser = argparse.ArgumentParser(description="Deploy Phase 7 orchestration + monitoring")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", AWS_REGION_DEFAULT))
    parser.add_argument("--firehose-stream-name", default=DELIVERY_STREAM_NAME)
    parser.add_argument("--sns-topic-arn", default=None, help="optional: SNS topic for alarm notifications")
    parser.add_argument("--confirm", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    run(region=args.region, firehose_stream_name=args.firehose_stream_name, sns_topic_arn=args.sns_topic_arn, confirm=args.confirm)


if __name__ == "__main__":
    _main()
