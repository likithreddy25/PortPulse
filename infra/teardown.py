"""Deletes every billable PortPulse AWS resource this project's deploy
scripts (streaming/deploy.py, warehouse/deploy.py, orchestration/deploy.py)
could have created, across whatever subset actually exists. Safe to run
at any point in the project's life -- every delete checks existence
first and is a no-op if the resource was never created or already
deleted (idempotent, re-runnable).

SAFETY: same pattern as every deploy script here -- mutating calls
gated behind --confirm, defaults to a dry-run/plan-only listing of what
exists and would be deleted. The S3 bucket is a further opt-in
(--delete-bucket) since it may hold real captured data worth keeping
even after tearing down the compute/streaming resources -- per the
project brief: "S3 objects if I ask."

Entry point is teardown.sh (`./teardown.sh` or `./teardown.sh --confirm`),
a thin wrapper around this module.
"""

from __future__ import annotations

import argparse
import logging
import os

import boto3
from botocore.exceptions import ClientError

from ingestion.config import AWS_REGION_DEFAULT

logger = logging.getLogger(__name__)

KINESIS_STREAM_NAME = "portpulse-ais-positions"
FIREHOSE_STREAM_NAME = "portpulse-ais-raw"
GLUE_DATABASE_NAME = "portpulse"
GLUE_TABLE_NAME = "ais_positions_raw"
GLUE_CRAWLER_NAME = "portpulse-raw-crawler"
EMR_APPLICATION_NAME = "portpulse-enrichment"
REDSHIFT_WORKGROUP_NAME = "portpulse-wg"
REDSHIFT_NAMESPACE_NAME = "portpulse"
IAM_ROLE_NAMES = [
    "portpulse-firehose-role", "portpulse-glue-role", "portpulse-lambda-role",
    "portpulse-trigger-lambda-role", "portpulse-freshness-lambda-role", "portpulse-emr-execution-role",
]
LAMBDA_FUNCTION_NAMES = ["portpulse-trigger-emr-job", "portpulse-check-freshness"]
EVENTBRIDGE_RULE_NAMES = ["portpulse-hourly-emr-trigger", "portpulse-freshness-check"]
CLOUDWATCH_ALARM_NAMES = ["portpulse-firehose-conversion-failures", "portpulse-curated-data-stale"]


def _not_found(e: ClientError, *codes: str) -> bool:
    return e.response["Error"]["Code"] in codes


def delete_kinesis_stream(client, confirm: bool) -> None:
    try:
        client.describe_stream_summary(StreamName=KINESIS_STREAM_NAME)
    except ClientError as e:
        if _not_found(e, "ResourceNotFoundException"):
            return
        raise
    logger.info("[plan] delete Kinesis stream %s", KINESIS_STREAM_NAME)
    if confirm:
        client.delete_stream(StreamName=KINESIS_STREAM_NAME, EnforceConsumerDeletion=True)


def delete_firehose_stream(client, confirm: bool) -> None:
    try:
        client.describe_delivery_stream(DeliveryStreamName=FIREHOSE_STREAM_NAME)
    except ClientError as e:
        if _not_found(e, "ResourceNotFoundException"):
            return
        raise
    logger.info("[plan] delete Firehose delivery stream %s", FIREHOSE_STREAM_NAME)
    if confirm:
        client.delete_delivery_stream(DeliveryStreamName=FIREHOSE_STREAM_NAME)


def delete_glue_resources(client, confirm: bool) -> None:
    try:
        client.get_crawler(Name=GLUE_CRAWLER_NAME)
        logger.info("[plan] delete Glue crawler %s", GLUE_CRAWLER_NAME)
        if confirm:
            client.delete_crawler(Name=GLUE_CRAWLER_NAME)
    except ClientError as e:
        if not _not_found(e, "EntityNotFoundException"):
            raise

    try:
        client.get_table(DatabaseName=GLUE_DATABASE_NAME, Name=GLUE_TABLE_NAME)
        logger.info("[plan] delete Glue table %s.%s", GLUE_DATABASE_NAME, GLUE_TABLE_NAME)
        if confirm:
            client.delete_table(DatabaseName=GLUE_DATABASE_NAME, Name=GLUE_TABLE_NAME)
    except ClientError as e:
        if not _not_found(e, "EntityNotFoundException"):
            raise

    try:
        client.get_database(Name=GLUE_DATABASE_NAME)
        logger.info("[plan] delete Glue database %s", GLUE_DATABASE_NAME)
        if confirm:
            client.delete_database(Name=GLUE_DATABASE_NAME)
    except ClientError as e:
        if not _not_found(e, "EntityNotFoundException"):
            raise


def delete_emr_application(client, confirm: bool) -> None:
    apps = client.list_applications(states=["CREATED", "STARTED", "STOPPED"])["applications"]
    match = next((a for a in apps if a["name"] == EMR_APPLICATION_NAME), None)
    if match is None:
        return
    logger.info("[plan] stop + delete EMR Serverless application %s", EMR_APPLICATION_NAME)
    if confirm:
        client.stop_application(applicationId=match["id"])
        client.delete_application(applicationId=match["id"])


def delete_redshift_resources(client, confirm: bool) -> None:
    try:
        client.get_workgroup(workgroupName=REDSHIFT_WORKGROUP_NAME)
        logger.info("[plan] delete Redshift Serverless workgroup %s", REDSHIFT_WORKGROUP_NAME)
        if confirm:
            client.delete_workgroup(workgroupName=REDSHIFT_WORKGROUP_NAME)
    except ClientError as e:
        if not _not_found(e, "ResourceNotFoundException"):
            raise

    try:
        client.get_namespace(namespaceName=REDSHIFT_NAMESPACE_NAME)
        logger.info("[plan] delete Redshift Serverless namespace %s", REDSHIFT_NAMESPACE_NAME)
        if confirm:
            client.delete_namespace(namespaceName=REDSHIFT_NAMESPACE_NAME)
    except ClientError as e:
        if not _not_found(e, "ResourceNotFoundException"):
            raise


def delete_eventbridge_rules(events_client, confirm: bool) -> None:
    for rule_name in EVENTBRIDGE_RULE_NAMES:
        try:
            events_client.describe_rule(Name=rule_name)
        except ClientError as e:
            if _not_found(e, "ResourceNotFoundException"):
                continue
            raise
        logger.info("[plan] delete EventBridge rule %s", rule_name)
        if confirm:
            targets = events_client.list_targets_by_rule(Rule=rule_name)["Targets"]
            if targets:
                events_client.remove_targets(Rule=rule_name, Ids=[t["Id"] for t in targets])
            events_client.delete_rule(Name=rule_name)


def delete_lambda_functions(client, confirm: bool) -> None:
    for name in LAMBDA_FUNCTION_NAMES:
        try:
            client.get_function(FunctionName=name)
        except ClientError as e:
            if _not_found(e, "ResourceNotFoundException"):
                continue
            raise
        logger.info("[plan] delete Lambda function %s", name)
        if confirm:
            client.delete_function(FunctionName=name)


def delete_cloudwatch_alarms(client, confirm: bool) -> None:
    existing = client.describe_alarms(AlarmNames=CLOUDWATCH_ALARM_NAMES)["MetricAlarms"]
    if not existing:
        return
    logger.info("[plan] delete CloudWatch alarms %s", [a["AlarmName"] for a in existing])
    if confirm:
        client.delete_alarms(AlarmNames=[a["AlarmName"] for a in existing])


def delete_iam_roles(client, confirm: bool) -> None:
    for role_name in IAM_ROLE_NAMES:
        try:
            client.get_role(RoleName=role_name)
        except ClientError as e:
            if _not_found(e, "NoSuchEntity"):
                continue
            raise
        logger.info("[plan] delete IAM role %s (and its inline policies)", role_name)
        if confirm:
            for policy_name in client.list_role_policies(RoleName=role_name)["PolicyNames"]:
                client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            for policy in client.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
                client.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])
            client.delete_role(RoleName=role_name)


def delete_s3_bucket(client, bucket_name: str, confirm: bool) -> None:
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError:
        return
    logger.info("[plan] empty + delete S3 bucket %s", bucket_name)
    if confirm:
        paginator = client.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket_name):
            objects = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("Versions", [])]
            objects += [{"Key": m["Key"], "VersionId": m["VersionId"]} for m in page.get("DeleteMarkers", [])]
            if objects:
                client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
        client.delete_bucket(Bucket=bucket_name)


def run(*, region: str, bucket_name: str | None, delete_bucket: bool, confirm: bool) -> None:
    kinesis = boto3.client("kinesis", region_name=region)
    firehose = boto3.client("firehose", region_name=region)
    glue = boto3.client("glue", region_name=region)
    emr = boto3.client("emr-serverless", region_name=region)
    redshift = boto3.client("redshift-serverless", region_name=region)
    events = boto3.client("events", region_name=region)
    lambda_client = boto3.client("lambda", region_name=region)
    cw = boto3.client("cloudwatch", region_name=region)
    iam = boto3.client("iam", region_name=region)
    s3 = boto3.client("s3", region_name=region)

    delete_eventbridge_rules(events, confirm)
    delete_lambda_functions(lambda_client, confirm)
    delete_cloudwatch_alarms(cw, confirm)
    delete_emr_application(emr, confirm)
    delete_redshift_resources(redshift, confirm)
    delete_firehose_stream(firehose, confirm)
    delete_glue_resources(glue, confirm)
    delete_kinesis_stream(kinesis, confirm)
    delete_iam_roles(iam, confirm)

    if delete_bucket:
        if not bucket_name:
            logger.warning("--delete-bucket set but no --bucket-name given, skipping")
        else:
            delete_s3_bucket(s3, bucket_name, confirm)
    elif bucket_name:
        logger.info(
            "S3 bucket %s left alone (pass --delete-bucket to also remove it -- "
            "it may hold real captured data worth keeping)", bucket_name,
        )

    if not confirm:
        logger.info("Dry run complete -- nothing was deleted. Re-run with --confirm to actually tear down.")


def _main() -> None:
    parser = argparse.ArgumentParser(description="Tear down PortPulse AWS resources")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", AWS_REGION_DEFAULT))
    parser.add_argument("--bucket-name", default=None, help="raw zone S3 bucket name, required with --delete-bucket")
    parser.add_argument("--delete-bucket", action="store_true", help="also empty and delete the S3 bucket")
    parser.add_argument("--confirm", action="store_true", help="actually delete resources (default: dry run/plan only)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    run(region=args.region, bucket_name=args.bucket_name, delete_bucket=args.delete_bucket, confirm=args.confirm)


if __name__ == "__main__":
    _main()
