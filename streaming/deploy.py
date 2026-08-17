"""Creates the real AWS resources for Phases 1-2: IAM roles, the raw S3
bucket, the Kinesis stream, the Glue database/table/crawler, and the
Firehose delivery stream.

SAFETY: every AWS-mutating call is gated behind an explicit --confirm
flag. Without it, this only prints the plan (resource names, config) and
touches nothing. This script is deliberately a thin, readable sequence
of boto3 calls rather than a CloudFormation stack -- Phase 8 formalizes
everything built across Phases 1-7 into proper split CloudFormation
stacks. This is how each phase gets stood up and validated before that
consolidation happens.

Each function is independently callable and idempotent-ish (checks
existence before creating) so a partial failure can be re-run.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

from ingestion.config import AWS_REGION_DEFAULT, KINESIS_STREAM_NAME_DEFAULT
from streaming.firehose_config import build_primary_config
from streaming.glue_catalog_config import crawler_config, database_input, raw_table_input
from streaming.table_schema import DATABASE_NAME, RAW_TABLE_NAME

logger = logging.getLogger(__name__)

FIREHOSE_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "firehose.amazonaws.com"}, "Action": "sts:AssumeRole"}],
}
GLUE_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}],
}

# Inline policies rather than AWS-managed *FullAccess ARNs -- narrower
# than the managed alternatives, though still broader than the
# per-resource least-privilege policies Phase 8's CloudFormation stacks
# will define (which can scope to exact ARNs that don't exist until
# after these resources are created).
FIREHOSE_INLINE_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetBucketLocation", "s3:ListBucket"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["kinesis:DescribeStream", "kinesis:GetShardIterator", "kinesis:GetRecords",
                                        "kinesis:ListShards"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["glue:GetTable", "glue:GetTableVersion", "glue:GetTableVersions"], "Resource": "*"},
    ],
}
GLUE_INLINE_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["glue:*"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], "Resource": "*"},
    ],
}


def create_kinesis_stream(client, stream_name: str, confirm: bool) -> None:
    try:
        client.describe_stream_summary(StreamName=stream_name)
        logger.info("Kinesis stream %s already exists", stream_name)
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    logger.info("[plan] create Kinesis stream %s (ON_DEMAND)", stream_name)
    if not confirm:
        return
    client.create_stream(StreamName=stream_name, StreamModeDetails={"StreamMode": "ON_DEMAND"})
    waiter = client.get_waiter("stream_exists")
    waiter.wait(StreamName=stream_name)
    logger.info("Kinesis stream %s created", stream_name)


def create_raw_bucket(client, bucket_name: str, region: str, confirm: bool) -> None:
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info("S3 bucket %s already exists", bucket_name)
        return
    except ClientError:
        pass

    logger.info("[plan] create S3 bucket %s in %s", bucket_name, region)
    if not confirm:
        return
    kwargs = {"Bucket": bucket_name}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    client.create_bucket(**kwargs)
    client.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": True,
        },
    )
    logger.info("S3 bucket %s created (public access blocked)", bucket_name)


def create_iam_role(client, role_name: str, trust_policy: dict, inline_policy: dict, confirm: bool) -> str:
    try:
        role = client.get_role(RoleName=role_name)
        logger.info("IAM role %s already exists", role_name)
        return role["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise

    logger.info("[plan] create IAM role %s with inline policy", role_name)
    if not confirm:
        return f"arn:aws:iam::PLANNED:role/{role_name}"

    role = client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy))
    client.put_role_policy(
        RoleName=role_name, PolicyName=f"{role_name}-inline", PolicyDocument=json.dumps(inline_policy)
    )
    logger.info("IAM role %s created", role_name)
    return role["Role"]["Arn"]


def create_glue_database(client, confirm: bool) -> None:
    try:
        client.get_database(Name=DATABASE_NAME)
        logger.info("Glue database %s already exists", DATABASE_NAME)
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise

    logger.info("[plan] create Glue database %s", DATABASE_NAME)
    if not confirm:
        return
    client.create_database(DatabaseInput=database_input())
    logger.info("Glue database %s created", DATABASE_NAME)


def create_glue_table(client, s3_location: str, confirm: bool) -> None:
    try:
        client.get_table(DatabaseName=DATABASE_NAME, Name=RAW_TABLE_NAME)
        logger.info("Glue table %s.%s already exists", DATABASE_NAME, RAW_TABLE_NAME)
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise

    logger.info("[plan] create Glue table %s.%s at %s", DATABASE_NAME, RAW_TABLE_NAME, s3_location)
    if not confirm:
        return
    client.create_table(DatabaseName=DATABASE_NAME, TableInput=raw_table_input(s3_location=s3_location))
    logger.info("Glue table %s.%s created", DATABASE_NAME, RAW_TABLE_NAME)


def create_glue_crawler(client, role_arn: str, s3_target_path: str, confirm: bool) -> None:
    cfg = crawler_config(role_arn=role_arn, s3_target_path=s3_target_path)
    try:
        client.get_crawler(Name=cfg["Name"])
        logger.info("Glue crawler %s already exists", cfg["Name"])
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise

    logger.info("[plan] create Glue crawler %s targeting %s (on-demand, no schedule yet)", cfg["Name"], s3_target_path)
    if not confirm:
        return
    client.create_crawler(**cfg)
    logger.info("Glue crawler %s created", cfg["Name"])


def create_firehose_stream(client, config: dict, confirm: bool) -> None:
    name = config["DeliveryStreamName"]
    try:
        client.describe_delivery_stream(DeliveryStreamName=name)
        logger.info("Firehose delivery stream %s already exists", name)
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    logger.info("[plan] create Firehose delivery stream %s", name)
    if not confirm:
        return
    client.create_delivery_stream(**config)
    logger.info("Firehose delivery stream %s created (may take a minute to become ACTIVE)", name)


def run(*, region: str, account_id: str, bucket_name: str, stream_name: str, confirm: bool) -> None:
    kinesis = boto3.client("kinesis", region_name=region)
    s3 = boto3.client("s3", region_name=region)
    iam = boto3.client("iam", region_name=region)
    glue = boto3.client("glue", region_name=region)
    firehose = boto3.client("firehose", region_name=region)

    create_kinesis_stream(kinesis, stream_name, confirm)
    create_raw_bucket(s3, bucket_name, region, confirm)

    firehose_role_arn = create_iam_role(
        iam, "portpulse-firehose-role", FIREHOSE_TRUST_POLICY, FIREHOSE_INLINE_POLICY, confirm,
    )
    glue_role_arn = create_iam_role(
        iam, "portpulse-glue-role", GLUE_TRUST_POLICY, GLUE_INLINE_POLICY, confirm,
    )

    s3_location = f"s3://{bucket_name}/raw/"
    create_glue_database(glue, confirm)
    create_glue_table(glue, s3_location, confirm)
    create_glue_crawler(glue, glue_role_arn, s3_location, confirm)

    if confirm:
        stream_arn = kinesis.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]["StreamARN"]
    else:
        stream_arn = f"arn:aws:kinesis:{region}:{account_id}:stream/{stream_name}"
    bucket_arn = f"arn:aws:s3:::{bucket_name}"

    firehose_config = build_primary_config(
        bucket_arn=bucket_arn, firehose_role_arn=firehose_role_arn,
        kinesis_stream_arn=stream_arn, region=region, account_id=account_id,
    )
    create_firehose_stream(firehose, firehose_config, confirm)

    if not confirm:
        logger.info("Dry run complete -- nothing was created. Re-run with --confirm to actually deploy.")


def _main() -> None:
    parser = argparse.ArgumentParser(description="Deploy Phase 1-2 AWS resources (Kinesis, S3, Glue, Firehose)")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", AWS_REGION_DEFAULT))
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--bucket-name", required=True)
    parser.add_argument("--stream-name", default=os.environ.get("KINESIS_STREAM_NAME", KINESIS_STREAM_NAME_DEFAULT))
    parser.add_argument("--confirm", action="store_true", help="actually create resources (default: dry run/plan only)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    run(region=args.region, account_id=args.account_id, bucket_name=args.bucket_name,
        stream_name=args.stream_name, confirm=args.confirm)


if __name__ == "__main__":
    _main()
