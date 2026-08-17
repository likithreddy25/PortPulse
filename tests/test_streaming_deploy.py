import boto3
from moto import mock_aws

from streaming.deploy import run
from streaming.table_schema import DATABASE_NAME, RAW_TABLE_NAME

REGION = "us-east-1"
ACCOUNT_ID = "123456789012"
BUCKET = "portpulse-test-raw"
STREAM = "portpulse-test-stream"


@mock_aws
def test_dry_run_creates_nothing():
    run(region=REGION, account_id=ACCOUNT_ID, bucket_name=BUCKET, stream_name=STREAM, confirm=False)

    s3 = boto3.client("s3", region_name=REGION)
    assert s3.list_buckets()["Buckets"] == []

    kinesis = boto3.client("kinesis", region_name=REGION)
    assert kinesis.list_streams()["StreamNames"] == []


@mock_aws
def test_confirmed_run_creates_full_pipeline_end_to_end():
    run(region=REGION, account_id=ACCOUNT_ID, bucket_name=BUCKET, stream_name=STREAM, confirm=True)

    kinesis = boto3.client("kinesis", region_name=REGION)
    assert kinesis.describe_stream_summary(StreamName=STREAM)["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE"

    s3 = boto3.client("s3", region_name=REGION)
    assert BUCKET in [b["Name"] for b in s3.list_buckets()["Buckets"]]

    iam = boto3.client("iam", region_name=REGION)
    role_names = [r["RoleName"] for r in iam.list_roles()["Roles"]]
    assert "portpulse-firehose-role" in role_names
    assert "portpulse-glue-role" in role_names

    glue = boto3.client("glue", region_name=REGION)
    assert glue.get_database(Name=DATABASE_NAME)["Database"]["Name"] == DATABASE_NAME
    table = glue.get_table(DatabaseName=DATABASE_NAME, Name=RAW_TABLE_NAME)["Table"]
    assert table["Name"] == RAW_TABLE_NAME
    assert [c["Name"] for c in table["PartitionKeys"]] == ["port_zone", "date", "hour"]
    assert glue.get_crawler(Name="portpulse-raw-crawler")["Crawler"]["Name"] == "portpulse-raw-crawler"

    firehose = boto3.client("firehose", region_name=REGION)
    desc = firehose.describe_delivery_stream(DeliveryStreamName="portpulse-ais-raw")["DeliveryStreamDescription"]
    assert desc["DeliveryStreamType"] == "KinesisStreamAsSource"


@mock_aws
def test_confirmed_run_is_idempotent_on_rerun():
    run(region=REGION, account_id=ACCOUNT_ID, bucket_name=BUCKET, stream_name=STREAM, confirm=True)
    # Re-running should hit the "already exists" branches, not raise.
    run(region=REGION, account_id=ACCOUNT_ID, bucket_name=BUCKET, stream_name=STREAM, confirm=True)

    glue = boto3.client("glue", region_name=REGION)
    assert glue.get_database(Name=DATABASE_NAME)["Database"]["Name"] == DATABASE_NAME
