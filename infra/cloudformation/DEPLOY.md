# Deploying the PortPulse CloudFormation stacks

Three stacks, deployed in this order (each depends on the previous
one's outputs):

1. **`ingestion.yaml`** — Kinesis, S3 raw zone, Firehose, Glue. No
   dependencies.
2. **`processing.yaml`** — EMR Serverless + hourly trigger Lambda. Needs
   `RawBucketArn`/`RawBucketName` from stack 1's outputs.
3. **`warehouse.yaml`** — Redshift Serverless + freshness Lambda. No
   dependencies on the other two stacks (queries Redshift directly via
   the Data API), but logically deployed last since it's the final
   consumer of curated data.

## Important: naming conflict with the existing Phase 2 deployment

**`ingestion.yaml` cannot be deployed as a new stack right now without
first tearing down the resources `streaming/deploy.py` already created**
(see README Build status / Cost). The Kinesis stream
(`portpulse-ais-positions`), IAM roles (`portpulse-firehose-role`,
`portpulse-glue-role`), Glue database (`portpulse`), and Glue table
(`ais_positions_raw`) all exist for real already, created via direct
boto3 calls, not through CloudFormation. Deploying this stack as-is
would hit `AlreadyExists` errors on every one of those resources.

Two ways to resolve this, not attempted here:
- **Recommended**: run Phase 9's `teardown.sh` to delete the boto3-created
  resources, then deploy `ingestion.yaml` cleanly.
- Alternative: use [CloudFormation resource import](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/resource-import.html)
  to bring the existing resources under this stack's management instead
  of recreating them. More complex, not covered by this project.

## Manual deploy commands (NOT run — creates real, billable resources)

```bash
# 1. Ingestion
aws cloudformation deploy \
  --template-file infra/cloudformation/ingestion.yaml \
  --stack-name portpulse-ingestion \
  --parameter-overrides RawBucketName=portpulse-raw-<your-account-id> \
  --capabilities CAPABILITY_NAMED_IAM

# Grab outputs for the next stack
RAW_BUCKET_ARN=$(aws cloudformation describe-stacks --stack-name portpulse-ingestion \
  --query "Stacks[0].Outputs[?OutputKey=='RawBucketArn'].OutputValue" --output text)
RAW_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name portpulse-ingestion \
  --query "Stacks[0].Outputs[?OutputKey=='RawBucketName'].OutputValue" --output text)

# 2. Processing (upload batch/emr_jobs/pipeline.py to S3 first -- not automated here)
aws s3 cp batch/emr_jobs/pipeline.py "s3://${RAW_BUCKET_NAME}/scripts/pipeline.py"

aws cloudformation deploy \
  --template-file infra/cloudformation/processing.yaml \
  --stack-name portpulse-processing \
  --parameter-overrides \
      RawBucketArn="$RAW_BUCKET_ARN" \
      RawBucketName="$RAW_BUCKET_NAME" \
      PipelineScriptS3Path="s3://${RAW_BUCKET_NAME}/scripts/pipeline.py" \
  --capabilities CAPABILITY_NAMED_IAM

# 3. Warehouse
aws cloudformation deploy \
  --template-file infra/cloudformation/warehouse.yaml \
  --stack-name portpulse-warehouse \
  --parameter-overrides AdminPassword='<a real password, 8+ chars, mixed case + digit>' \
  --capabilities CAPABILITY_NAMED_IAM
```

## What's actually been verified

- All three templates pass `cfn-lint` with zero warnings.
- All three templates pass `aws cloudformation validate-template`
  against the real AWS account (read-only, creates nothing) — confirms
  YAML structure, resource/property names, and parameter references are
  well-formed.
- **Not verified**: an actual `deploy`/`create-stack` has never been run
  for any of these three templates — see the naming-conflict note above
  for `ingestion.yaml` specifically, and README Build status for what's
  genuinely uncertain in `ingestion.yaml`'s Firehose resource (the
  Parquet + dynamic-partitioning combination).
