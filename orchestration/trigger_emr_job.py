"""Lambda handler triggered hourly by EventBridge: starts an EMR
Serverless job run of batch/emr_jobs/pipeline.py.

NOT YET RUNNABLE FOR REAL: no EMR Serverless application has been
created (Phase 3 only runs the same pipeline locally against Spark --
see README Build status). The application ID and role ARN are read from
environment variables so this same handler works once Phase 8's
CloudFormation creates the real application, without code changes.
"""

from __future__ import annotations

import os
from typing import Any, Optional


def handler(event: dict, context: Any, emr_client: Optional[Any] = None) -> dict:
    """emr_client is injectable for testing; in real Lambda execution
    it's created from the environment (has IAM role credentials via the
    Lambda execution role, not explicit keys)."""
    if emr_client is None:
        import boto3
        emr_client = boto3.client("emr-serverless")

    application_id = os.environ["EMR_SERVERLESS_APPLICATION_ID"]
    execution_role_arn = os.environ["EMR_EXECUTION_ROLE_ARN"]
    input_path = os.environ["CURATED_INPUT_PATH"]  # s3://.../raw/ -- the Phase 2 raw zone
    output_path = os.environ["CURATED_OUTPUT_PATH"]  # s3://.../curated/
    script_path = os.environ["PIPELINE_SCRIPT_S3_PATH"]  # s3://.../pipeline.py

    response = emr_client.start_job_run(
        applicationId=application_id,
        executionRoleArn=execution_role_arn,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": script_path,
                "entryPointArguments": [
                    "--input", input_path,
                    "--output", output_path,
                    "--input-format", "parquet",
                ],
                "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g",
            }
        },
        name="portpulse-hourly-enrichment",
    )
    return {"jobRunId": response["jobRunId"], "applicationId": application_id}
