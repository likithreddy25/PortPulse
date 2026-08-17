"""Creates the real Redshift Serverless namespace + workgroup, then
applies schema.sql/seed files via the Redshift Data API (no VPC network
path needed to run this from a laptop -- see the publiclyAccessible note
below).

SAFETY: mutating calls gated behind --confirm, same pattern as
streaming/deploy.py. Without it, this only prints the plan.

NOT VERIFIED VIA MOTO: moto does not implement redshift-serverless
(CreateNamespace returns "Not yet implemented" as of the moto version
this project pins -- checked directly, not assumed). The request shapes
below were checked against AWS's own API reference docs instead. A real
dry-run (--confirm omitted) against the actual AWS account exercises the
read-only existence checks, which is the extent of verification possible
before an actual deploy.

publiclyAccessible=False by default: the workgroup won't be reachable by
direct psql/JDBC from a laptop. Querying it (the load script, the
dashboard) should go through the Redshift Data API (execute_statement /
get_statement_result), which is IAM-authenticated over the AWS API and
needs no VPC network path. Set --publicly-accessible if direct DB-client
access is wanted instead -- that also needs a security group rule
allowing the connecting IP, not set up here.
"""

from __future__ import annotations

import argparse
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

from ingestion.config import AWS_REGION_DEFAULT

logger = logging.getLogger(__name__)

NAMESPACE_NAME = "portpulse"
WORKGROUP_NAME = "portpulse-wg"
DB_NAME = "portpulse"
BASE_CAPACITY_RPU = 4  # minimum allowed; $0.375/RPU-hour, billed only while active (60s minimum per activation)


def create_namespace(client, admin_password: str, confirm: bool) -> None:
    try:
        client.get_namespace(namespaceName=NAMESPACE_NAME)
        logger.info("Redshift Serverless namespace %s already exists", NAMESPACE_NAME)
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    logger.info("[plan] create Redshift Serverless namespace %s (db=%s)", NAMESPACE_NAME, DB_NAME)
    if not confirm:
        return
    client.create_namespace(
        namespaceName=NAMESPACE_NAME, dbName=DB_NAME, adminUsername="portpulse_admin",
        adminUserPassword=admin_password,
    )
    logger.info("Namespace %s created", NAMESPACE_NAME)


def create_workgroup(client, publicly_accessible: bool, confirm: bool) -> None:
    try:
        client.get_workgroup(workgroupName=WORKGROUP_NAME)
        logger.info("Redshift Serverless workgroup %s already exists", WORKGROUP_NAME)
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    logger.info(
        "[plan] create Redshift Serverless workgroup %s (namespace=%s, baseCapacity=%d RPU, publiclyAccessible=%s)",
        WORKGROUP_NAME, NAMESPACE_NAME, BASE_CAPACITY_RPU, publicly_accessible,
    )
    if not confirm:
        return
    client.create_workgroup(
        workgroupName=WORKGROUP_NAME, namespaceName=NAMESPACE_NAME,
        baseCapacity=BASE_CAPACITY_RPU, publiclyAccessible=publicly_accessible,
    )
    logger.info("Workgroup %s created (may take several minutes to become AVAILABLE)", WORKGROUP_NAME)


def wait_for_workgroup_available(client, timeout_seconds: int = 600) -> None:
    start = time.monotonic()
    while time.monotonic() - start < timeout_seconds:
        status = client.get_workgroup(workgroupName=WORKGROUP_NAME)["workgroup"]["status"]
        if status == "AVAILABLE":
            logger.info("Workgroup %s is AVAILABLE", WORKGROUP_NAME)
            return
        logger.info("Workgroup status: %s, waiting...", status)
        time.sleep(15)
    raise TimeoutError(f"Workgroup {WORKGROUP_NAME} did not become AVAILABLE within {timeout_seconds}s")


def parse_statements(sql_text: str) -> list[str]:
    """Strips '--' line comments, then splits on ';'. Comments in these
    DDL/seed files are prose and routinely contain literal semicolons
    (e.g. "...enforced; they are informational only") -- splitting on
    ';' *before* removing comments (an earlier version of this function
    did that) breaks statements apart at semicolons inside comment text,
    not just at real statement boundaries. Not a general SQL parser
    (would mishandle '--' or ';' inside a string literal), but this
    project's DDL/seed files don't have either."""
    lines_without_comments = []
    for line in sql_text.splitlines():
        comment_start = line.find("--")
        lines_without_comments.append(line[:comment_start] if comment_start != -1 else line)
    stripped = "\n".join(lines_without_comments)
    return [s.strip() for s in stripped.split(";") if s.strip()]


def apply_sql_file(data_client, path: str, database: str = DB_NAME) -> None:
    """Runs a .sql file's statements via the Redshift Data API (works
    without a VPC network path)."""
    with open(path) as fh:
        content = fh.read()
    statements = parse_statements(content)

    for statement in statements:
        resp = data_client.execute_statement(
            WorkgroupName=WORKGROUP_NAME, Database=database, Sql=statement
        )
        statement_id = resp["Id"]
        _wait_for_statement(data_client, statement_id)


def _wait_for_statement(data_client, statement_id: str, timeout_seconds: int = 120) -> None:
    start = time.monotonic()
    while time.monotonic() - start < timeout_seconds:
        desc = data_client.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status == "FINISHED":
            return
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Statement {statement_id} {status}: {desc.get('Error')}")
        time.sleep(2)
    raise TimeoutError(f"Statement {statement_id} did not finish within {timeout_seconds}s")


def _main() -> None:
    parser = argparse.ArgumentParser(description="Deploy Redshift Serverless namespace/workgroup + schema")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", AWS_REGION_DEFAULT))
    parser.add_argument("--admin-password", help="required with --confirm")
    parser.add_argument("--publicly-accessible", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--apply-schema", action="store_true", help="also run schema.sql + seed files after the workgroup is available")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.confirm and not args.admin_password:
        parser.error("--admin-password is required with --confirm")

    rs_client = boto3.client("redshift-serverless", region_name=args.region)
    create_namespace(rs_client, args.admin_password or "", args.confirm)
    create_workgroup(rs_client, args.publicly_accessible, args.confirm)

    if not args.confirm:
        logger.info("Dry run complete -- nothing was created. Re-run with --confirm and --admin-password to actually deploy.")
        return

    if args.apply_schema:
        wait_for_workgroup_available(rs_client)
        data_client = boto3.client("redshift-data", region_name=args.region)
        ddl_dir = os.path.join(os.path.dirname(__file__), "redshift_ddl")
        for filename in ["schema.sql", "seed_dim_port.sql", "seed_dim_vessel_type.sql"]:
            logger.info("Applying %s", filename)
            apply_sql_file(data_client, os.path.join(ddl_dir, filename))
        logger.info("Schema applied")


if __name__ == "__main__":
    _main()
