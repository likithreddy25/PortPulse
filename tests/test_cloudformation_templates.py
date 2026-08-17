"""Validates the CloudFormation templates two ways:
1. cfn-lint (static analysis -- always runs, cfn-lint is a hard dev dep)
2. `aws cloudformation validate-template` against the real service
   (read-only, free API call, creates nothing) -- skips if AWS creds
   aren't configured, since that's environment-dependent.

Neither of these is a substitute for actually deploying the stacks --
see infra/cloudformation/*.yaml docstrings and README Build status for
what's genuinely unverified (e.g. whether AWS accepts the Firehose
Parquet+dynamic-partitioning combination) versus what these checks do
catch (YAML syntax, resource/property names, unresolved references).
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

CFN_DIR = Path(__file__).parent.parent / "infra" / "cloudformation"
TEMPLATES = ["ingestion.yaml", "processing.yaml", "warehouse.yaml"]


@pytest.mark.parametrize("template", TEMPLATES)
def test_cfn_lint_passes(template):
    result = subprocess.run(
        ["cfn-lint", str(CFN_DIR / template)], capture_output=True, text=True
    )
    assert result.returncode == 0, f"cfn-lint failed for {template}:\n{result.stdout}\n{result.stderr}"


def _aws_credentials_available() -> bool:
    if shutil.which("aws") is None:
        return False
    result = subprocess.run(["aws", "sts", "get-caller-identity"], capture_output=True, timeout=15)
    return result.returncode == 0


@pytest.mark.parametrize("template", TEMPLATES)
def test_validate_template_against_real_cloudformation_service(template):
    if not _aws_credentials_available():
        pytest.skip("AWS credentials not configured in this environment")

    result = subprocess.run(
        ["aws", "cloudformation", "validate-template", "--template-body", f"file://{CFN_DIR / template}",
         "--region", "us-east-1"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, f"validate-template failed for {template}:\n{result.stderr}"
    response = json.loads(result.stdout)
    assert "Parameters" in response
