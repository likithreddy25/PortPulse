"""Validates the ANSI-portable parts of the Redshift schema against a
real local Postgres container. Skips if Docker isn't available/running
-- this isn't a substitute for testing against real Redshift (see the
discrepancies documented in warehouse/redshift_ddl/schema.sql: Redshift
doesn't enforce PK/FK/UNIQUE constraints, and ON CONFLICT isn't valid
Redshift syntax even though it is valid Postgres), but it does catch
real SQL mistakes (a column too narrow for its data was caught this way
during development) before they'd show up against a real, billable
Redshift Serverless endpoint.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

import pytest

DDL_DIR = Path(__file__).parent.parent / "warehouse" / "redshift_ddl"
CONTAINER_NAME = "portpulse-pg-pytest"


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=5, check=True)
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available/running")


@pytest.fixture(scope="module")
def pg_container():
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)
    subprocess.run(
        ["docker", "run", "-d", "--name", CONTAINER_NAME, "-e", "POSTGRES_PASSWORD=test", "postgres:16"],
        capture_output=True, check=True,
    )
    for _ in range(30):
        result = subprocess.run(
            ["docker", "exec", CONTAINER_NAME, "pg_isready", "-U", "postgres"], capture_output=True
        )
        if result.returncode == 0:
            break
        time.sleep(1)
    else:
        subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)
        pytest.fail("Postgres container did not become ready in time")

    yield CONTAINER_NAME
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)


def _psql(container: str, *, sql: str | None = None, sql_file: Path | None = None) -> subprocess.CompletedProcess:
    args = ["docker", "exec", "-u", "postgres", container, "psql", "-v", "ON_ERROR_STOP=1"]
    if sql_file is not None:
        remote_path = f"/tmp/{sql_file.name}"
        subprocess.run(["docker", "cp", str(sql_file), f"{container}:{remote_path}"], check=True)
        args += ["-f", remote_path]
    else:
        args += ["-c", sql]
    return subprocess.run(args, capture_output=True, text=True)


def test_schema_and_seeds_apply_cleanly(pg_container):
    result = _psql(pg_container, sql_file=DDL_DIR / "postgres_compat.sql")
    assert result.returncode == 0, result.stderr

    result = _psql(pg_container, sql_file=DDL_DIR / "seed_dim_port.sql")
    assert result.returncode == 0, result.stderr

    result = _psql(pg_container, sql_file=DDL_DIR / "seed_dim_vessel_type.sql")
    assert result.returncode == 0, result.stderr


def test_dim_port_has_five_rows_matching_config(pg_container):
    result = _psql(pg_container, sql="SELECT count(*) FROM portpulse.dim_port;")
    assert "5" in result.stdout


def test_dim_vessel_type_has_ten_categories(pg_container):
    result = _psql(pg_container, sql="SELECT count(*) FROM portpulse.dim_vessel_type;")
    assert "10" in result.stdout


def test_fact_table_accepts_valid_insert(pg_container):
    result = _psql(
        pg_container,
        sql=(
            "INSERT INTO portpulse.fact_port_congestion_hourly "
            "(port_id, hour_start, vessel_count, avg_dwell_time_hours, queue_length, congestion_index) "
            "VALUES ('NLRTM', '2026-08-17 15:00:00', 401, NULL, 339, 60.0);"
        ),
    )
    assert result.returncode == 0, result.stderr
