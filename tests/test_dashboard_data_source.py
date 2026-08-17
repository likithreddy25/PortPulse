from pathlib import Path

import pytest

from dashboard.data_source import load_local


def test_load_local_against_real_captured_curated_data():
    curated_path = Path("/tmp/portpulse_curated")
    if not curated_path.exists():
        pytest.skip("run batch.emr_jobs.pipeline against the real sample first (see README)")

    df = load_local(str(curated_path))

    assert not df.empty
    assert {"port_id", "port_name", "hour_start", "congestion_index", "vessel_count"}.issubset(df.columns)
    assert "NLRTM" in df["port_id"].values
    # port_name should be joined in from ingestion/config.py, not left null
    assert df[df["port_id"] == "NLRTM"]["port_name"].iloc[0] == "Rotterdam, Netherlands"


def test_load_local_missing_path_raises_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_local("/tmp/definitely_does_not_exist_portpulse")
