import numpy as np
import pandas as pd

from forecasting.sarimax_forecast import (
    MIN_COVERAGE_RATIO,
    MIN_HISTORY_HOURS,
    forecast_congestion_index,
)


def _synthetic_series(hours: int, seed: int = 0) -> pd.Series:
    rng = np.random.default_rng(seed)
    index = pd.date_range("2026-01-01", periods=hours, freq="h", tz="UTC")
    # mild trend + noise, clipped to a plausible congestion_index range
    values = np.clip(30 + 0.05 * np.arange(hours) + rng.normal(0, 3, hours), 0, 100)
    return pd.Series(values, index=index)


def test_insufficient_data_when_too_few_points():
    series = _synthetic_series(10)
    result = forecast_congestion_index("USLAX", series, horizon_hours=6)

    assert result.status == "insufficient_data"
    assert "10" in result.reason
    assert str(MIN_HISTORY_HOURS) in result.reason
    assert result.forecast is None


def test_insufficient_data_when_gappy_even_with_enough_points():
    # 60 points scattered across a 400-hour span -> well under MIN_COVERAGE_RATIO
    index = pd.date_range("2026-01-01", periods=400, freq="h", tz="UTC")[::7][:60]
    series = pd.Series(np.random.default_rng(1).uniform(10, 50, len(index)), index=index)
    assert len(series) >= MIN_HISTORY_HOURS  # enough raw points...

    result = forecast_congestion_index("NLRTM", series, horizon_hours=6)

    assert result.status == "insufficient_data"
    assert "coverage" in result.reason or "gaps" in result.reason


def test_ok_forecast_with_sufficient_contiguous_history():
    series = _synthetic_series(72)
    result = forecast_congestion_index("USLAX", series, horizon_hours=6)

    assert result.status == "ok"
    assert result.reason is None
    assert result.history_hours_used == 72
    assert len(result.forecast) == 6

    for i, point in enumerate(result.forecast):
        assert point.hour_start == series.index.max() + pd.Timedelta(hours=i + 1)
        assert 0.0 <= point.predicted_congestion_index <= 100.0
        assert point.ci_lower <= point.predicted_congestion_index <= point.ci_upper


def test_forecast_horizon_is_configurable():
    series = _synthetic_series(60)
    result = forecast_congestion_index("SGSIN", series, horizon_hours=3)
    assert len(result.forecast) == 3


def test_real_captured_data_is_honestly_reported_as_insufficient():
    """Runs the actual Phase 3 curated output from the real captured
    sample through the forecaster -- with only 1-2 hours of real data
    per port, this must report insufficient_data, not fabricate a
    forecast. This is the honesty check the project brief asks for."""
    import pyarrow.parquet as pq
    from pathlib import Path

    curated_path = Path("/tmp/portpulse_curated")
    if not curated_path.exists():
        import pytest
        pytest.skip("run batch.emr_jobs.pipeline against the real sample first (see README)")

    df = pq.read_table(str(curated_path)).to_pandas()
    df["hour_start"] = pd.to_datetime(df["date"] + " " + df["hour"] + ":00:00", utc=True)

    for port_id, group in df.groupby("port_id"):
        series = group.set_index("hour_start")["congestion_index"].sort_index()
        result = forecast_congestion_index(port_id, series, horizon_hours=6)
        assert result.status == "insufficient_data", (
            f"expected insufficient_data for {port_id} with only {len(series)} real hourly points, "
            f"got a real forecast -- that would be exactly the fake-looking-chart the project brief "
            f"says not to produce"
        )
