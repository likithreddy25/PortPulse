"""CLI: reads curated hourly congestion data (local Parquet from Phase 3
for now -- would read from Redshift once Phase 4 is deployed) and runs
forecast_congestion_index() per port, printing the real result honestly
-- including "insufficient_data" when that's what the data supports.
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd
import pyarrow.parquet as pq

from forecasting.sarimax_forecast import forecast_congestion_index

logger = logging.getLogger(__name__)


def load_history_by_port(curated_parquet_path: str) -> dict[str, pd.Series]:
    table = pq.read_table(curated_parquet_path)
    df = table.to_pandas()
    df["hour_start"] = pd.to_datetime(df["date"] + " " + df["hour"] + ":00:00", utc=True)

    result = {}
    for port_id, group in df.groupby("port_id"):
        series = group.set_index("hour_start")["congestion_index"].sort_index()
        result[port_id] = series
    return result


def _main() -> None:
    parser = argparse.ArgumentParser(description="Run SARIMAX congestion_index forecast per port")
    parser.add_argument("--curated", required=True, help="path to curated Parquet dir (e.g. /tmp/portpulse_curated)")
    parser.add_argument("--horizon-hours", type=int, default=6)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    history_by_port = load_history_by_port(args.curated)
    for port_id, series in sorted(history_by_port.items()):
        result = forecast_congestion_index(port_id, series, horizon_hours=args.horizon_hours)
        if result.status == "insufficient_data":
            logger.info("%s: INSUFFICIENT DATA -- %s", port_id, result.reason)
        else:
            logger.info("%s: forecast (order=%s, %d hours history):", port_id, result.model_order, result.history_hours_used)
            for point in result.forecast:
                logger.info(
                    "    %s -> %.1f (80%% CI %.1f-%.1f)",
                    point.hour_start, point.predicted_congestion_index, point.ci_lower, point.ci_upper,
                )


if __name__ == "__main__":
    _main()
