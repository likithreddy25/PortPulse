"""Short-horizon congestion_index forecast per port, using SARIMAX.

Deliberately honest about data sufficiency (per the project brief): if
there isn't enough real history for a given port, this returns an
explicit "insufficient_data" result with a stated reason instead of
fitting a model on too little data and producing a forecast that looks
real but isn't trustworthy. See README for what this looks like run
against PortPulse's actual (currently ~1-2 hours of real) data.

## Why these specific minimums

- **MIN_HISTORY_HOURS = 48** (2 days): the smallest amount of history
  where fitting even a low-order ARIMA(1,1,1) isn't just curve-fitting
  noise. This is a reasonable floor, not a rigorously derived number --
  standard guidance for ARIMA-family models is "enough points to
  estimate the model's parameters with some stability," and 48 hourly
  points is on the thin side of that even for a simple order.
- **Not attempting weekly seasonality yet**: port congestion plausibly
  has weekly patterns (weekday vs weekend traffic), but capturing that
  reliably needs several weeks of history (order of 336-504+ hourly
  points), which nowhere close to exists yet. `order=(1,1,1)` with no
  seasonal component is what's used here -- upgrading to a seasonal
  SARIMAX order once enough history accumulates is a documented future
  step, not something faked now.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

MIN_HISTORY_HOURS = 48
MIN_COVERAGE_RATIO = 0.8  # require at least 80% of expected hourly slots present, not just N points
DEFAULT_ORDER = (1, 1, 1)


@dataclass
class ForecastPoint:
    hour_start: pd.Timestamp
    predicted_congestion_index: float
    ci_lower: float
    ci_upper: float


@dataclass
class ForecastResult:
    port_id: str
    status: str  # "ok" | "insufficient_data"
    reason: Optional[str]
    model_order: Optional[tuple[int, int, int]]
    history_hours_used: int
    forecast: Optional[list[ForecastPoint]]


def _coverage_ratio(history: pd.Series) -> float:
    if len(history) < 2:
        return 0.0
    span_hours = int((history.index.max() - history.index.min()) / pd.Timedelta(hours=1)) + 1
    return len(history) / span_hours if span_hours > 0 else 0.0


def forecast_congestion_index(
    port_id: str, history: pd.Series, horizon_hours: int = 6, order: tuple[int, int, int] = DEFAULT_ORDER,
) -> ForecastResult:
    """history: a pandas Series of congestion_index values indexed by
    hourly-truncated UTC timestamps (pd.Timestamp), one port's data.
    Duplicate/out-of-order timestamps are not handled here -- caller
    (e.g. a Redshift query) is expected to provide clean hourly rows."""
    n = len(history)

    if n < MIN_HISTORY_HOURS:
        return ForecastResult(
            port_id=port_id, status="insufficient_data",
            reason=(
                f"only {n} hourly observations available, need at least "
                f"{MIN_HISTORY_HOURS} (~{MIN_HISTORY_HOURS // 24} days) to fit a "
                f"meaningful SARIMAX{order} model without just curve-fitting noise"
            ),
            model_order=None, history_hours_used=n, forecast=None,
        )

    coverage = _coverage_ratio(history)
    if coverage < MIN_COVERAGE_RATIO:
        return ForecastResult(
            port_id=port_id, status="insufficient_data",
            reason=(
                f"{n} observations span a range that should have significantly more "
                f"hourly data points ({coverage:.0%} coverage, need >={MIN_COVERAGE_RATIO:.0%}) "
                f"-- likely gaps from ingestion not running continuously; a model fit on "
                f"this gappy a series would not be trustworthy"
            ),
            model_order=None, history_hours_used=n, forecast=None,
        )

    from statsmodels.tsa.statespace.sarimax import SARIMAX

    series = history.sort_index()
    model = SARIMAX(series, order=order, enforce_stationarity=False, enforce_invertibility=False)
    fitted = model.fit(disp=False)

    pred = fitted.get_forecast(steps=horizon_hours)
    mean = pred.predicted_mean
    ci = pred.conf_int(alpha=0.2)  # 80% interval -- short-horizon, small-sample forecast; not claiming tight precision

    last_hour = series.index.max()
    forecast_points = []
    for i in range(horizon_hours):
        hour_start = last_hour + pd.Timedelta(hours=i + 1)
        forecast_points.append(ForecastPoint(
            hour_start=hour_start,
            predicted_congestion_index=max(0.0, min(100.0, float(mean.iloc[i]))),
            ci_lower=max(0.0, float(ci.iloc[i, 0])),
            ci_upper=min(100.0, float(ci.iloc[i, 1])),
        ))

    return ForecastResult(
        port_id=port_id, status="ok", reason=None, model_order=order,
        history_hours_used=n, forecast=forecast_points,
    )
