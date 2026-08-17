"""PortPulse dashboard (Streamlit). Shows live congestion_index per port
over time with a forecast overlay where there's enough real history to
produce one honestly.

Run: streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from dashboard.data_source import load_local, load_redshift
from forecasting.sarimax_forecast import forecast_congestion_index
from ingestion.config import PORTS

st.set_page_config(page_title="PortPulse", layout="wide")

st.title("PortPulse — Port Congestion Analytics")

with st.sidebar:
    st.header("Data source")
    mode = st.radio("Mode", ["Local Parquet (demo)", "Redshift (live)"], index=0)

    if mode == "Local Parquet (demo)":
        curated_path = st.text_input("Curated Parquet path", value="/tmp/portpulse_curated")
    else:
        redshift_region = st.text_input("AWS region", value="us-east-1")
        redshift_workgroup = st.text_input("Redshift workgroup", value="portpulse-wg")

    horizon_hours = st.slider("Forecast horizon (hours)", 1, 12, 6)

if mode == "Local Parquet (demo)":
    st.warning(
        "**DEMO MODE** — reading local Parquet output from a manual Phase 3 run, "
        "not a live pipeline. This is real data from PortPulse's own captured AIS "
        "sample, not synthetic, but it is not continuously updating.",
        icon="🧪",
    )
    try:
        df = load_local(curated_path)
    except FileNotFoundError:
        st.error(
            f"No curated Parquet found at `{curated_path}`. Run the Phase 3 pipeline first — "
            f"see README Setup section (`python -m batch.emr_jobs.pipeline ...`)."
        )
        st.stop()
else:
    st.info(
        "**LIVE MODE** — querying Redshift Serverless via the Data API. "
        "Untested against a real deployment as of this build (see README Build status) — "
        "Redshift hasn't been deployed yet.",
        icon="🔴",
    )
    try:
        df = load_redshift(redshift_region, redshift_workgroup)
    except Exception as e:
        st.error(f"Redshift query failed: {e}")
        st.stop()

if df.empty:
    st.warning("No curated data available yet.")
    st.stop()

port_ids_with_data = set(df["port_id"].unique())

for port in PORTS:
    st.subheader(f"{port.name} ({port.port_id})")

    if port.port_id not in port_ids_with_data:
        st.info("No data for this port yet in the current sample window.")
        continue

    port_df = df[df["port_id"] == port.port_id].sort_values("hour_start")
    latest = port_df.iloc[-1]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Vessel count (latest hour)", int(latest["vessel_count"]))
    col2.metric("Queue length (latest hour)", int(latest["queue_length"]))
    dwell = latest["avg_dwell_time_hours"]
    col3.metric("Avg dwell time (h)", f"{dwell:.1f}" if pd.notna(dwell) else "n/a (no completed visits yet)")
    col4.metric("Congestion index", f"{latest['congestion_index']:.1f} / 100")

    series = port_df.set_index("hour_start")["congestion_index"]
    forecast_result = forecast_congestion_index(port.port_id, series, horizon_hours=horizon_hours)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=port_df["hour_start"], y=port_df["congestion_index"],
                              mode="lines+markers", name="Observed congestion_index"))

    if forecast_result.status == "ok":
        fx = [p.hour_start for p in forecast_result.forecast]
        fy = [p.predicted_congestion_index for p in forecast_result.forecast]
        fy_lo = [p.ci_lower for p in forecast_result.forecast]
        fy_hi = [p.ci_upper for p in forecast_result.forecast]
        fig.add_trace(go.Scatter(x=fx, y=fy, mode="lines+markers", name="Forecast",
                                  line=dict(dash="dash")))
        fig.add_trace(go.Scatter(x=fx + fx[::-1], y=fy_hi + fy_lo[::-1],
                                  fill="toself", fillcolor="rgba(99,110,250,0.15)",
                                  line=dict(width=0), name="80% CI", showlegend=True))
    else:
        st.caption(f"⚠️ No forecast shown: {forecast_result.reason}")

    fig.update_layout(height=300, margin=dict(l=20, r=20, t=20, b=20), yaxis_range=[0, 100])
    st.plotly_chart(fig, use_container_width=True, key=f"chart-{port.port_id}")
