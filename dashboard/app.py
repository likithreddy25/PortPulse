"""PortPulse dashboard (Streamlit). Port congestion from real AIS data: a cross-port
snapshot, a per-port breakdown of the congestion index, and trend + forecast views
that only appear when there is enough real history to support them.

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
from forecasting.sarimax_forecast import MIN_HISTORY_HOURS, forecast_congestion_index
from ingestion.config import PORTS

# Documented in README "Congestion index formula" and batch/emr_jobs/congestion.py
W_DWELL, W_VESSEL, W_QUEUE = 0.4, 0.3, 0.3
CAP_VESSEL, CAP_DWELL_H, CAP_QUEUE = 40, 48, 25

C_VESSEL, C_DWELL, C_QUEUE = "#4C78A8", "#F58518", "#E45756"
PLOT_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(size=13),
)

st.set_page_config(page_title="PortPulse", page_icon="🚢", layout="wide")

st.markdown(
    """
    <style>
    .block-container {padding-top: 2rem; max-width: 1250px;}
    [data-testid="stMetricValue"] {font-size: 1.9rem;}
    [data-testid="stMetricLabel"] p {opacity: .75; font-size: .85rem;}
    h1 {margin-bottom: 0;}
    </style>
    """,
    unsafe_allow_html=True,
)


def components(row: pd.Series) -> dict[str, float]:
    """Points each component contributes to the 0-100 index (formula from the README)."""
    dwell = row["avg_dwell_time_hours"]
    dwell = 0.0 if pd.isna(dwell) else float(dwell)
    return {
        "Vessel count": 100 * W_VESSEL * min(row["vessel_count"] / CAP_VESSEL, 1.0),
        "Dwell time": 100 * W_DWELL * min(dwell / CAP_DWELL_H, 1.0),
        "Queue length": 100 * W_QUEUE * min(row["queue_length"] / CAP_QUEUE, 1.0),
    }


# ----------------------------------------------------------------- header + data
st.title("🚢 PortPulse")
st.caption("Port congestion analytics from real AIS vessel positions: Spark → curated Parquet / Redshift → SARIMAX forecast")

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
    st.info(
        "**Demo mode.** Real AIS data from PortPulse's own captured sample, processed by a "
        "manual run of the Spark job. Not a live pipeline and not continuously updating.",
        icon="🧪",
    )
    try:
        df = load_local(curated_path)
    except FileNotFoundError:
        st.error(
            f"No curated Parquet found at `{curated_path}`. Run the Phase 3 pipeline first "
            "(see README Setup: `python -m batch.emr_jobs.pipeline ...`)."
        )
        st.stop()
else:
    st.warning(
        "**Live mode.** Queries Redshift Serverless via the Data API. The stack was deployed and "
        "loaded once on 2026-10-15 (`warehouse/live_load.py`, 4 port-hour rows verified), then "
        "torn down to avoid cost. Redeploy `warehouse.yaml` for this mode to return data.",
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

ports_with_data = [p for p in PORTS if p.port_id in set(df["port_id"].unique())]
latest_by_port = (
    df.sort_values("hour_start").groupby("port_id").tail(1).set_index("port_id")
)
hours_by_port = df.groupby("port_id")["hour_start"].nunique()

# ----------------------------------------------------------------- summary row
c1, c2, c3, c4 = st.columns(4)
c1.metric("Ports with data", f"{len(ports_with_data)} of {len(PORTS)}")
c2.metric("Vessels in port zones", int(latest_by_port["vessel_count"].sum()))
c3.metric("Vessels queued (<1 kn)", int(latest_by_port["queue_length"].sum()))
c4.metric("Hours of history (max per port)", int(hours_by_port.max()))

# ----------------------------------------------------------------- cross-port comparison
st.subheader("Port comparison, latest hour")
left, right = st.columns(2)

names = {p.port_id: p.name.split(",")[0].split(" /")[0] for p in PORTS}
order = latest_by_port.sort_values("congestion_index", ascending=False).index.tolist()

with left:
    fig = go.Figure()
    for label, color in (("Vessel count", C_VESSEL), ("Dwell time", C_DWELL), ("Queue length", C_QUEUE)):
        fig.add_trace(
            go.Bar(
                y=[names[p] for p in order],
                x=[components(latest_by_port.loc[p])[label] for p in order],
                name=label,
                orientation="h",
                marker_color=color,
            )
        )
    fig.update_layout(
        barmode="stack",
        height=300,
        xaxis=dict(range=[0, 100], title="Congestion index (0-100), by component"),
        yaxis=dict(autorange="reversed"),
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", y=1.18),
        **PLOT_LAYOUT,
    )
    st.plotly_chart(fig, use_container_width=True, key="cmp-index")

with right:
    fig = go.Figure()
    fig.add_trace(go.Bar(x=[names[p] for p in order],
                         y=[latest_by_port.loc[p, "vessel_count"] for p in order],
                         name="Vessels in zone", marker_color=C_VESSEL))
    fig.add_trace(go.Bar(x=[names[p] for p in order],
                         y=[latest_by_port.loc[p, "queue_length"] for p in order],
                         name="Queued (<1 kn)", marker_color=C_QUEUE))
    fig.update_layout(
        barmode="group", height=300, yaxis_title="Vessels",
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", y=1.18),
        **PLOT_LAYOUT,
    )
    st.plotly_chart(fig, use_container_width=True, key="cmp-vessels")

missing = [p.name for p in PORTS if p.port_id not in set(df["port_id"].unique())]
if missing:
    st.caption("No data in this sample window for: " + ", ".join(missing) + ".")

# ----------------------------------------------------------------- per-port detail
st.subheader("Port detail")
tabs = st.tabs([f"{p.port_id}" for p in ports_with_data])

for tab, port in zip(tabs, ports_with_data):
    with tab:
        port_df = df[df["port_id"] == port.port_id].sort_values("hour_start")
        latest = port_df.iloc[-1]
        n_hours = port_df["hour_start"].nunique()
        st.markdown(f"**{port.name}**, latest hour starting {latest['hour_start']:%Y-%m-%d %H:%M} UTC")

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Vessels in zone", int(latest["vessel_count"]))
        m2.metric("Queued (<1 kn)", int(latest["queue_length"]))
        dwell = latest["avg_dwell_time_hours"]
        m3.metric("Avg dwell (h)", f"{dwell:.2f}" if pd.notna(dwell) else "n/a")
        m4.metric("Congestion index", f"{latest['congestion_index']:.0f} / 100")

        if pd.isna(dwell):
            st.caption("Dwell time is n/a: no anchorage visit finished in this window, so there is nothing to average.")

        # What drives the index
        comp = components(latest)
        cap_note = []
        if latest["vessel_count"] >= CAP_VESSEL:
            cap_note.append(f"vessel count is above its {CAP_VESSEL}-vessel cap")
        if latest["queue_length"] >= CAP_QUEUE:
            cap_note.append(f"queue length is above its {CAP_QUEUE}-vessel cap")
        if cap_note:
            st.caption("Index is capped on: " + " and ".join(cap_note) + ". Caps are starting assumptions, to be recalibrated with more history (see Methodology).")

        # Data coverage / readiness
        st.markdown("**Data coverage**")
        st.progress(min(n_hours / 2, 1.0), text=f"Trend chart: {n_hours} of 2 hours needed")
        st.progress(min(n_hours / MIN_HISTORY_HOURS, 1.0), text=f"Forecast: {n_hours} of {MIN_HISTORY_HOURS} hours needed")

        series = port_df.set_index("hour_start")["congestion_index"]
        forecast_result = forecast_congestion_index(port.port_id, series, horizon_hours=horizon_hours)

        if n_hours >= 2:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=port_df["hour_start"], y=port_df["congestion_index"],
                                     mode="lines+markers", name="Observed", line=dict(color=C_VESSEL)))
            if forecast_result.status == "ok":
                fx = [p.hour_start for p in forecast_result.forecast]
                fy = [p.predicted_congestion_index for p in forecast_result.forecast]
                lo = [p.ci_lower for p in forecast_result.forecast]
                hi = [p.ci_upper for p in forecast_result.forecast]
                fig.add_trace(go.Scatter(x=fx + fx[::-1], y=hi + lo[::-1], fill="toself",
                                         fillcolor="rgba(245,133,24,0.15)", line=dict(width=0),
                                         name="80% interval"))
                fig.add_trace(go.Scatter(x=fx, y=fy, mode="lines+markers", name="SARIMAX forecast",
                                         line=dict(color=C_DWELL, dash="dash")))
            fig.update_layout(height=320, yaxis=dict(range=[0, 100], title="Congestion index"),
                              margin=dict(l=10, r=10, t=10, b=10),
                              legend=dict(orientation="h", y=1.15), **PLOT_LAYOUT)
            st.plotly_chart(fig, use_container_width=True, key=f"trend-{port.port_id}")
        else:
            st.info(
                "A trend chart appears once there are at least 2 hours of data. "
                "A single hour is shown above as a snapshot instead of a one-point line.",
                icon="📈",
            )
        if forecast_result.status != "ok":
            st.caption(f"No forecast: {forecast_result.reason}")

# ----------------------------------------------------------------- methodology
with st.expander("Methodology and limitations"):
    st.markdown(
        f"""
**Congestion index (0-100)** = 100 × (0.3·min(vessels/{CAP_VESSEL}, 1) + 0.4·min(avg dwell h/{CAP_DWELL_H}, 1) + 0.3·min(queued/{CAP_QUEUE}, 1)).
The stacked bars show each component's points.

- **Vessels in zone:** distinct vessels inside a 20 km radius of the port center during the hour.
- **Queued:** of those, vessels moving under 1 knot.
- **Dwell time:** average length of anchorage visits that *ended* in that hour.
- **Forecast:** SARIMAX(1,1,1), shown only with at least {MIN_HISTORY_HOURS} hourly observations, so it never fits noise.

**Limitations.** The caps are documented starting assumptions, not derived from history, and
likely saturate for busy ports. The demo data is a short real capture, so it is a snapshot,
not a time series. Redshift live mode has not been deployed. See the README for build status.
"""
    )
    with st.expander("Raw curated rows"):
        st.dataframe(
            df.sort_values(["port_id", "hour_start"])[
                ["port_id", "hour_start", "vessel_count", "queue_length", "avg_dwell_time_hours", "congestion_index"]
            ],
            use_container_width=True,
            hide_index=True,
        )
