#!/usr/bin/env python3
# app_streamlit.py — ISO-NE Load Dashboard (Streamlit + DuckDB)

import os
from datetime import timedelta
import pandas as pd
import duckdb
import streamlit as st
import altair as alt

# ---- Config ----
DB_PATH = os.getenv("DUCKDB_PATH", "data/warehouse/isone.duckdb")
DEFAULT_WINDOW_HOURS = 24        # for sparklines
AUTOREFRESH_SECS = int(os.getenv("DASH_REFRESH_SEC", "60"))  # cache TTLs

# ---- Connections & cached queries ----
@st.cache_resource
def get_conn():
    # read_only avoids accidental writes and improves stability
    return duckdb.connect(DB_PATH, read_only=True)

@st.cache_data(ttl=AUTOREFRESH_SECS)
def latest_timestamps():
    con = get_conn()
    sys_ts = con.execute("SELECT MAX(ts_utc) FROM system_load").fetchone()[0]
    zon_ts = con.execute("SELECT MAX(ts_utc) FROM zonal_load").fetchone()[0]
    # Convert to pandas Timestamps for safety
    return pd.to_datetime(sys_ts), pd.to_datetime(zon_ts)

@st.cache_data(ttl=AUTOREFRESH_SECS)
def latest_system():
    con = get_conn()
    q = """
      SELECT iso, ts_utc, ts_local, location, load_mw
      FROM system_load
      WHERE ts_utc = (SELECT MAX(ts_utc) FROM system_load)
    """
    return con.execute(q).df()

@st.cache_data(ttl=AUTOREFRESH_SECS)
def latest_zonal():
    con = get_conn()
    q = """
      SELECT iso, ts_utc, ts_local, zone_id, zone_name, load_mw
      FROM zonal_load
      WHERE ts_utc = (SELECT MAX(ts_utc) FROM zonal_load)
    """
    return con.execute(q).df()

@st.cache_data(ttl=AUTOREFRESH_SECS)
def windowed_zonal(hours: int):
    con = get_conn()
    # Pull last N hours (relative to max ts_utc present)
    q = f"""
      WITH max_ts AS (SELECT MAX(ts_utc) AS mx FROM zonal_load)
      SELECT z.iso, z.ts_utc, z.ts_local, z.zone_id, z.zone_name, z.load_mw
      FROM zonal_load z, max_ts m
      WHERE z.ts_utc >= m.mx - INTERVAL {hours} HOUR
    """
    df = con.execute(q).df()
    return df

@st.cache_data(ttl=AUTOREFRESH_SECS)
def heatmap_data(days: int = 28):
    con = get_conn()
    q = f"""
      WITH max_ts AS (SELECT MAX(ts_utc) AS mx FROM zonal_load)
      SELECT
        zone_name,
        EXTRACT('dow'  FROM ts_utc) AS dow,     -- 0=Sunday in DuckDB
        EXTRACT('hour' FROM ts_utc) AS hr,
        AVG(load_mw) AS avg_mw
      FROM zonal_load z, max_ts m
      WHERE z.ts_utc >= m.mx - INTERVAL {days} DAY
      GROUP BY zone_name, dow, hr
    """
    df = con.execute(q).df()
    # Map 0..6 -> labels. DuckDB: 0=Sunday
    df["weekday"] = df["dow"].map({0:"Sun",1:"Mon",2:"Tue",3:"Wed",4:"Thu",5:"Fri",6:"Sat"})
    return df

# ---- Small helpers for visuals ----
def parity_card(sys_df: pd.DataFrame, zon_df: pd.DataFrame):
    if sys_df.empty or zon_df.empty:
        return "n/a", "n/a", "n/a"
    ts = pd.to_datetime(sys_df["ts_utc"].iloc[0])
    system = float(sys_df["load_mw"].iloc[0])
    zones_sum = float(zon_df["load_mw"].sum())
    delta = zones_sum - system
    pct = (delta / system) * 100 if system else 0.0
    return system, zones_sum, f"{delta:+.1f} MW ({pct:+.2f}%)"

def bar_latest_zones(zon_df: pd.DataFrame):
    df = zon_df.copy()
    df["zone_label"] = df["zone_name"].fillna(df["zone_id"]).astype(str)
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("zone_label:N", sort="-y", title="Zone"),
            y=alt.Y("load_mw:Q", title="MW"),
            tooltip=["zone_label","load_mw"]
        )
        .properties(height=300)
    )
    return chart

def sparklines(df: pd.DataFrame):
    # Faceted, per-zone mini line charts (robust across Altair versions)
    d = df.copy()
    d["zone_name"] = d["zone_name"].fillna(d["zone_id"]).astype(str)
    d = d.sort_values(["zone_name", "ts_utc"])

    base = (
        alt.Chart(d)
        .mark_line()
        .encode(
            x=alt.X("ts_utc:T", title=None, axis=alt.Axis(labels=False, ticks=False, grid=False)),
            y=alt.Y("load_mw:Q", title=None, axis=alt.Axis(labels=False, ticks=False, grid=False)),
        )
        # IMPORTANT: set size on the inner chart *before* facet
        .properties(width=250, height=120)
    )

    chart = base.facet(
        facet=alt.Facet("zone_name:N", title=None),
        columns=4,
    )

    return chart

    # Robust, minimal small multiples: one line per zone, faceted grid
    d = df.copy()
    d["zone_name"] = d["zone_name"].fillna(d["zone_id"]).astype(str)
    d = d.sort_values(["zone_name", "ts_utc"])
    base = alt.Chart(d).mark_line().encode(
        x=alt.X("ts_utc:T", title=None, axis=alt.Axis(labels=False, ticks=False)),
        y=alt.Y("load_mw:Q", title=None, axis=alt.Axis(labels=False, ticks=False)),
    )
    facet = base.facet(
        facet=alt.Facet("zone_name:N", title=None),
        columns=4,
    ).properties(width=250, height=120)
    return facet

def heatmap(df: pd.DataFrame):
    chart = (
        alt.Chart(df)
        .mark_rect()
        .encode(
            x=alt.X("hr:O", title="Hour (UTC)"),
            y=alt.Y("weekday:O", sort=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], title="Day"),
            color=alt.Color("avg_mw:Q", title="Avg MW"),
            tooltip=["zone_name","weekday","hr","avg_mw"]
        )
        .properties(height=240)
    )
    return chart

# ---- UI ----
st.set_page_config(page_title="ISO-NE Load Dashboard", layout="wide")
st.title("ISO-NE Load Dashboard")

# Sidebar controls
win = st.sidebar.selectbox("Window (hours)", [1, 6, 24, 48, 168], index=2)
st.sidebar.caption(f"DuckDB: {DB_PATH}")
st.sidebar.caption("Tip: refresh the page to fetch the newest interval.")

# Load datasets (with gentle error handling)
try:
    sys_df = latest_system()
    zon_df_latest = latest_zonal()
except Exception as e:
    st.error(f"Error reading DuckDB at {DB_PATH}: {e}")
    st.stop()

# Now panel
col1, col2, col3, col4 = st.columns([2,2,2,3], gap="large")
system, zones_sum, delta_str = parity_card(sys_df, zon_df_latest)
ts_utc, ts_utc_z = latest_timestamps()
with col1:
    st.metric("System Load (latest MW)", f"{system:,.0f}" if system != 'n/a' else "n/a")
with col2:
    st.metric("Sum of Zones (MW)", f"{zones_sum:,.0f}" if zones_sum != 'n/a' else "n/a")
with col3:
    st.metric("Parity (zones − system)", delta_str)
with col4:
    st.write("**Latest timestamp (UTC)**")
    st.code(str(ts_utc))

# Zonal bar (latest)
st.subheader("Zonal Load — Latest Interval")
st.altair_chart(bar_latest_zones(zon_df_latest), use_container_width=True)

# Sparklines (last N hours)
st.subheader(f"Per-Zone Sparklines — Last {win} Hours")
try:
    zon_win = windowed_zonal(int(win))
    zon_win["zone_name"] = zon_win["zone_name"].fillna(zon_win["zone_id"])
    st.altair_chart(sparklines(zon_win), use_container_width=True)
except Exception as e:
    st.warning(f"Could not render sparklines: {e}")

# Heatmap (patterns)
st.subheader("Zonal Load Heatmap — Hour × Weekday (last 28 days)")
try:
    hm = heatmap(heatmap_data(days=28))
    st.altair_chart(hm, use_container_width=True)
except Exception as e:
    st.warning(f"Could not render heatmap: {e}")
