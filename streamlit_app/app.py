import streamlit as st
import pandas as pd
from pathlib import Path
import subprocess
import sys
import time
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
from datetime import datetime

st.set_page_config(page_title="SmartRetail Insights", layout="wide")

st.title("SmartRetail Insights — Daily Revenue")
st.caption(f"🟢 Live • Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "🧩 RFM Segments", "⚡ Live Feed"])



# ---------- 1. PATHS ----------
APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent
DAILY_PATH = PROJECT_ROOT / "data" / "processed" / "daily_revenue.parquet"
RAW_EXCEL  = PROJECT_ROOT / "data" / "raw" / "online_retail_II.xlsx"
LIVE_PATH = PROJECT_ROOT / "data" / "processed" / "live_transactions.parquet"


# ---------- 2. LOAD DATA ----------
# Pre-aggregated daily data (for date defaults)
df_daily_all = pd.read_parquet(DAILY_PATH).sort_values("d")


# 🔧 FIX: standardize column name
df_daily_all = df_daily_all.rename(columns={"line_total": "revenue"})


# Ensure d is a date
df_daily_all["d"] = pd.to_datetime(df_daily_all["d"]).dt.date

TX_PATH = PROJECT_ROOT / "data" / "processed" / "transactions_clean.parquet"

@st.cache_data(show_spinner=False)
def load_daily(path):
    df = pd.read_parquet(path).sort_values("d")
    df = df.rename(columns={"line_total": "revenue"})
    df["d"] = pd.to_datetime(df["d"]).dt.date
    return df

@st.cache_data(show_spinner=False)
def load_tx(path):
    df = pd.read_parquet(path)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df["InvoiceDate_date"] = df["InvoiceDate"].dt.date
    return df

df_daily_all = load_daily(DAILY_PATH)
df_raw = load_tx(TX_PATH)


# ---------- 3. SIDEBAR FILTERS ----------
# ---------- SIDEBAR: REFRESH DATA ----------
st.sidebar.subheader("⚙️ Data Controls")

auto_on = st.sidebar.checkbox("Auto-refresh", value=True)
refresh_sec = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)

auto_gen = st.sidebar.checkbox("Auto-generate live data", value=False)
gen_every = st.sidebar.slider("Generate every (seconds)", 10, 120, 30)

# Start the auto-refresh loop (re-runs the app)
if auto_on:
    st_autorefresh(interval=refresh_sec * 1000, key="auto_refresh")

# --- Safe throttling: only generate once per gen_every seconds ---
if "last_gen_ts" not in st.session_state:
    st.session_state.last_gen_ts = 0.0

def run_pipeline():
    sim_script = PROJECT_ROOT / "src" / "simulate_transactions.py"
    conv_script = PROJECT_ROOT / "src" / "convert_online_retail.py"
    rfm_script = PROJECT_ROOT / "src" / "rfm_segmentation.py"

    r_sim = subprocess.run([sys.executable, str(sim_script)], capture_output=True, text=True)
    r_conv = subprocess.run([sys.executable, str(conv_script)], capture_output=True, text=True)
    r_rfm = subprocess.run([sys.executable, str(rfm_script)], capture_output=True, text=True)

    return r_sim, r_conv, r_rfm

# Auto-generate (only when enabled + enough time has passed)
now = time.time()
if auto_on and auto_gen and (now - st.session_state.last_gen_ts) >= gen_every:
    st.session_state.last_gen_ts = now
    with st.spinner("Auto-generating live data + rebuilding metrics..."):
        r_sim, r_conv, r_rfm = run_pipeline()

    if r_sim.returncode != 0 or r_conv.returncode != 0 or r_rfm.returncode != 0:
        st.error("❌ Auto-generate failed. Showing errors below.")
        st.write("Simulate stderr:"); st.code(r_sim.stderr)
        st.write("Convert stderr:"); st.code(r_conv.stderr)
        st.write("RFM stderr:"); st.code(r_rfm.stderr)


if st.sidebar.button(" Refresh data from raw file"):
    with st.spinner("Refreshing data... this may take a few seconds"):
        # Paths to your scripts
        conv_script = PROJECT_ROOT / "src" / "convert_online_retail.py"
        rfm_script = PROJECT_ROOT / "src" / "rfm_segmentation.py"

        # Run the ETL script
        r1 = subprocess.run(
            [sys.executable, str(conv_script)],
            capture_output=True,
            text=True,
        )
        # Run the RFM script
        r2 = subprocess.run(
            [sys.executable, str(rfm_script)],
            capture_output=True,
            text=True,
        )

        if r1.returncode != 0 or r2.returncode != 0:
            st.error("There was an error while refreshing data.")
            st.write("ETL stderr:")
            st.code(r1.stderr)
            st.write("RFM stderr:")
            st.code(r2.stderr)
        else:
            st.success(" Data refreshed successfully! Reloading dashboard...")
            st.rerun()


st.sidebar.header("Filters")
view_mode = st.sidebar.radio(
    "View",
    ["Recent (recommended)", "Full history"],
    index=0
)

min_d = df_daily_all["d"].min()
max_d = df_daily_all["d"].max()

start_date = st.sidebar.date_input(
    "Start date",
    value=min_d,
    min_value=min_d,
    max_value=max_d,
)

end_date = st.sidebar.date_input(
    "End date",
    value=max_d,
    min_value=min_d,
    max_value=max_d,
)

# Safety: if user picks end before start, swap them
if end_date < start_date:
    start_date, end_date = end_date, start_date


countries = ["All"] + sorted(df_raw["Country"].unique().tolist())
country_selected = st.sidebar.selectbox("Country", countries)

# ---------- 4. APPLY FILTERS TO RAW DATA ----------
mask = (df_raw["InvoiceDate_date"] >= start_date) & (df_raw["InvoiceDate_date"] <= end_date)

if country_selected != "All":
    mask &= df_raw["Country"] == country_selected

df_raw_f = df_raw.loc[mask].copy()

# ✅ Trend data comes from parquet (includes 2025 live day)
df_daily_f = df_daily_all[
    (df_daily_all["d"] >= start_date) & (df_daily_all["d"] <= end_date)
].copy()

# ✅ Make it look like a real-time dashboard (avoid long empty gaps)
if view_mode == "Recent (recommended)" and not df_daily_f.empty:
    df_daily_f["d_dt"] = pd.to_datetime(df_daily_f["d"])
    last_day = df_daily_f["d_dt"].max()
    cutoff = last_day - pd.Timedelta(days=90)
    df_daily_f = df_daily_f[df_daily_f["d_dt"] >= cutoff].copy()
    df_daily_f["d"] = df_daily_f["d_dt"].dt.date
    df_daily_f = df_daily_f.drop(columns=["d_dt"])


# ---------- 5. KPIs ----------
with tab1:

    total_rev = df_raw_f["line_total"].sum()
    avg_rev = df_daily_f["revenue"].mean() if not df_daily_f.empty else 0

    if not df_daily_f.empty:
        peak_row = df_daily_f.loc[df_daily_f["revenue"].idxmax()]
        peak_day = peak_row["d"]
    else:
        peak_day = None

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Revenue", f"${total_rev:,.0f}")
    c2.metric("Avg Daily Revenue", f"${avg_rev:,.0f}" if avg_rev else "N/A")
    c3.metric("Peak Day", str(peak_day) if peak_day else "N/A")


    # ---------- 6. TREND CHART ----------
    st.subheader("📈 Daily Revenue Trend")

    if not df_daily_f.empty:
        df_daily_f["7d_avg"] = df_daily_f["revenue"].rolling(7).mean()

        df_plot = df_daily_f.copy()
        df_plot["d"] = pd.to_datetime(df_plot["d"])

        full_range = pd.date_range(df_plot["d"].min(), df_plot["d"].max(), freq="D")
        df_plot = (
            df_plot.set_index("d")
                .reindex(full_range)
                .rename_axis("d")
                .reset_index()
        )

        df_plot["7d_avg"] = df_plot["revenue"].rolling(7).mean()

        fig = px.line(
            df_plot,
            x="d",
            y=["revenue", "7d_avg"],
            title="Revenue Trend (Daily vs 7-Day Average)",
            template="plotly_white"
        )
        fig.update_traces(mode="lines+markers")
        fig.update_layout(hovermode="x unified")

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for the selected filters.")



    # ---------- 7. TOP COUNTRIES ----------
    st.subheader("Top 5 Countries by Total Revenue")

    if not df_raw_f.empty:
        if country_selected == "All":
            top_countries = (
                df_raw_f.groupby("Country")["line_total"]
                      .sum()
                      .sort_values(ascending=False)
                      .head(5)
                      .reset_index()
            )
        else:
            top_countries = (
                df_raw_f.groupby("Country")["line_total"]
                      .sum()
                      .reset_index()
            )

        st.bar_chart(top_countries.set_index("Country"))
    else:
        st.info("No data for the selected filters / country.")
    

        # ---------- LIVE ZOOM (LAST 7 DAYS) ----------
    st.subheader("⚡ Live Zoom (Last 7 Days)")

    df_live_zoom = df_daily_all.copy()
    df_live_zoom["d"] = pd.to_datetime(df_live_zoom["d"])

    last_day = df_live_zoom["d"].max()
    df_live_zoom = df_live_zoom[
        df_live_zoom["d"] >= (last_day - pd.Timedelta(days=7))
    ].copy()

    df_live_zoom["7d_avg"] = df_live_zoom["revenue"].rolling(7).mean()

    fig2 = px.line(
        df_live_zoom,
        x="d",
        y=["revenue", "7d_avg"],
        title="Live Revenue (Last 7 Days)",
        template="plotly_white"
    )

    fig2.update_traces(mode="lines+markers")
    fig2.update_layout(hovermode="x unified")

    st.plotly_chart(fig2, use_container_width=True)



with tab2:
    st.header("🧩 RFM Customer Segments")

    rfm_path = PROJECT_ROOT / "data" / "processed" / "rfm_segments.parquet"

    if rfm_path.exists():
        rfm = pd.read_parquet(rfm_path).reset_index()

        st.subheader("Customer distribution by segment")
        seg = rfm["Segment"].value_counts().reset_index()
        seg.columns = ["Segment", "Customers"]
        st.bar_chart(seg.set_index("Segment"))

        st.subheader("Top 20 customers by revenue")
        st.dataframe(
            rfm.sort_values("Monetary", ascending=False)[
                ["CustomerID", "Recency", "Frequency", "Monetary", "Segment"]
            ].head(20),
            use_container_width=True
        )
    else:
        st.warning("Run rfm_segmentation.py first")

with tab3:
    st.header("⚡ Live Transaction Feed")

    if LIVE_PATH.exists():
        live = pd.read_parquet(LIVE_PATH)
        live["InvoiceDate"] = pd.to_datetime(live["InvoiceDate"], errors="coerce")
        live = live.sort_values("InvoiceDate", ascending=False)

        st.metric("Live transactions", f"{len(live):,}")
        st.dataframe(
            live[["Invoice", "InvoiceDate", "StockCode", "Quantity", "Price", "Country"]]
            .head(20),
            use_container_width=True
        )
    else:
        st.info("No live data yet")

