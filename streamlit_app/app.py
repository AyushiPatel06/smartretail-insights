import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import date

st.title("📊 SmartRetail Insights — Daily Revenue (Kaggle)")

# ---------- 1. PATHS (don't modify) ----------
APP_DIR = Path(__file__).resolve().parent          # .../smartretail-insights/streamlit_app
PROJECT_ROOT = APP_DIR.parent                      # .../smartretail-insights
DAILY_PATH = PROJECT_ROOT / "data" / "processed" / "daily_revenue.parquet"
RAW_EXCEL  = PROJECT_ROOT / "data" / "raw" / "online_retail_II.xlsx"

# ---------- 2. LOAD DATA ----------
# Daily revenue from processed parquet
df_daily = pd.read_parquet(DAILY_PATH).sort_values("d")

# Raw Excel for country + detailed filters
xls = pd.ExcelFile(RAW_EXCEL)
df_raw = pd.concat(
    [
        pd.read_excel(xls, sheet_name=0),
        pd.read_excel(xls, sheet_name=1),
    ],
    ignore_index=True,
)

# Basic clean on raw
df_raw = df_raw.dropna(subset=["InvoiceDate", "Quantity", "Price", "Country"])
df_raw = df_raw[(df_raw["Quantity"] > 0) & (df_raw["Price"] > 0)].copy()
df_raw["InvoiceDate"] = pd.to_datetime(df_raw["InvoiceDate"], errors="coerce")
df_raw = df_raw.dropna(subset=["InvoiceDate"])
df_raw["line_total"] = df_raw["Quantity"] * df_raw["Price"]

# ---------- 3. SIDEBAR FILTERS ----------
st.sidebar.header("🔍 Filters")

# Date range
min_d = df_daily["d"].min()
max_d = df_daily["d"].max()

start_date, end_date = st.sidebar.date_input(
    "Date range",
    value=(min_d, max_d),
    min_value=min_d,
    max_value=max_d,
)

# Country filter
countries = ["All"] + sorted(df_raw["Country"].unique().tolist())
country_selected = st.sidebar.selectbox("Country", countries)

# ---------- 4. APPLY FILTERS ----------
# Filter daily df
mask_daily = (df_daily["d"] >= start_date) & (df_daily["d"] <= end_date)
df_daily_f = df_daily.loc[mask_daily].copy()

# Filter raw df by date
df_raw["InvoiceDate_date"] = df_raw["InvoiceDate"].dt.date
mask_raw = (df_raw["InvoiceDate_date"] >= start_date) & (df_raw["InvoiceDate_date"] <= end_date)
df_raw_f = df_raw.loc[mask_raw].copy()

# Filter by country (if not "All")
if country_selected != "All":
    df_raw_f = df_raw_f[df_raw_f["Country"] == country_selected]

# ---------- 5. KPIs ----------
total_rev = df_raw_f["line_total"].sum()
avg_rev = df_daily_f["revenue"].mean()
peak_row = df_daily_f.loc[df_daily_f["revenue"].idxmax()] if not df_daily_f.empty else None
peak_day = peak_row["d"] if peak_row is not None else None

c1, c2, c3 = st.columns(3)
c1.metric("💰 Total Revenue", f"${total_rev:,.0f}")
c2.metric("📅 Avg Daily Revenue", f"${avg_rev:,.0f}" if pd.notna(avg_rev) else "N/A")
c3.metric("🚀 Peak Day", str(peak_day) if peak_day is not None else "N/A")

# ---------- 6. TREND CHART ----------
df_daily_f["7d_avg"] = df_daily_f["revenue"].rolling(7).mean()

st.subheader("📈 Daily Revenue Trend")
st.line_chart(df_daily_f.set_index("d")[["revenue", "7d_avg"]])

# ---------- 7. TOP COUNTRIES ----------
st.subheader("🌍 Top 5 Countries by Total Revenue")

if not df_raw_f.empty:
    top_countries = (
        df_raw_f.groupby("Country")["line_total"]
                .sum()
                .sort_values(ascending=False)
                .head(5)
                .reset_index()
    )
    st.bar_chart(top_countries.set_index("Country"))
else:
    st.info("No data for the selected filters.")

st.caption("Data: Online Retail II (Kaggle). Cleaned with pandas, visualized in Streamlit.")
