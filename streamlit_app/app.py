import streamlit as st
import pandas as pd
from pathlib import Path

st.title("📊 SmartRetail Insights — Daily Revenue (Kaggle)")

# ---------- 1. PATHS ----------
APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent
DAILY_PATH = PROJECT_ROOT / "data" / "processed" / "daily_revenue.parquet"
RAW_EXCEL  = PROJECT_ROOT / "data" / "raw" / "online_retail_II.xlsx"

# ---------- 2. LOAD DATA ----------
# Pre-aggregated daily data (for date defaults)
df_daily_all = pd.read_parquet(DAILY_PATH).sort_values("d")
# Ensure d is a date
df_daily_all["d"] = pd.to_datetime(df_daily_all["d"]).dt.date

# Raw Excel for detailed filtering
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
df_raw["InvoiceDate_date"] = df_raw["InvoiceDate"].dt.date
df_raw["line_total"] = df_raw["Quantity"] * df_raw["Price"]

# ---------- 3. SIDEBAR FILTERS ----------
st.sidebar.header("🔍 Filters")

min_d = df_daily_all["d"].min()
max_d = df_daily_all["d"].max()

start_date, end_date = st.sidebar.date_input(
    "Date range",
    value=(min_d, max_d),
    min_value=min_d,
    max_value=max_d,
)

countries = ["All"] + sorted(df_raw["Country"].unique().tolist())
country_selected = st.sidebar.selectbox("Country", countries)

# ---------- 4. APPLY FILTERS TO RAW DATA ----------
mask = (df_raw["InvoiceDate_date"] >= start_date) & (df_raw["InvoiceDate_date"] <= end_date)

if country_selected != "All":
    mask &= df_raw["Country"] == country_selected

df_raw_f = df_raw.loc[mask].copy()

# Rebuild daily revenue from filtered raw data
if not df_raw_f.empty:
    df_daily_f = (
        df_raw_f
        .groupby("InvoiceDate_date")["line_total"]
        .sum()
        .reset_index()
        .rename(columns={"InvoiceDate_date": "d", "line_total": "revenue"})
        .sort_values("d")
    )
else:
    df_daily_f = pd.DataFrame(columns=["d", "revenue"])

# ---------- 5. KPIs ----------
total_rev = df_raw_f["line_total"].sum()
avg_rev = df_daily_f["revenue"].mean() if not df_daily_f.empty else 0
if not df_daily_f.empty:
    peak_row = df_daily_f.loc[df_daily_f["revenue"].idxmax()]
    peak_day = peak_row["d"]
else:
    peak_day = None

c1, c2, c3 = st.columns(3)
c1.metric("💰 Total Revenue", f"${total_rev:,.0f}")
c2.metric("📅 Avg Daily Revenue", f"${avg_rev:,.0f}" if avg_rev else "N/A")
c3.metric("🚀 Peak Day", str(peak_day) if peak_day else "N/A")

# ---------- 6. TREND CHART ----------
if not df_daily_f.empty:
    df_daily_f["7d_avg"] = df_daily_f["revenue"].rolling(7).mean()
    st.subheader("📈 Daily Revenue Trend")
    st.line_chart(df_daily_f.set_index("d")[["revenue", "7d_avg"]])
else:
    st.subheader("📈 Daily Revenue Trend")
    st.info("No data for the selected filters.")

# ---------- 7. TOP COUNTRIES ----------
st.subheader("🌍 Top 5 Countries by Total Revenue")

if not df_raw_f.empty:
    # If All countries, show top 5; if single country, show just that one
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

st.caption("Data: Online Retail II (Kaggle). Cleaned with pandas, visualized in Streamlit.")
