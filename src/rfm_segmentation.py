from pathlib import Path
import pandas as pd

# ---------- Paths ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]   # .../smartretail-insights
RAW_EXCEL = PROJECT_ROOT / "data" / "raw" / "online_retail_II.xlsx"
OUTDIR = PROJECT_ROOT / "data" / "processed"
OUTDIR.mkdir(parents=True, exist_ok=True)

print("📂 Project root:", PROJECT_ROOT)
print("📂 Raw Excel:", RAW_EXCEL)

# ---------- Load raw data (both sheets) ----------
xls = pd.ExcelFile(RAW_EXCEL)
df1 = pd.read_excel(xls, sheet_name=0)
df2 = pd.read_excel(xls, sheet_name=1)
df = pd.concat([df1, df2], ignore_index=True)

# Columns: Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country

# ---------- Basic clean ----------
df = df.dropna(subset=["InvoiceDate", "Quantity", "Price", "Customer ID"])
df = df[(df["Quantity"] > 0) & (df["Price"] > 0)].copy()

df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
df = df.dropna(subset=["InvoiceDate"])

# Make CustomerID a clean string
df["CustomerID"] = df["Customer ID"].astype(str)

# Revenue per line
df["line_total"] = df["Quantity"] * df["Price"]

# ---------- RFM calculation ----------
# Recency: days since last purchase (lower is better)
analysis_date = df["InvoiceDate"].max() + pd.Timedelta(days=1)

rfm = df.groupby("CustomerID").agg(
    Recency=("InvoiceDate", lambda x: (analysis_date - x.max()).days),
    Frequency=("Invoice", "nunique"),
    Monetary=("line_total", "sum"),
)

# ---------- Score each dimension 1–5 ----------
# For Recency, lower is better → reverse labels
rfm["R_score"] = pd.qcut(rfm["Recency"], 5, labels=[5, 4, 3, 2, 1])

# For Frequency and Monetary, higher is better
rfm["F_score"] = pd.qcut(
    rfm["Frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]
)
rfm["M_score"] = pd.qcut(
    rfm["Monetary"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]
)

rfm["RFM_score"] = (
    rfm["R_score"].astype(str)
    + rfm["F_score"].astype(str)
    + rfm["M_score"].astype(str)
)

# ---------- Segment mapping ----------
def segment_row(row):
    r = int(row["R_score"])
    f = int(row["F_score"])
    m = int(row["M_score"])

    if r >= 4 and f >= 4 and m >= 4:
        return "Champions"
    elif r >= 3 and f >= 3 and m >= 3:
        return "Loyal"
    elif r >= 4 and f <= 2:
        return "New Customers"
    elif r <= 2 and f >= 3 and m >= 3:
        return "At Risk"
    elif m >= 4 and f <= 2:
        return "Big Spenders"
    else:
        return "Others"

rfm["Segment"] = rfm.apply(segment_row, axis=1)

# ---------- Save ----------
rfm.sort_values("Monetary", ascending=False, inplace=True)

out_file = OUTDIR / "rfm_segments.parquet"
rfm.to_parquet(out_file)

print("✅ Saved RFM segments to:", out_file)
print("🔎 Sample:")
print(rfm.head())
