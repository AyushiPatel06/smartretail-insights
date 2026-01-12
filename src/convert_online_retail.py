from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]  # .../smartretail-insights
RAW = ROOT / "data" / "raw" / "online_retail_II.xlsx"
OUTDIR = ROOT / "data" / "processed"
OUTDIR.mkdir(parents=True, exist_ok=True)

LIVE_PATH = OUTDIR / "live_transactions.parquet"

use_cols = ["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country"]

xls = pd.ExcelFile(RAW)
base = pd.concat([
    pd.read_excel(xls, sheet_name=0, usecols=use_cols),
    pd.read_excel(xls, sheet_name=1, usecols=use_cols),
], ignore_index=True)

df = base

if LIVE_PATH.exists():
    live = pd.read_parquet(LIVE_PATH)

    # Ensure live has the exact base columns
    live = live[["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country"]].copy()

    df = pd.concat([df, live], ignore_index=True)
    print("🔄 Included live transactions:", len(live))
else:
    print("ℹ️ No live transactions file found. Using base data only.")

# Standardize names
df = df.rename(columns={
    "Invoice": "InvoiceNo",
    "Customer ID": "CustomerID",
    "Price": "UnitPrice",
})

# Remove duplicate columns, just in case
df = df.loc[:, ~df.columns.duplicated()].copy()

# Parse types BEFORE filtering
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")
df["StockCode"] = df["StockCode"].astype(str)

print("COMBINED max InvoiceDate (before filter):", df["InvoiceDate"].max())
print("Rows with year >= 2020 (before filter):", (df["InvoiceDate"].dt.year >= 2020).sum())

# Clean
df = df.dropna(subset=["InvoiceDate", "Quantity", "UnitPrice"])
df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)].copy()

df["line_total"] = df["Quantity"] * df["UnitPrice"]

daily = (
    df.assign(d=df["InvoiceDate"].dt.floor("D"))
      .groupby("d", as_index=False)["line_total"]
      .sum()
      .sort_values("d")
)

out = OUTDIR / "daily_revenue.parquet"
daily.to_parquet(out)

print("✅ Wrote:", out)
print(daily.tail(10))
