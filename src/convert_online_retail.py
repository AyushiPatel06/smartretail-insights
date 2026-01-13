from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]  # .../smartretail-insights
RAW = ROOT / "data" / "raw" / "online_retail_II.xlsx"
OUTDIR = ROOT / "data" / "processed"
OUTDIR.mkdir(parents=True, exist_ok=True)

LIVE_PATH = OUTDIR / "live_transactions.parquet"

# Load only needed columns
use_cols = ["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country"]

xls = pd.ExcelFile(RAW)
df1 = pd.read_excel(xls, sheet_name=0, usecols=use_cols)
df2 = pd.read_excel(xls, sheet_name=1, usecols=use_cols)
df = pd.concat([df1, df2], ignore_index=True)

# Append live transactions if present
if LIVE_PATH.exists():
    live_df = pd.read_parquet(LIVE_PATH)

    # Keep schema compatible with base
    live_df = live_df[["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country"]].copy()

    df = pd.concat([df, live_df], ignore_index=True)
    print("🔄 Included live transactions:", len(live_df))
else:
    print("ℹ️ No live transactions file found. Using base data only.")

# Standardize column names
df = df.rename(columns={
    "Invoice": "InvoiceNo",
    "Customer ID": "CustomerID",
    "Price": "UnitPrice",
})

# Remove duplicate column names (safety)
df = df.loc[:, ~df.columns.duplicated()].copy()

# Parse types BEFORE filtering (important)
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")
df["StockCode"] = df["StockCode"].astype(str)

# Debug proof
print("COMBINED max InvoiceDate (before filter):", df["InvoiceDate"].max())
print("Rows with year >= 2020 (before filter):", (df["InvoiceDate"].dt.year >= 2020).sum())

# Clean
df = df.dropna(subset=["InvoiceDate", "Quantity", "UnitPrice"])
df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)].copy()

# Line total
df["line_total"] = df["Quantity"] * df["UnitPrice"]

# ✅ Save cleaned transactions for fast dashboard queries (no Excel in Streamlit)
tx_out = OUTDIR / "transactions_clean.parquet"

# Ensure IDs are strings (invoice numbers can be like 'C496350')
df["InvoiceNo"] = df["InvoiceNo"].astype(str)
df["CustomerID"] = df["CustomerID"].astype(str)


df[[
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country", "line_total"
]].to_parquet(tx_out)
print("✅ Wrote:", tx_out)


# Daily revenue (robust datetime day bucket)
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
