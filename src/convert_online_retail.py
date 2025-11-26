from pathlib import Path
import pandas as pd

RAW = Path("data/raw/online_retail_II.xlsx")
OUTDIR = Path("data/processed"); OUTDIR.mkdir(parents=True, exist_ok=True)

# Load only needed columns to keep it snappy
use_cols = ["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country"]

xls = pd.ExcelFile(RAW)

# Read both sheets (Online Retail II has 2 years)
df1 = pd.read_excel(xls, sheet_name=0, usecols=use_cols)
df2 = pd.read_excel(xls, sheet_name=1, usecols=use_cols)
df = pd.concat([df1, df2], ignore_index=True)

# Standardize column names
df = df.rename(columns={
    "Invoice": "InvoiceNo",
    "Customer ID": "CustomerID",
    "Price": "UnitPrice"          # <-- important: map Price -> UnitPrice
})

# Clean
df = df.dropna(subset=["InvoiceDate", "Quantity", "UnitPrice"])
df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)].copy()
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
df = df.dropna(subset=["InvoiceDate"])
df["line_total"] = df["Quantity"] * df["UnitPrice"]

# Daily revenue
daily = (
    df.groupby(df["InvoiceDate"].dt.date)["line_total"]
      .sum()
      .reset_index()
      .rename(columns={"InvoiceDate": "d", "line_total": "revenue"})
      .sort_values("d")
)

out = OUTDIR / "daily_revenue.parquet"
daily.to_parquet(out)
print("✅ Wrote:", out)
print(daily.head())
