from pathlib import Path
import pandas as pd
import numpy as np

# ---------- Paths ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]   # .../smartretail-insights
RAW_EXCEL = PROJECT_ROOT / "data" / "raw" / "online_retail_II.xlsx"
OUTDIR = PROJECT_ROOT / "data" / "processed"
OUTDIR.mkdir(parents=True, exist_ok=True)

LIVE_PATH = OUTDIR / "live_transactions.parquet"

print("📂 Project root:", PROJECT_ROOT)
print("📂 Raw Excel:", RAW_EXCEL)
print("📂 Live file:", LIVE_PATH)

# ---------- Load & clean base data ----------
xls = pd.ExcelFile(RAW_EXCEL)
df1 = pd.read_excel(xls, sheet_name=0)
df2 = pd.read_excel(xls, sheet_name=1)
df = pd.concat([df1, df2], ignore_index=True)

# Columns: Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country

df = df.dropna(subset=["InvoiceDate", "Quantity", "Price", "Customer ID"])
df = df[(df["Quantity"] > 0) & (df["Price"] > 0)].copy()

df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
df = df.dropna(subset=["InvoiceDate"])

# Ensure StockCode is always treated as text
df["StockCode"] = df["StockCode"].astype(str)

df["CustomerID"] = df["Customer ID"].astype(str)
df["line_total"] = df["Quantity"] * df["Price"]



# ---------- Simulate new "today" transactions ----------
# How many new rows to create each time?
N_NEW = 200

if len(df) < N_NEW:
    N_NEW = len(df)

sample = df.sample(n=N_NEW, replace=True).copy()

# Set InvoiceDate across last 7 days with random times
base_day = pd.Timestamp.today().normalize()
day_offsets = np.random.randint(0, 7, size=N_NEW)  # 0..6 days back
rand_minutes = np.random.randint(0, 24 * 60, size=N_NEW)

sample["InvoiceDate"] = (
    base_day
    - pd.to_timedelta(day_offsets, unit="D")
    + pd.to_timedelta(rand_minutes, unit="m")
)


# Mark invoices as LIVE-XXXX
sample["Invoice"] = [
    f"LIVE-{i}" for i in range(1, N_NEW + 1)
]

# ---------- Append to existing live file ----------
if LIVE_PATH.exists():
    existing = pd.read_parquet(LIVE_PATH)
    combined = pd.concat([existing, sample], ignore_index=True)
else:
    combined = sample

# Make sure StockCode stays as string before saving
combined["StockCode"] = combined["StockCode"].astype(str)

combined.to_parquet(LIVE_PATH)


print(f"✅ Added {N_NEW} new simulated transactions for {today.date()}")
print("📈 Live file now has:", len(combined), "rows")
