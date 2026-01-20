# SmartRetail Insights — Real-Time Retail Analytics Dashboard  
*(Streamlit + Python)*

SmartRetail Insights is an end-to-end retail analytics project built on the **Online Retail II** dataset.

It demonstrates a complete analytics workflow:

**Raw data → ETL → Parquet analytics tables → Interactive dashboard**,  
with a **simulated real-time transaction stream** and **auto-refresh monitoring views**.

---

## 🚀 What This Project Demonstrates

- Reproducible analytics pipeline (ETL + data quality checks)
- Conversion of raw transaction files into analytics-ready **Parquet** datasets
- Time-series revenue reporting (daily trends + rolling averages)
- Customer analytics using **RFM segmentation**
- A **“live dashboard” pattern**: simulated incremental ingestion + refreshable KPIs

---

## ✨ Key Features

### 📊 Dashboard
- KPI cards: **Total Revenue**, **Average Daily Revenue**, **Peak Day**
- Revenue trend chart (daily revenue + 7-day moving average)
- Top countries by total revenue
- Monitoring-friendly **Live View** (last 7–14 days)

### ⚡ Real-Time Simulation
- Generates incremental **live transactions**
- Appends data to a Parquet store
- Rebuilds aggregated tables and customer segments
- Auto-refresh and optional auto-generate controls

### 🧩 Customer Segmentation (RFM)
- Recency, Frequency, Monetary calculations at customer level
- Segment labels: **Champions, Loyal, At Risk, New Customers**, etc.
- Segment distribution charts and top-customer views

---

## 🗂 Project Structure

```text
smartretail-insights/
├── streamlit_app/
│   └── app.py                      # Streamlit dashboard (tabs + charts)
├── src/
│   ├── convert_online_retail.py     # ETL: raw → parquet tables
│   ├── simulate_transactions.py     # Simulated live ingestion
│   └── rfm_segmentation.py          # Customer RFM segmentation
├── data/
│   ├── raw/                         # Raw dataset (not committed)
│   └── processed/                   # Parquet outputs (not committed)
├── assets/                          # Screenshots for README (commit these)
├── requirements.txt
└── README.md
