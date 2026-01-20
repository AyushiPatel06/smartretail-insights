# SmartRetail Insights
# SmartRetail Insights — Real-Time Retail Analytics Dashboard (Streamlit + Python)

SmartRetail Insights is an end-to-end retail analytics project built on the **Online Retail II** dataset.  
It demonstrates a full analytics workflow: **raw data → ETL → Parquet analytics tables → interactive dashboard**, with a **simulated real-time transaction stream** and **auto-refresh** monitoring view.

---

## What this project demonstrates
- Building a reproducible analytics pipeline (ETL + data quality checks)
- Converting raw transaction files into analytics-ready **Parquet** datasets
- Time-series revenue reporting (daily trends + moving averages)
- Customer analytics (**RFM segmentation**)
- A “live” dashboard pattern: simulated incremental ingestion + refreshable KPIs/visuals

---

## Key Features
### Dashboard
- KPI cards: Total Revenue, Avg Daily Revenue, Peak Day
- Revenue trend chart (daily + 7-day moving average)
- Top countries by total revenue
- “Live” view (last 7–14 days) designed for monitoring

### Real-time simulation
- Generates incremental “live transactions” and appends them to a Parquet store
- Rebuilds aggregated tables and segments
- Auto-refresh + optional auto-generate controls

### Customer segmentation (RFM)
- Recency, Frequency, Monetary calculations at customer level
- Segment labels such as Champions, Loyal, At Risk, etc.
- Segment distribution visualizations + top customers

---

## Project Structure
```text
smartretail-insights/
├── streamlit_app/
│   └── app.py                      # Streamlit dashboard (tabs + charts)
├── src/
│   ├── convert_online_retail.py     # ETL: raw → parquet tables (daily + transactions)
│   ├── simulate_transactions.py     # Simulated live ingestion (incremental data)
│   └── rfm_segmentation.py          # Customer RFM segmentation
├── data/
│   ├── raw/                         # Raw dataset (not committed)
│   └── processed/                   # Parquet outputs (not committed)
├── assets/                          # Screenshots for README (commit these) 
├── requirements.txt
└── README.md


## 📸 Screenshots

### 🔹 Dashboard Overview
![Dashboard Overview](assets/dashboard_overview.png)

### 🔹 Historical Revenue (2009–2011)
![Historical Revenue](assets/historical_revenue_2009_2011.png)

### 🔹 Live Revenue Trend (Last 7 Days)
![Live Revenue](assets/live_revenue_last_7_days.png)

### 🔹 RFM Customer Segments
![RFM Segments](assets/rfm_customer_segments.png)

### 🔹 Live Transaction Feed
![Live Transactions](assets/live_transaction_feed.png)


