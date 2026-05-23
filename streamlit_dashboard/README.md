# E-Commerce Analytics — Streamlit Dashboard

Real-time + batch analytics over your BigQuery Gold layer.

## Setup

```bash
pip install -r requirements.txt
```

Set your project ID and service account path at the top of `dashboard.py`:

```python
PROJECT_ID       = "your-gcp-project-id"
SERVICE_ACCT_KEY = "config/gcp_service_account.json"
```

Place your GCP service account JSON at `config/gcp_service_account.json`,
or delete `SERVICE_ACCT_KEY` and let the client fall back to
Application Default Credentials (`gcloud auth application-default login`).

## Run

```bash
streamlit run dashboard.py
```

## Key architecture decisions

### Why no page reload on live data?

All layout (headers, column structure, section labels) is rendered ONCE
before the `while True` loop starts. Only `st.empty()` placeholder
containers are updated inside the loop. This means:

- The page structure never re-renders.
- Only the data inside each chart/metric refreshes.
- `time.sleep(N)` in the loop controls the poll interval.

```python
# Layout rendered ONCE (outside loop)
ph_kpi = st.empty()
ph_chart = st.empty()

# Loop only updates the placeholders
while True:
    df = run_query(sql)
    ph_kpi.metric("Orders", df.iloc[0]["orders"])
    ph_chart.plotly_chart(make_chart(df))
    time.sleep(30)
```

### Why NOT `st.rerun()`?

`st.rerun()` re-executes the entire script from top to bottom, including
all layout code — which causes the visible "flash" / full-page reload.
`st.empty()` avoids this by updating only the placeholder's content.

### How are tables joined?

All joins happen in **BigQuery SQL**, not in Python. The query returns a
flat DataFrame that Streamlit just renders. Example:

```sql
-- fact_orders JOIN dim_payment_method
SELECT dp.payment_method_name, COUNT(fo.order_id) AS orders
FROM fact_orders fo
JOIN dim_payment_method dp ON dp.payment_method_code = fo.payment_method
GROUP BY 1
```

Calling `pd.merge()` in Python is also fine for small tables, but always
prefer SQL joins for anything that touches BigQuery — the engine is fast,
and you avoid pulling two large tables into memory.

### Streaming vs Batch — separate queries

The streaming panel queries `fact_orders_stream`, `fact_user_events`, etc.
The batch panel queries `fact_sales`, `fact_orders`, and dim tables.

These are intentionally kept separate because:
- Streaming tables have at-least-once delivery (Kafka) — can double-count.
- Batch tables are deduplicated and reconciled nightly.
- Mixing them on the same report page would inflate revenue numbers.

## Folder structure

```
streamlit_dashboard/
├── dashboard.py        ← main app
├── requirements.txt
├── config/
│   └── gcp_service_account.json   ← your key (gitignored)
└── README.md
```

## Deploy to Cloud Run (optional)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["streamlit", "run", "dashboard.py",
     "--server.port=8080",
     "--server.address=0.0.0.0",
     "--server.headless=true"]
```

```bash
gcloud run deploy ecommerce-dashboard \
  --source . \
  --region asia-southeast1 \
  --allow-unauthenticated
```
