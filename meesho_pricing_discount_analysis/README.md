# Pricing & Discount Effectiveness Analysis (Meesho BA Portfolio)

Analyze how discounts impact GMV, conversion, and ROI. Includes:
- Synthetic data generator (`data_gen.py`)
- Streamlit app (`app.py`) with filters, KPIs, ROI proxy, A/B testing from exposures
- SQL playground + sample queries
- SQLite DB + CSVs

## 1) Quick Start (Local)

```bash
# 1. Create virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # (Windows) .venv\Scripts\activate

# 2. Install deps
pip install -r requirements.txt

# 3. (Optional) Regenerate data
python data_gen.py

# 4. Run the app
streamlit run app.py
```

The app will open at http://localhost:8501

## 2) Streamlit Cloud Deployment

1. Push this folder to a public GitHub repo.
2. In Streamlit Cloud, set **Main file path**: `app.py`
3. Add **Python version** (3.10+) and **requirements.txt**
4. Deploy. Done!

## 3) Data Description

### `campaigns.csv`
- `campaign_id` (int), `campaign_type`, `start_date`, `end_date`, `name`

### `transactions.csv`
- `order_id`, `customer_id`, `product_id`, `order_date`, `category`
- `original_price`, `discount_percent`, `final_price`, `campaign_id`
- Derived: `discount_amount = original_price - final_price`, `discount_bracket`

### `exposures.csv`
- `campaign_id`, `campaign_type`, `discount_bracket`
- `impressions`, `conversions`, `start_date`, `end_date`

> **Why exposures?** Enables true **conversion A/B testing** and significance checks.

## 4) ROI Proxy vs True Incremental ROI

- **ROI Proxy (in app):** `(GMV/order − Discount/order) ÷ Discount/order` — simple, comparable signal across brackets.
- **True Incremental ROI:** requires controlled experiments (A/B) measuring **lift** vs baseline. Use the **A/B test** section in the app with `exposures.csv` to evaluate statistical significance.

## 5) Example SQL (see `sql/queries.sql`)

- GMV by discount bracket
- Campaign-wise revenue & average discount
- Category leaders within 11–20% bracket

## 6) Customize

- Change synthetic distributions in `data_gen.py` (categories, price ranges, discount probabilities).
- Add a **PySpark** pipeline in a separate module if you want to showcase big-data handling.
- Extend the app with: margin curves, elasticity charts, or per-region analysis.

## 7) Resume Bullets (example)

- Built and deployed a Streamlit app to analyze discount effectiveness on 200K+ synthetic transactions; identified **11–20%** as optimal bracket by ROI proxy and validated uplift via **two-proportion z-test** on exposures.
- Implemented SQL KPIs & interactive dashboards, enabling **campaign ROI** comparison and **A/B testing**.
- Packaged dataset and SQL playground (SQLite) for reproducible insights; productionized with clear README and tests.


---

## 8) PySpark Batch (Big-Data Flavor)

```bash
# In this folder
pip install pyspark
python spark_job.py --input_dir . --output_dir ./spark_out
ls spark_out/agg_by_discount_bracket  # Parquet outputs
```

**What it does:** Computes bracket-level and campaign-level aggregates in **distributed** mode (Spark), ready for data lakes / Parquet.

## 9) Postgres Integration

```bash
pip install sqlalchemy psycopg2-binary python-dotenv
cp .env.example .env  # edit credentials
export $(grep -v '^#' .env | xargs)  # Windows: setx DATABASE_URL "..."
python db_loader.py --input_dir .    # loads CSVs into Postgres
streamlit run app_pg.py              # Postgres-backed app
```

### Example connection string
`DATABASE_URL=postgresql+psycopg2://user:pass@localhost:5432/meesho_pricing`

## 10) BigQuery (Optional)

```bash
pip install google-cloud-bigquery pandas-gbq
# Example Python snippet:
from google.cloud import bigquery
client = bigquery.Client()
client.load_table_from_dataframe(df, "project.dataset.transactions").result()
```

**Tip:** Mirror the same schema (transactions, campaigns, exposures) and point your BI layer to BigQuery views for ROI/CR calculations.


## 11) Unified App Data Source Toggle (CSV/SQLite/Postgres/BigQuery)
The main `app.py` now includes a sidebar data source selector:
- **Sample (CSV)** / **Upload CSVs**
- **Sample (SQLite DB)**
- **Postgres** — set `DATABASE_URL` (e.g., `postgresql+psycopg2://user:pass@host:5432/db`)
- **BigQuery** — set `BQ_PROJECT`, `BQ_DATASET` (or type in the sidebar), and ensure auth is configured.

For Postgres/BigQuery support, ensure extra dependencies are installed (see `requirements.txt`).


---
## 12) Scheduled Refresh Options

### Cron (simple, local)
1. Make the script executable:
   ```bash
   chmod +x refresh_data.sh
   ```
2. Add to crontab (daily at 02:30):
   ```bash
   crontab -e
   # then add
   30 2 * * * /bin/bash -lc "cd /ABS/PATH/meesho_pricing_discount_analysis && ./refresh_data.sh >> refresh.log 2>&1"
   ```

### Airflow (orchestrated)
- Place `dags/discount_effectiveness_refresh.py` in your Airflow `dags/` folder.
- Set Airflow Variable `DATABASE_URL` (Admin → Variables).
- Trigger manually with `dag_run.conf={"repo_path": "/ABS/PATH/meesho_pricing_discount_analysis"}` or let it run on its cron.

## 13) BI Templates (Superset & Metabase)

### Superset
- Install Superset and connect your Postgres DB (transactions/campaigns/exposures).
- Import `superset/superset_dashboard.yaml` via **Settings → Import**.
- Adjust the database URI if needed.

### Metabase
- Go to **Settings → Admin → Collections → Import** (or use API).
- Import `metabase/metabase_collection.json` and point it to your Postgres database (id may vary).



---
## 14) Badges & One-click Deploy

![Built with Streamlit](https://img.shields.io/badge/Built%20with-Streamlit-red)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Made for BA Portfolios](https://img.shields.io/badge/Use-BA%20Portfolio-brightgreen)

