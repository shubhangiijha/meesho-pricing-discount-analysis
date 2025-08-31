from pathlib import Path

# Base directory (the folder where app.py is located)
APP_DIR = Path(__file__).parent

import os
import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from io import StringIO
from datetime import datetime
from scipy import stats

# Optional imports for external warehouses
try:
    from sqlalchemy import create_engine, text as sa_text
except Exception:
    create_engine = None
    sa_text = None

try:
    from pandas_gbq import read_gbq as gbq_read
except Exception:
    gbq_read = None

st.set_page_config(page_title="Pricing & Discount Effectiveness", layout="wide")
st.title("💸 Pricing & Discount Effectiveness Analysis")
st.caption("Analyze discount impact on GMV, conversion, and ROI. Supports CSV, SQLite, Postgres, and BigQuery.")



@st.cache_data
def load_sample_csv(name):
    # Read CSVs from same folder as app.py
    return pd.read_csv(APP_DIR / name)

@st.cache_data
def load_sqlite(db_path):
    # Read SQLite DB from same folder as app.py
    con = sqlite3.connect(APP_DIR / db_path)
    df_t = pd.read_sql_query("SELECT * FROM transactions", con)
    df_c = pd.read_sql_query("SELECT * FROM campaigns", con)
    df_e = pd.read_sql_query("SELECT * FROM exposures", con)
    con.close()
    return df_t, df_c, df_e


@st.cache_data(ttl=600)
def load_postgres(db_url):
    if create_engine is None:
        raise RuntimeError("SQLAlchemy not installed. Please `pip install sqlalchemy psycopg2-binary`.")
    engine = create_engine(db_url, pool_pre_ping=True)
    with engine.begin() as conn:
        df_t = pd.read_sql(sa_text("SELECT * FROM transactions"), conn)
        df_c = pd.read_sql(sa_text("SELECT * FROM campaigns"), conn)
        try:
            df_e = pd.read_sql(sa_text("SELECT * FROM exposures"), conn)
        except Exception:
            df_e = None
    return df_t, df_c, df_e

@st.cache_data(ttl=600)
def load_bigquery(project=None, dataset=None, table_t="transactions", table_c="campaigns", table_e="exposures"):
    if gbq_read is None:
        raise RuntimeError("pandas-gbq not installed. Please `pip install pandas-gbq google-cloud-bigquery`.")
    if not project or not dataset:
        raise RuntimeError("Set BQ project and dataset.")
    q_t = f"SELECT * FROM `{project}.{dataset}.{table_t}`"
    q_c = f"SELECT * FROM `{project}.{dataset}.{table_c}`"
    q_e = f"SELECT * FROM `{project}.{dataset}.{table_e}`"
    df_t = gbq_read(q_t, project_id=project)
    df_c = gbq_read(q_c, project_id=project)
    try:
        df_e = gbq_read(q_e, project_id=project)
    except Exception:
        df_e = None
    return df_t, df_c, df_e

# --- Data sources ---
st.sidebar.header("Data Source")
use_sample = st.sidebar.selectbox(
    "Choose dataset",
    ["Sample (CSV)", "Upload CSVs", "Sample (SQLite DB)", "Postgres", "BigQuery"],
    index=0
)

df_t = df_c = df_e = None

if use_sample == "Sample (CSV)":
    df_t = load_sample_csv("transactions.csv")
    df_c = load_sample_csv("campaigns.csv")
    df_e = load_sample_csv("exposures.csv")
elif use_sample == "Sample (SQLite DB)":
    df_t, df_c, df_e = load_sqlite("meesho_pricing.db")
elif use_sample == "Upload CSVs":
    t_file = st.sidebar.file_uploader("Upload transactions.csv", type=["csv"])
    c_file = st.sidebar.file_uploader("Upload campaigns.csv", type=["csv"])
    e_file = st.sidebar.file_uploader("Upload exposures.csv (optional)", type=["csv"])
    if t_file is not None and c_file is not None:
        df_t = pd.read_csv(t_file)
        df_c = pd.read_csv(c_file)
        if e_file is not None:
            df_e = pd.read_csv(e_file)
elif use_sample == "Postgres":
    st.sidebar.caption("Set env var DATABASE_URL or paste below.")
    db_url = os.getenv("DATABASE_URL", "")
    db_url = st.sidebar.text_input("DATABASE_URL", db_url, type="password")
    if db_url:
        try:
            df_t, df_c, df_e = load_postgres(db_url)
        except Exception as e:
            st.error(f"Postgres load error: {e}")
            st.stop()
    else:
        st.warning("Provide DATABASE_URL to load Postgres data."); st.stop()
elif use_sample == "BigQuery":
    st.sidebar.caption("Use service account auth or gcloud auth. Provide project/dataset.")
    bq_project = st.sidebar.text_input("BigQuery project", os.getenv("BQ_PROJECT", ""))
    bq_dataset = st.sidebar.text_input("BigQuery dataset", os.getenv("BQ_DATASET", ""))
    t_name = st.sidebar.text_input("Transactions table", os.getenv("BQ_TABLE_T", "transactions"))
    c_name = st.sidebar.text_input("Campaigns table", os.getenv("BQ_TABLE_C", "campaigns"))
    e_name = st.sidebar.text_input("Exposures table (optional)", os.getenv("BQ_TABLE_E", "exposures"))
    if bq_project and bq_dataset:
        try:
            df_t, df_c, df_e = load_bigquery(bq_project, bq_dataset, t_name, c_name, e_name)
        except Exception as e:
            st.error(f"BigQuery load error: {e}")
            st.stop()
    else:
        st.warning("Enter BigQuery project and dataset to proceed."); st.stop()

if df_t is None or df_c is None:
    st.warning("Please provide transactions and campaigns data to proceed.")
    st.stop()

# --- Filters ---
st.sidebar.header("Filters")
df_t["order_date"] = pd.to_datetime(df_t["order_date"])
min_date = pd.to_datetime(df_t["order_date"]).min()
max_date = pd.to_datetime(df_t["order_date"]).max()
date_range = st.sidebar.date_input("Order date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
categories = sorted(df_t["category"].dropna().unique().tolist())
cat_sel = st.sidebar.multiselect("Categories", categories, default=categories)

# campaign filter
camp_map = dict(zip(df_c.get("campaign_id", pd.Series(dtype=int)), df_c.get("name", df_c.get("campaign_type", pd.Series(dtype=str)))))
camp_ids = [int(x) for x in df_c["campaign_id"].dropna().unique().tolist()] if "campaign_id" in df_c.columns else []
camp_sel = st.sidebar.multiselect("Campaigns", options=camp_ids, default=camp_ids, format_func=lambda x: str(camp_map.get(x, x)))

# margin settings
st.sidebar.header("Assumptions")
base_margin_pct = st.sidebar.slider("Baseline gross margin % (pre-discount)", 5, 60, 25)

# --- Prepare data ---
mask = (df_t["order_date"].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]))) & (df_t["category"].isin(cat_sel))
if len(camp_sel) > 0 and "campaign_id" in df_t.columns:
    mask = mask & (df_t["campaign_id"].isin(camp_sel) | df_t["campaign_id"].isna())

data = df_t[mask].copy()
if data.empty:
    st.warning("No transactions match the current filters.")
    st.stop()

# add brackets
def bracket(p):
    try:
        p = int(p)
    except Exception:
        return "Unknown"
    if p == 0: return "0%"
    if 1 <= p <= 10: return "1-10%"
    if 11 <= p <= 20: return "11-20%"
    if 21 <= p <= 30: return "21-30%"
    return "31-50%"

data["discount_bracket"] = data["discount_percent"].apply(bracket)
data["discount_amount"] = data["original_price"] - data["final_price"]

# --- KPIs ---
total_orders = len(data)
gmv = float(data["final_price"].sum())
avg_order_value = float(data["final_price"].mean())
avg_discount_pct = (1 - (data["final_price"].sum() / data["original_price"].sum())) * 100 if data["original_price"].sum() > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Orders", f"{total_orders:,}")
col2.metric("GMV (₹)", f"{gmv:,.0f}")
col3.metric("Avg Order Value (₹)", f"{avg_order_value:,.0f}")
col4.metric("Avg Discount %", f"{avg_discount_pct:.1f}%")

st.divider()

# --- Revenue & ROI by discount bracket ---
grp = data.groupby("discount_bracket").agg(
    orders=("order_id", "count"),
    gmv=("final_price", "sum"),
    original=("original_price","sum"),
    discount_cost=("discount_amount","sum")
).reset_index()

grp["discount_per_order"] = grp["discount_cost"] / grp["orders"]
grp["gmv_per_order"] = grp["gmv"] / grp["orders"]
grp["roi_proxy"] = (grp["gmv_per_order"] - grp["discount_per_order"]) / grp["discount_per_order"]

st.subheader("Revenue & ROI (proxy) by Discount Bracket")
st.dataframe(grp.round(2), use_container_width=True)

best_row = grp.loc[grp["roi_proxy"].idxmax()]
st.success(f"Best discount bracket (by ROI proxy): **{best_row['discount_bracket']}** | ROI proxy: **{best_row['roi_proxy']:.2f}x** | Orders: **{int(best_row['orders'])}**")

st.divider()

# --- A/B test on conversion if exposures available ---
if df_e is not None and not df_e.empty and all(col in df_e.columns for col in ["impressions","conversions","discount_bracket"]):
    st.subheader("A/B Test on Conversion (from exposures)")
    df_e_filtered = df_e.copy()
    # filter by selected campaigns if any
    if len(camp_sel) > 0 and "campaign_id" in df_e_filtered.columns:
        df_e_filtered = df_e_filtered[df_e_filtered["campaign_id"].isin(camp_sel)]
    brackets = sorted(df_e_filtered["discount_bracket"].dropna().unique().tolist())
    if brackets:
        left = st.selectbox("Variant A (discount bracket)", brackets, index=0)
        right = st.selectbox("Variant B (discount bracket)", brackets, index=min(1, len(brackets)-1))

        a = df_e_filtered[df_e_filtered["discount_bracket"]==left][["impressions","conversions"]].sum(numeric_only=True)
        b = df_e_filtered[df_e_filtered["discount_bracket"]==right][["impressions","conversions"]].sum(numeric_only=True)
        cr_a = a["conversions"]/a["impressions"] if a["impressions"]>0 else 0
        cr_b = b["conversions"]/b["impressions"] if b["impressions"]>0 else 0

        # two-proportion z-test (manual)
        p_pool = (a["conversions"] + b["conversions"]) / (a["impressions"] + b["impressions"]) if (a["impressions"] + b["impressions"])>0 else np.nan
        se = np.sqrt(p_pool*(1-p_pool)*(1/a["impressions"] + 1/b["impressions"])) if a["impressions"]>0 and b["impressions"]>0 else np.nan
        z = (cr_a - cr_b)/se if (se is not None and isinstance(se, (float,int)) and se>0) else np.nan
        from scipy import stats as _stats
        p_val = 2*(1 - _stats.norm.cdf(abs(z))) if not np.isnan(z) else np.nan

        c1, c2, c3 = st.columns(3)
        c1.metric(f"CR {left}", f"{cr_a*100:.2f}%")
        c2.metric(f"CR {right}", f"{cr_b*100:.2f}%")
        c3.metric("p-value", f"{p_val:.4f}" if not np.isnan(p_val) else "N/A")

        if not np.isnan(p_val):
            if p_val < 0.05:
                st.success("Result: Statistically significant difference (p < 0.05).")
            else:
                st.info("Result: No statistically significant difference detected.")
else:
    st.info("Upload/Provide `exposures` to enable A/B test (impressions, conversions, discount_bracket).")

st.divider()
st.caption("Export filtered results from the 'Download' button below for further analysis.")

csv = data.to_csv(index=False).encode("utf-8")
st.download_button("Download filtered transactions CSV", csv, file_name="filtered_transactions.csv", mime="text/csv")
