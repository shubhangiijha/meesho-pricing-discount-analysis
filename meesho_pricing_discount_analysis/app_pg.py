import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Pricing Analysis (Postgres)", layout="wide")
st.title("💾 Pricing & Discount Effectiveness — Postgres-backed")

db_url = os.getenv("DATABASE_URL")  # e.g., postgresql+psycopg2://user:pass@host:5432/db
if not db_url:
    st.error("Please set DATABASE_URL environment variable.")
    st.stop()

engine = create_engine(db_url, pool_pre_ping=True)

@st.cache_data(ttl=600)
def load_tables():
    with engine.begin() as conn:
        tx = pd.read_sql(text("SELECT * FROM transactions"), conn)
        campaigns = pd.read_sql(text("SELECT * FROM campaigns"), conn)
        try:
            exposures = pd.read_sql(text("SELECT * FROM exposures"), conn)
        except Exception:
            exposures = None
    return tx, campaigns, exposures

df_t, df_c, df_e = load_tables()

# Filters
st.sidebar.header("Filters")
df_t["order_date"] = pd.to_datetime(df_t["order_date"])
min_date, max_date = df_t["order_date"].min().date(), df_t["order_date"].max().date()
date_range = st.sidebar.date_input("Order date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
categories = sorted(df_t["category"].dropna().unique().tolist())
cat_sel = st.sidebar.multiselect("Categories", categories, default=categories)
camp_sel = st.sidebar.multiselect("Campaign IDs", sorted(df_t["campaign_id"].dropna().unique().astype(int)), default=[])

mask = (df_t["order_date"].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]))) & (df_t["category"].isin(cat_sel))
if camp_sel:
    mask &= df_t["campaign_id"].isin(camp_sel)

data = df_t[mask].copy()
if data.empty:
    st.warning("No transactions match filters")
    st.stop()

def bracket(p):
    if p == 0: return "0%"
    if 1 <= p <= 10: return "1-10%"
    if 11 <= p <= 20: return "11-20%"
    if 21 <= p <= 30: return "21-30%"
    return "31-50%"

data["discount_bracket"] = data["discount_percent"].apply(bracket)
data["discount_amount"] = data["original_price"] - data["final_price"]

# KPIs
orders = len(data)
gmv = data["final_price"].sum()
aov = data["final_price"].mean()
avg_disc = (1 - data["final_price"].sum()/data["original_price"].sum())*100 if data["original_price"].sum() else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Orders", f"{orders:,}")
c2.metric("GMV (₹)", f"{gmv:,.0f}")
c3.metric("Avg Order Value (₹)", f"{aov:,.0f}")
c4.metric("Avg Discount %", f"{avg_disc:.1f}%")

st.divider()

grp = (data.groupby("discount_bracket")
          .agg(orders=("order_id","count"),
               gmv=("final_price","sum"),
               original=("original_price","sum"),
               discount_cost=("discount_amount","sum"))
          .reset_index())
grp["discount_per_order"] = grp["discount_cost"]/grp["orders"]
grp["gmv_per_order"] = grp["gmv"]/grp["orders"]
grp["roi_proxy"] = (grp["gmv_per_order"] - grp["discount_per_order"]) / grp["discount_per_order"]
st.subheader("Bracket Metrics")
st.dataframe(grp.round(2), use_container_width=True)

best = grp.loc[grp["roi_proxy"].idxmax()]
st.success(f"Best bracket: **{best['discount_bracket']}** | ROI proxy **{best['roi_proxy']:.2f}x**")

# Optional SQL box
st.subheader("SQL (read-only)")
user_sql = st.text_area("Run SQL on transactions", "SELECT discount_percent, COUNT(*) AS orders FROM transactions GROUP BY 1 ORDER BY 1;")
if st.button("Run"):
    try:
        with engine.begin() as conn:
            res = pd.read_sql(text(user_sql), conn)
        st.dataframe(res, use_container_width=True)
    except Exception as e:
        st.error(str(e))
