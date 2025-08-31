#!/usr/bin/env python3
"""
Generate synthetic datasets for Pricing & Discount Effectiveness Analysis.
Creates:
- campaigns.csv
- transactions.csv
- exposures.csv
- meesho_pricing.db (SQLite with tables campaigns, transactions, exposures)
"""
import os
import random
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import sqlite3

random.seed(42)
np.random.seed(42)

OUT_DIR = os.path.dirname(__file__) if os.path.dirname(__file__) else "."

def make_campaigns(n_campaigns=12, start="2024-01-01", end="2025-08-15"):
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    total_days = (end_dt - start_dt).days

    types = ["Festival", "Flash Sale", "Referral", "Clearance", "Payday", "New User", "Category Push"]
    campaigns = []
    for cid in range(1, n_campaigns+1):
        dur = random.randint(7, 21)
        s = start_dt + timedelta(days=random.randint(0, max(1,total_days-dur)))
        e = s + timedelta(days=dur)
        campaigns.append({
            "campaign_id": cid,
            "campaign_type": random.choice(types),
            "start_date": s.date().isoformat(),
            "end_date": e.date().isoformat(),
            "name": f"{cid}-{random.choice(types)}-{s.strftime('%b%Y')}"
        })
    return pd.DataFrame(campaigns)

def draw_price(category):
    # Typical price distributions by category (INR)
    base = {
        "Fashion": (250, 1200),
        "Home & Kitchen": (300, 2500),
        "Electronics": (500, 8000),
        "Beauty": (150, 1200),
        "Grocery": (50, 500),
        "Footwear": (350, 2200),
        "Accessories": (100, 1500),
    }
    lo, hi = base.get(category, (200, 2000))
    return round(np.random.gamma(shape=2.0, scale=(hi-lo)/4) + lo, 2)

def gen_transactions(n_orders=200_000, n_customers=50_000, n_products=5_000,
                     start="2024-01-01", end="2025-08-15", campaigns_df=None):
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    categories = ["Fashion", "Home & Kitchen", "Electronics", "Beauty", "Grocery", "Footwear", "Accessories"]
    records = []

    # Choose discount bracket with probabilities (more weight on lower discounts)
    # 0, 1-10, 11-20, 21-30, 31-50
    brackets = [(0,0), (1,10), (11,20), (21,30), (31,50)]
    probs = [0.25, 0.30, 0.25, 0.15, 0.05]
    bracket_choices = np.random.choice(len(brackets), size=n_orders, p=probs)

    # sample campaign for each order based on whether order_date falls into campaign range
    # if no matching campaign by date, assign None
    for oid in range(1, n_orders+1):
        order_date = start_dt + timedelta(days=np.random.randint(0, (end_dt-start_dt).days+1))
        category = random.choice(categories)
        original_price = draw_price(category)

        # discount
        bidx = bracket_choices[oid-1]
        lo, hi = brackets[bidx]
        discount_percent = 0 if (lo==0 and hi==0) else np.random.randint(lo, hi+1)

        final_price = round(original_price * (1 - discount_percent/100), 2)

        # choose campaign if active on that date, else None
        campaign_id = None
        if campaigns_df is not None and len(campaigns_df) > 0:
            active = campaigns_df[
                (campaigns_df["start_date"] <= order_date.date().isoformat()) &
                (campaigns_df["end_date"] >= order_date.date().isoformat())
            ]
            if not active.empty:
                campaign_id = int(active.sample(1)["campaign_id"].iloc[0])

        records.append({
            "order_id": oid,
            "customer_id": np.random.randint(1, n_customers+1),
            "product_id": np.random.randint(1, n_products+1),
            "order_date": order_date.date().isoformat(),
            "category": category,
            "original_price": original_price,
            "discount_percent": int(discount_percent),
            "final_price": final_price,
            "campaign_id": campaign_id
        })
    return pd.DataFrame(records)

def gen_exposures(campaigns_df, n_rows=50_000):
    """Generate exposures per (campaign, discount_bracket). Provide impressions and conversions.

    Conversion rate increases with higher discounts but has diminishing ROI.
    """
    def bracket_of(p):
        if p == 0: return "0%"
        if 1 <= p <= 10: return "1-10%"
        if 11 <= p <= 20: return "11-20%"
        if 21 <= p <= 30: return "21-30%"
        return "31-50%"

    rows = []
    for _ in range(n_rows):
        camp = campaigns_df.sample(1).iloc[0]
        # pick a discount bracket, biased to lower
        bracket = np.random.choice(["0%", "1-10%", "11-20%", "21-30%", "31-50%"], p=[0.25,0.3,0.25,0.15,0.05])
        # base impressions
        impressions = np.random.randint(50, 5000)

        # base conversion rate by bracket (synthetic but reasonable)
        base_cr = {
            "0%": 0.9/100,
            "1-10%": 1.4/100,
            "11-20%": 2.0/100,
            "21-30%": 2.3/100,
            "31-50%": 2.35/100,
        }[bracket]
        # add slight campaign noise
        cr = max(0.0001, np.random.normal(base_cr, base_cr*0.15))
        conversions = np.random.binomial(impressions, cr)
        rows.append({
            "campaign_id": int(camp["campaign_id"]),
            "campaign_type": camp["campaign_type"],
            "discount_bracket": bracket,
            "impressions": impressions,
            "conversions": int(conversions),
            "start_date": camp["start_date"],
            "end_date": camp["end_date"]
        })
    return pd.DataFrame(rows)

def build_sqlite(db_path, campaigns, transactions, exposures):
    con = sqlite3.connect(db_path)
    campaigns.to_sql("campaigns", con, if_exists="replace", index=False)
    transactions.to_sql("transactions", con, if_exists="replace", index=False)
    exposures.to_sql("exposures", con, if_exists="replace", index=False)
    # useful indices
    con.execute("CREATE INDEX IF NOT EXISTS idx_trans_date ON transactions(order_date);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_trans_campaign ON transactions(campaign_id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_expo_campaign ON exposures(campaign_id);")
    con.commit()
    con.close()

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    campaigns = make_campaigns()
    transactions = gen_transactions(campaigns_df=campaigns)
    exposures = gen_exposures(campaigns)

    campaigns.to_csv(os.path.join(OUT_DIR, "campaigns.csv"), index=False)
    transactions.to_csv(os.path.join(OUT_DIR, "transactions.csv"), index=False)
    exposures.to_csv(os.path.join(OUT_DIR, "exposures.csv"), index=False)

    build_sqlite(os.path.join(OUT_DIR, "meesho_pricing.db"),
                 campaigns, transactions, exposures)
    print("Data generated at:", OUT_DIR)

if __name__ == "__main__":
    main()
