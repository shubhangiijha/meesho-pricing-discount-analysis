#!/usr/bin/env python3
"""
Load CSVs into Postgres (transactions, campaigns, exposures) using SQLAlchemy.
Env:
  DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/dbname
Usage:
  python db_loader.py --input_dir .
"""
import os, argparse
import pandas as pd
from sqlalchemy import create_engine, text

def main(args):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL env var not set. See .env.example")

    engine = create_engine(db_url, pool_pre_ping=True)

    tx = pd.read_csv(f"{args.input_dir}/transactions.csv")
    campaigns = pd.read_csv(f"{args.input_dir}/campaigns.csv")
    exposures = pd.read_csv(f"{args.input_dir}/exposures.csv")

    with engine.begin() as conn:
        campaigns.to_sql("campaigns", conn, if_exists="replace", index=False)
        tx.to_sql("transactions", conn, if_exists="replace", index=False)
        exposures.to_sql("exposures", conn, if_exists="replace", index=False)

        # Indexes
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(order_date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_campaign ON transactions(campaign_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_expo_campaign ON exposures(campaign_id)"))

    print("✅ Loaded CSVs into Postgres.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", default=".", help="Folder with CSVs")
    main(ap.parse_args())
