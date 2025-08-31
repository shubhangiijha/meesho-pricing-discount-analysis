#!/usr/bin/env python3
"""
PySpark batch job for Pricing & Discount Effectiveness Analysis.

Usage:
  python spark_job.py --input_dir . --output_dir ./spark_out

Reads campaigns.csv, transactions.csv, exposures.csv
Writes Parquet aggregates in output_dir (per-bracket metrics, campaign metrics).
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def discount_bracket_col(col):
    return (F.when(col == 0, F.lit("0%"))
             .when((col >= 1) & (col <= 10), F.lit("1-10%"))
             .when((col >= 11) & (col <= 20), F.lit("11-20%"))
             .when((col >= 21) & (col <= 30), F.lit("21-30%"))
             .otherwise(F.lit("31-50%")))

def main(args):
    spark = (SparkSession.builder
             .appName("MeeshoPricingDiscountSpark")
             .getOrCreate())

    input_dir = args.input_dir
    out = args.output_dir

    tx = spark.read.option("header", True).option("inferSchema", True).csv(f"{input_dir}/transactions.csv")
    campaigns = spark.read.option("header", True).option("inferSchema", True).csv(f"{input_dir}/campaigns.csv")
    exposures = spark.read.option("header", True).option("inferSchema", True).csv(f"{input_dir}/exposures.csv")

    tx = tx.withColumn("discount_bracket", discount_bracket_col(F.col("discount_percent"))) \
           .withColumn("discount_amount", F.col("original_price") - F.col("final_price"))

    # Aggregate by discount bracket
    agg_bracket = (tx.groupBy("discount_bracket")
                     .agg(F.count("*").alias("orders"),
                          F.sum("final_price").alias("gmv"),
                          F.sum("original_price").alias("original"),
                          F.sum("discount_amount").alias("discount_cost"))
                     .withColumn("discount_per_order", F.col("discount_cost")/F.col("orders"))
                     .withColumn("gmv_per_order", F.col("gmv")/F.col("orders"))
                     .withColumn("roi_proxy", (F.col("gmv_per_order") - F.col("discount_per_order")) / F.col("discount_per_order")))

    # Campaign-level metrics
    tx_camp = tx.join(campaigns, on="campaign_id", how="left")
    agg_campaign = (tx_camp.groupBy("campaign_id", "campaign_type")
                      .agg(F.count("*").alias("orders"),
                           F.sum("final_price").alias("gmv"),
                           F.avg("discount_percent").alias("avg_discount_pct")))

    # Exposures conversion by bracket (for CR & lift calculations later)
    exp_conv = (exposures.groupBy("campaign_id", "discount_bracket")
                  .agg(F.sum("impressions").alias("impressions"),
                       F.sum("conversions").alias("conversions"))
                  .withColumn("cr", F.col("conversions")/F.col("impressions")))

    # Write out
    agg_bracket.repartition(1).write.mode("overwrite").parquet(f"{out}/agg_by_discount_bracket")
    agg_campaign.repartition(1).write.mode("overwrite").parquet(f"{out}/agg_by_campaign")
    exp_conv.repartition(1).write.mode("overwrite").parquet(f"{out}/exposures_by_campaign_bracket")

    print(f"✅ Wrote Parquet outputs to {out}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input_dir", default=".", help="Folder with CSVs")
    p.add_argument("--output_dir", default="./spark_out", help="Folder for Parquet outputs")
    main(p.parse_args())
