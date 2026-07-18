"""Gold — CLV value tiers (High / Medium / Low).

Aggregates lifetime spend per customer and tiers by percentile (per the BRD):
  High   : top 20%   of customers by total CLV
  Medium : middle 60%
  Low    : bottom 20%

Tiers use ntile(5) over customers ranked by total CLV desc — quintile 1 is the
top 20% (High), quintile 5 the bottom 20% (Low), quintiles 2-4 the middle 60%
(Medium). `total_clv` here equals a customer's final `cumulative_clv` in the
daily CLV table (both are Σ order_revenue) — computed from the orders fact so
this job is self-contained.

Point-in-time snapshot, full recompute each run.

Reads  <bronze>/silver/facts/orders
Writes <bronze>/gold/customer_clv_tiers/

Run:
    spark-submit jobs/gold/customer_clv_tiers.py --bronze s3://...
"""
import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

GOLD_RELPATH = "gold/customer_clv_tiers"


def compute_clv_tiers(orders: DataFrame) -> DataFrame:
    """orders: user_id, order_revenue. Returns one row per customer with its tier."""
    agg = orders.groupBy("user_id").agg(
        F.sum(F.col("order_revenue").cast("double")).alias("total_clv"),
        F.count(F.lit(1)).alias("total_orders"),
    )

    # Quintile by CLV desc: q1 = top 20% (High), q5 = bottom 20% (Low).
    agg = agg.withColumn("clv_quintile", F.ntile(5).over(Window.orderBy(F.col("total_clv").desc())))

    tier = (
        F.when(F.col("clv_quintile") == 1, F.lit("High"))
        .when(F.col("clv_quintile") == 5, F.lit("Low"))
        .otherwise(F.lit("Medium"))
    )
    return agg.withColumn("clv_tier", tier).select(
        "user_id", "total_clv", "total_orders", "clv_quintile", "clv_tier"
    )


def run(spark, bronze):
    orders = spark.read.parquet(f"{bronze}/silver/facts/orders").select("user_id", "order_revenue").filter(F.col("user_id").isNotNull())
    out = compute_clv_tiers(orders)
    path = f"{bronze}/{GOLD_RELPATH}"
    out.write.mode("overwrite").parquet(path)
    print(f"[clv_tiers] rows={out.count()} -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Gold — CLV value tiers")
    p.add_argument("--bronze", required=True)
    args, _ = p.parse_known_args(argv)  # ignore Glue-injected args (--JOB_NAME etc.)

    spark = SparkSession.builder.appName("gpb-gold-clv-tiers").getOrCreate()
    run(spark, args.bronze)
    spark.stop()


if __name__ == "__main__":
    main()
