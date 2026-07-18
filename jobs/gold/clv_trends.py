"""Gold — portfolio CLV trends (daily).

Aggregates the dense per-customer CLV table down to ONE row per day so the
dashboard can chart "CLV evolving daily" without loading ~11M rows:

  total_cumulative_clv : Σ cumulative_clv across all customers that day
  avg_cumulative_clv   : mean cumulative_clv per active customer
  daily_revenue        : Σ revenue booked that day
  active_customers     : customers whose lifetime includes that day (acquired so far)
  new_customers        : customers whose first order is that day

Reads  <bronze>/gold/customer_clv_daily
Writes <bronze>/gold/clv_trends/

Run:
    spark-submit jobs/gold/clv_trends.py --bronze s3://...
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GOLD_RELPATH = "gold/clv_trends"


def compute_clv_trends(clv_daily: DataFrame) -> DataFrame:
    """clv_daily: user_id, clv_date, first_order_date, daily_revenue, cumulative_clv."""
    return (
        clv_daily.groupBy("clv_date")
        .agg(
            F.sum("cumulative_clv").alias("total_cumulative_clv"),
            F.avg("cumulative_clv").alias("avg_cumulative_clv"),
            F.sum("daily_revenue").alias("daily_revenue"),
            F.count(F.lit(1)).alias("active_customers"),
            F.sum(F.when(F.col("clv_date") == F.col("first_order_date"), 1).otherwise(0)).alias("new_customers"),
        )
        .orderBy("clv_date")
    )


def run(spark, bronze):
    clv = spark.read.parquet(f"{bronze}/gold/customer_clv_daily")
    out = compute_clv_trends(clv)
    path = f"{bronze}/{GOLD_RELPATH}"
    out.write.mode("overwrite").parquet(path)
    print(f"[clv_trends] rows={out.count()} -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Gold — portfolio CLV trends")
    p.add_argument("--bronze", required=True)
    args, _ = p.parse_known_args(argv)  # ignore Glue-injected args (--JOB_NAME etc.)
    spark = SparkSession.builder.appName("gpb-gold-clv-trends").getOrCreate()
    run(spark, args.bronze)
    spark.stop()


if __name__ == "__main__":
    main()
