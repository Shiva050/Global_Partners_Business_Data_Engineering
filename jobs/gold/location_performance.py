"""Gold — location (restaurant) performance.

Ranks locations by revenue and reports the operational metrics that distinguish
top from bottom performers.

  total_revenue, order_count, unique_customers
  avg_order_value = revenue / orders
  revenue_rank    = dense rank by total revenue (1 = best)

Reads  <bronze>/silver/facts/orders
Writes <bronze>/gold/location_performance/

Run:
    spark-submit jobs/gold/location_performance.py --bronze s3://...
"""
import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

GOLD_RELPATH = "gold/location_performance"


def compute_location_performance(orders: DataFrame) -> DataFrame:
    agg = orders.groupBy("restaurant_id").agg(
        F.sum(F.col("order_revenue").cast("double")).alias("total_revenue"),
        F.count(F.lit(1)).alias("order_count"),
        F.countDistinct("user_id").alias("unique_customers"),
    ).withColumn("avg_order_value", F.col("total_revenue") / F.col("order_count"))

    return agg.withColumn(
        "revenue_rank", F.dense_rank().over(Window.orderBy(F.col("total_revenue").desc()))
    ).select(
        "restaurant_id", "revenue_rank", "total_revenue", "order_count",
        "unique_customers", "avg_order_value",
    )


def run(spark, bronze):
    orders = spark.read.parquet(f"{bronze}/silver/facts/orders").select(
        "restaurant_id", "user_id", "order_revenue"
    )
    out = compute_location_performance(orders)
    path = f"{bronze}/{GOLD_RELPATH}"
    out.write.mode("overwrite").parquet(path)
    print(f"[location_performance] rows={out.count()} -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Gold — location performance")
    p.add_argument("--bronze", required=True)
    args, _ = p.parse_known_args(argv)  # ignore Glue-injected args (--JOB_NAME etc.)
    spark = SparkSession.builder.appName("gpb-gold-location").getOrCreate()
    run(spark, args.bronze)
    spark.stop()


if __name__ == "__main__":
    main()
