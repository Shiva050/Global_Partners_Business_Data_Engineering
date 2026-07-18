"""Gold — loyalty program impact.

Compares loyalty members vs non-members on spend and engagement. A customer is
classified as a loyalty member if ANY of their orders carried the loyalty flag
(max of is_loyalty), then per-customer measures are aggregated to two segment
rows for a direct comparison.

  customers            : distinct customers in the segment
  total_orders / revenue
  avg_order_value      : revenue / orders (AOV)
  avg_orders_per_customer : engagement / repeat behaviour
  avg_clv              : revenue / customers (lifetime value)
  repeat_rate          : share of customers with more than one order

Reads  <bronze>/silver/facts/orders
Writes <bronze>/gold/loyalty_impact/

Run:
    spark-submit jobs/gold/loyalty_impact.py --bronze s3://...
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GOLD_RELPATH = "gold/loyalty_impact"


def compute_loyalty_impact(orders: DataFrame) -> DataFrame:
    per_customer = orders.groupBy("user_id").agg(
        F.max(F.col("is_loyalty").cast("int")).alias("_member"),
        F.count(F.lit(1)).alias("n_orders"),
        F.sum(F.col("order_revenue").cast("double")).alias("spend"),
    ).withColumn("is_loyalty_member", F.col("_member") > 0)

    return (
        per_customer.groupBy("is_loyalty_member")
        .agg(
            F.countDistinct("user_id").alias("customers"),
            F.sum("n_orders").alias("total_orders"),
            F.sum("spend").alias("total_revenue"),
            F.avg("n_orders").alias("avg_orders_per_customer"),
            F.avg("spend").alias("avg_clv"),
            F.avg((F.col("n_orders") > 1).cast("double")).alias("repeat_rate"),
        )
        .withColumn("avg_order_value", F.col("total_revenue") / F.col("total_orders"))
        .withColumn("segment", F.when(F.col("is_loyalty_member"), F.lit("Loyalty")).otherwise(F.lit("Non-Loyalty")))
        .select(
            "segment", "is_loyalty_member", "customers", "total_orders", "total_revenue",
            "avg_order_value", "avg_orders_per_customer", "avg_clv", "repeat_rate",
        )
    )


def run(spark, bronze):
    orders = spark.read.parquet(f"{bronze}/silver/facts/orders").select(
        "user_id", "order_revenue", "is_loyalty"
    )
    out = compute_loyalty_impact(orders)
    path = f"{bronze}/{GOLD_RELPATH}"
    out.write.mode("overwrite").parquet(path)
    print(f"[loyalty_impact] rows={out.count()} -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Gold — loyalty impact")
    p.add_argument("--bronze", required=True)
    args, _ = p.parse_known_args(argv)  # ignore Glue-injected args (--JOB_NAME etc.)
    spark = SparkSession.builder.appName("gpb-gold-loyalty").getOrCreate()
    run(spark, args.bronze)
    spark.stop()


if __name__ == "__main__":
    main()
