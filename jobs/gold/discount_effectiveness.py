"""Gold — pricing & discount effectiveness.

Compares discounted vs full-price orders on volume and revenue. Discounts are
detected upstream (order_item_options.option_price < 0 -> negative
discount_amount); an order is "discounted" when its total discount is negative.

  orders, total_gross_revenue, total_net_revenue, total_discount
  avg_order_value_net = net revenue / orders
  discount_depth_pct  = -discount / gross  (share of gross given back)

Reads  <bronze>/silver/facts/orders
Writes <bronze>/gold/discount_effectiveness/

Run:
    spark-submit jobs/gold/discount_effectiveness.py --bronze s3://...
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GOLD_RELPATH = "gold/discount_effectiveness"


def compute_discount_effectiveness(orders: DataFrame) -> DataFrame:
    return (
        orders.groupBy("is_discounted")
        .agg(
            F.count(F.lit(1)).alias("orders"),
            F.sum(F.col("order_gross_revenue").cast("double")).alias("total_gross_revenue"),
            F.sum(F.col("order_revenue").cast("double")).alias("total_net_revenue"),
            F.sum(F.col("order_discount_amount").cast("double")).alias("total_discount"),
        )
        .withColumn("avg_order_value_net", F.col("total_net_revenue") / F.col("orders"))
        .withColumn(
            "discount_depth_pct",
            F.when(
                F.col("total_gross_revenue") > 0,
                -F.col("total_discount") / F.col("total_gross_revenue") * 100,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "segment", F.when(F.col("is_discounted"), F.lit("Discounted")).otherwise(F.lit("Full-price"))
        )
        .select(
            "segment", "is_discounted", "orders", "total_gross_revenue",
            "total_net_revenue", "total_discount", "avg_order_value_net", "discount_depth_pct",
        )
    )


def run(spark, bronze):
    orders = spark.read.parquet(f"{bronze}/silver/facts/orders").filter(~F.col("is_outlier")).select(
        "is_discounted", "order_gross_revenue", "order_revenue", "order_discount_amount"
    )
    out = compute_discount_effectiveness(orders)
    path = f"{bronze}/{GOLD_RELPATH}"
    out.write.mode("overwrite").parquet(path)
    print(f"[discount_effectiveness] rows={out.count()} -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Gold — discount effectiveness")
    p.add_argument("--bronze", required=True)
    args, _ = p.parse_known_args(argv)  # ignore Glue-injected args (--JOB_NAME etc.)
    spark = SparkSession.builder.appName("gpb-gold-discount").getOrCreate()
    run(spark, args.bronze)
    spark.stop()


if __name__ == "__main__":
    main()
