"""Silver — revenue-enriched order facts.

Builds two grains from the CDC current-state tables:

  * order_line_items : one row per (order_id, lineitem_id) with revenue split
                       into gross / discount / net, plus the line's attributes.
  * orders           : one row per order_id — revenue rolled up, customer and
                       channel attributes, order date (for time-based joins).

Revenue model
-------------
  item_amount    = item_price * item_quantity
  option amount  = option_price * option_quantity   (per option row)
     add-ons     : option_price >= 0
     discounts   : option_price <  0   (order_item_options carries discounts as
                                         negative option_price)
  gross_revenue  = item_amount + Σ add-ons
  discount_amount= Σ discounts                        (<= 0)
  net_revenue    = item_amount + Σ all options = gross_revenue + discount_amount

Reads  <bronze>/silver/current/{order_items, order_item_options}/
Writes <bronze>/silver/facts/{order_line_items, orders}/

Run:
    spark-submit jobs/silver/order_facts.py --bronze s3://dms-...-bronze
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _amount(price_col: str, qty_col: str):
    """Money = price * quantity, cast defensively (source may be decimal/str)."""
    return F.col(price_col).cast("double") * F.col(qty_col).cast("double")


def line_item_facts(order_items: DataFrame, options: DataFrame, outlier_threshold: float = 10000.0) -> DataFrame:
    """Per (order_id, lineitem_id): item + option revenue split into gross/discount/net.

    Flags is_outlier on absurd line revenue (same threshold as orders) so
    line-item-grain gold (sales trends) can exclude the same anomalies.
    """
    opt = options.withColumn("_amt", _amount("option_price", "option_quantity"))
    opt_agg = opt.groupBy("order_id", "lineitem_id").agg(
        F.sum("_amt").alias("options_amount"),
        F.sum(F.when(F.col("_amt") < 0, F.col("_amt")).otherwise(F.lit(0.0))).alias("options_discount"),
        F.sum(F.when(F.col("_amt") > 0, F.col("_amt")).otherwise(F.lit(0.0))).alias("options_addon"),
    )

    li = order_items.withColumn("item_amount", _amount("item_price", "item_quantity"))
    li = li.join(opt_agg, ["order_id", "lineitem_id"], "left")
    for c in ("options_amount", "options_discount", "options_addon"):
        li = li.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))

    return (
        li.withColumn("gross_revenue", F.col("item_amount") + F.col("options_addon"))
        .withColumn("discount_amount", F.col("options_discount"))
        .withColumn("net_revenue", F.col("item_amount") + F.col("options_amount"))
        .withColumn("is_discounted", F.col("options_discount") < 0)
        .withColumn("is_outlier", (F.col("item_amount") + F.col("options_amount")) > F.lit(outlier_threshold))
        .drop("options_amount", "options_addon")
    )


def order_facts(line_items: DataFrame, outlier_threshold: float = 10000.0) -> DataFrame:
    """Roll line items up to one row per order_id.

    Flags (does not drop) orders whose revenue exceeds `outlier_threshold` — the
    raw data contains a handful of absurd orders ($2.5M, one $5,000 item x 500)
    that would otherwise dominate CLV/tiers/location. Dashboards can filter
    is_outlier=false for a clean view; all rows are retained and auditable.
    """
    return (
        line_items.groupBy("order_id")
        .agg(
            F.first("user_id", ignorenulls=True).alias("user_id"),
            F.first("restaurant_id", ignorenulls=True).alias("restaurant_id"),
            F.first("app_name", ignorenulls=True).alias("app_name"),
            F.first("currency", ignorenulls=True).alias("currency"),
            F.first("is_loyalty", ignorenulls=True).alias("is_loyalty"),
            F.first("creation_time_utc", ignorenulls=True).alias("creation_time_utc"),
            F.sum("net_revenue").alias("order_revenue"),
            F.sum("gross_revenue").alias("order_gross_revenue"),
            F.sum("discount_amount").alias("order_discount_amount"),
            F.count("*").alias("line_item_count"),
            F.sum(F.col("item_quantity").cast("double")).alias("total_item_quantity"),
        )
        .withColumn("order_ts", F.to_timestamp("creation_time_utc"))
        .withColumn("order_date", F.to_date("creation_time_utc"))
        .withColumn("is_discounted", F.col("order_discount_amount") < 0)
        .withColumn("is_outlier", F.col("order_revenue") > F.lit(outlier_threshold))
    )


def run(spark: SparkSession, bronze: str):
    oi = spark.read.parquet(f"{bronze}/silver/current/order_items")
    oo = spark.read.parquet(f"{bronze}/silver/current/order_item_options")

    li = line_item_facts(oi, oo)
    li_path = f"{bronze}/silver/facts/order_line_items"
    li.write.mode("overwrite").parquet(li_path)
    print(f"[order_facts] order_line_items rows = {li.count()} -> {li_path}")

    of = order_facts(spark.read.parquet(li_path))  # read back for clean lineage
    of_path = f"{bronze}/silver/facts/orders"
    of.write.mode("overwrite").parquet(of_path)
    print(f"[order_facts] orders rows = {of.count()} -> {of_path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Silver — revenue-enriched order facts")
    p.add_argument("--bronze", required=True)
    args, _ = p.parse_known_args(argv)  # ignore Glue-injected args (--JOB_NAME etc.)

    spark = SparkSession.builder.appName("gpb-silver-order-facts").getOrCreate()
    run(spark, args.bronze)
    spark.stop()


if __name__ == "__main__":
    main()
