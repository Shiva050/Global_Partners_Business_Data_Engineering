"""Gold — sales trends & seasonality.

Daily sales fact at grain (order_date, restaurant_id, item_category), joined to
the date dimension for calendar attributes (week/month/day-of-week/holiday). The
dashboard rolls this up to weekly/monthly and slices by location, category, and
holiday — so one daily fact serves all the "daily/weekly/monthly" views without
materializing three tables.

Revenue is line-item net revenue (item + options, discounts applied), so it
splits cleanly by menu category (an order spans multiple categories).

Reads  <bronze>/silver/facts/order_line_items , <bronze>/silver/current/date_dim
Writes <bronze>/gold/sales_daily/

Run:
    spark-submit jobs/gold/sales_trends.py --bronze s3://...
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GOLD_RELPATH = "gold/sales_daily"
CAL_COLS = ["year", "month", "week", "day_of_week", "is_weekend", "is_holiday", "holiday_name"]


def compute_sales_daily(line_items: DataFrame, date_dim: DataFrame) -> DataFrame:
    li = line_items.withColumn("order_date", F.to_date("creation_time_utc"))
    daily = li.groupBy("order_date", "restaurant_id", "item_category").agg(
        F.sum(F.col("net_revenue").cast("double")).alias("revenue"),
        F.sum(F.col("gross_revenue").cast("double")).alias("gross_revenue"),
        F.sum(F.col("discount_amount").cast("double")).alias("discount_amount"),
        F.sum(F.col("item_quantity").cast("double")).alias("quantity"),
        F.countDistinct("order_id").alias("order_count"),
        F.count(F.lit(1)).alias("line_item_count"),
    )
    cal = date_dim.select(F.col("date_key"), *CAL_COLS)
    return daily.join(cal, daily["order_date"] == cal["date_key"], "left").drop("date_key")


def run(spark, bronze):
    li = spark.read.parquet(f"{bronze}/silver/facts/order_line_items").filter(~F.col("is_outlier"))
    dd = spark.read.parquet(f"{bronze}/silver/current/date_dim")
    out = compute_sales_daily(li, dd)
    path = f"{bronze}/{GOLD_RELPATH}"
    out.write.mode("overwrite").parquet(path)
    print(f"[sales_trends] rows={out.count()} -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Gold — sales trends (daily fact)")
    p.add_argument("--bronze", required=True)
    args, _ = p.parse_known_args(argv)  # ignore Glue-injected args (--JOB_NAME etc.)
    spark = SparkSession.builder.appName("gpb-gold-sales-trends").getOrCreate()
    run(spark, args.bronze)
    spark.stop()


if __name__ == "__main__":
    main()
