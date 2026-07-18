"""Gold — RFM segmentation.

Per customer, as of a reference date:
  Recency   = days since their LIFETIME last order (smaller = more recent)
  Frequency = number of orders within the last N months
  Monetary  = net revenue within the last N months

Each dimension is scored into quintiles (1-5) across the customer population:
recent -> R=5, frequent -> F=5, high spend -> M=5. Segments follow the spec:

  VIP        : high R, F, M
  New        : recent (high R) but few orders (low F)
  Churn Risk : stale (low R) and few orders (low F)
  Regular    : everything else

Recency uses the lifetime last order (so customers who went quiet are caught),
while Frequency/Monetary use the rolling window (the spec's "last N months").

This is a point-in-time snapshot recomputed in full each run — no cumulative
state, so (unlike CLV) it needs no incremental window.

Reads  <bronze>/silver/facts/orders
Writes <bronze>/gold/customer_rfm/

Run:
    spark-submit jobs/gold/customer_rfm.py --bronze s3://... [--lookback-months 12] [--as-of 2023-12-31]
"""
import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

GOLD_RELPATH = "gold/customer_rfm"
HIGH, LOW = 4, 2  # quintile thresholds for "high"/"low"


def compute_rfm(orders: DataFrame, as_of: str, lookback_months: int = 12) -> DataFrame:
    """orders: user_id, order_revenue, order_date (date). Returns one row per customer."""
    as_of_c = F.to_date(F.lit(as_of))
    window_start = F.add_months(as_of_c, -lookback_months)

    # Recency from the lifetime last order.
    recency = (
        orders.groupBy("user_id")
        .agg(F.max("order_date").alias("last_order_date"))
        .withColumn("recency_days", F.datediff(as_of_c, F.col("last_order_date")))
    )

    # Frequency & Monetary within the rolling window.
    windowed = orders.filter(
        (F.col("order_date") >= window_start) & (F.col("order_date") <= as_of_c)
    )
    fm = windowed.groupBy("user_id").agg(
        F.count(F.lit(1)).alias("frequency"),
        F.sum(F.col("order_revenue").cast("double")).alias("monetary"),
    )

    rfm = (
        recency.join(fm, "user_id", "left")
        .withColumn("frequency", F.coalesce(F.col("frequency"), F.lit(0)))
        .withColumn("monetary", F.coalesce(F.col("monetary"), F.lit(0.0)))
    )

    # Quintile scores (ntile over the whole population).
    rfm = (
        rfm.withColumn("r_score", F.ntile(5).over(Window.orderBy(F.col("recency_days").desc())))
        .withColumn("f_score", F.ntile(5).over(Window.orderBy(F.col("frequency").asc())))
        .withColumn("m_score", F.ntile(5).over(Window.orderBy(F.col("monetary").asc())))
    )

    r, f, m = F.col("r_score"), F.col("f_score"), F.col("m_score")
    segment = (
        F.when((r >= HIGH) & (f >= HIGH) & (m >= HIGH), F.lit("VIP"))
        .when((r >= HIGH) & (f <= LOW), F.lit("New"))
        .when((r <= LOW) & (f <= LOW), F.lit("Churn Risk"))
        .otherwise(F.lit("Regular"))
    )

    return (
        rfm.withColumn("rfm_segment", segment)
        .withColumn("rfm_score", r + f + m)
        .withColumn("as_of_date", as_of_c)
        .withColumn("lookback_months", F.lit(lookback_months))
        .select(
            "user_id", "as_of_date", "last_order_date", "recency_days",
            "frequency", "monetary", "r_score", "f_score", "m_score",
            "rfm_score", "rfm_segment", "lookback_months",
        )
    )


def run(spark, bronze, as_of, lookback_months):
    orders = spark.read.parquet(f"{bronze}/silver/facts/orders").select(
        "user_id", "order_revenue", "order_date"
    )
    if as_of is None:
        as_of = str(orders.agg(F.max("order_date")).first()[0])

    out = compute_rfm(orders, as_of, lookback_months)
    path = f"{bronze}/{GOLD_RELPATH}"
    out.write.mode("overwrite").parquet(path)
    print(f"[rfm] as_of={as_of} lookback={lookback_months}m rows={out.count()} -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Gold — RFM segmentation")
    p.add_argument("--bronze", required=True)
    p.add_argument("--as-of", help="Reference date (default: max order_date)")
    p.add_argument("--lookback-months", type=int, default=12)
    args = p.parse_args(argv)

    spark = SparkSession.builder.appName("gpb-gold-rfm").getOrCreate()
    run(spark, args.bronze, args.as_of, args.lookback_months)
    spark.stop()


if __name__ == "__main__":
    main()
