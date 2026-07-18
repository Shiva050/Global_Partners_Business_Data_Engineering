"""Gold — Customer Lifetime Value, evolving daily (the assessment's primary goal).

Grain: one row per (user_id, clv_date), **dense** from each customer's first
order date through `as_of` — so CLV is defined for every day of their lifetime,
carrying forward on days with no order.

    daily_revenue          = Σ order_revenue for the customer on that day (0 if none)
    cumulative_clv         = running Σ daily_revenue up to and including clv_date
    cumulative_order_count = running Σ orders (also the RFM "frequency-to-date")

Incremental & late-arriving data
--------------------------------
CLV is a running sum, so a late order dated D invalidates the cumulative for
that customer on every day >= D. We therefore recompute a **window** and
overwrite only the affected month partitions:

    floor   = trunc(batch_min_order_date, 'month')     # first day of that month
    as_of   = max(order_date)                          # latest processed day
    base(C) = existing cumulative_clv at (floor - 1)   # seed so we don't
                                                        # recompute all history
    cumulative(C, d) = base(C) + running_sum(daily_revenue over [floor .. d])

Output is partitioned by `clv_month`; a dynamic overwrite replaces only the
partitions >= floor, leaving earlier months untouched. The full population is
recomputed within the window (not just changed customers) — required because a
partition overwrite replaces the whole month, so every active customer must be
re-emitted (flat carry-forward when they had no orders in the window).

Reads  <bronze>/silver/facts/orders , <bronze>/silver/current/date_dim
Writes <bronze>/gold/customer_clv_daily/  (partitioned by clv_month)

Run:
    # full backfill (first run)
    spark-submit jobs/gold/customer_clv_daily.py --bronze s3://... --full
    # incremental batch (orchestrator passes the batch's min order date)
    spark-submit jobs/gold/customer_clv_daily.py --bronze s3://... \
        --batch-min-order-date 2023-06-01
"""
import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

GOLD_RELPATH = "gold/customer_clv_daily"


def compute_clv_daily(
    orders: DataFrame,
    date_dim: DataFrame,
    floor: str,
    as_of: str,
    baseline: DataFrame = None,
) -> DataFrame:
    """Dense per-customer daily CLV over [floor .. as_of], seeded by `baseline`.

    orders   : user_id, order_revenue, order_date (date)
    date_dim : date_key (date) — the calendar spine
    floor/as_of : 'YYYY-MM-DD' inclusive window bounds
    baseline : optional (user_id, base_clv, base_orders) = cumulative at floor-1;
               None => full mode (everyone starts at 0)
    """
    floor_c, as_of_c = F.to_date(F.lit(floor)), F.to_date(F.lit(as_of))

    # First order per customer over ALL orders (bounds the dense start).
    first_order = orders.groupBy("user_id").agg(F.min("order_date").alias("first_order_date"))

    # Daily revenue within the window.
    daily = (
        orders.filter((F.col("order_date") >= floor_c) & (F.col("order_date") <= as_of_c))
        .groupBy("user_id", "order_date")
        .agg(
            F.sum(F.col("order_revenue").cast("double")).alias("daily_revenue"),
            F.count(F.lit(1)).alias("daily_order_count"),
        )
        .select(
            F.col("user_id").alias("d_user"),
            F.col("order_date").alias("d_date"),
            "daily_revenue",
            "daily_order_count",
        )
    )

    # Calendar spine restricted to the window.
    spine = date_dim.select(F.col("date_key").alias("clv_date")).filter(
        (F.col("clv_date") >= floor_c) & (F.col("clv_date") <= as_of_c)
    )

    # Dense (customer × day) from the customer's window start to as_of.
    cust = first_order.withColumn("window_start", F.greatest(F.col("first_order_date"), floor_c))
    dense = cust.crossJoin(spine).filter(F.col("clv_date") >= F.col("window_start"))

    dense = (
        dense.join(
            daily,
            (F.col("user_id") == F.col("d_user")) & (F.col("clv_date") == F.col("d_date")),
            "left",
        )
        .drop("d_user", "d_date")
        .withColumn("daily_revenue", F.coalesce(F.col("daily_revenue"), F.lit(0.0)))
        .withColumn("daily_order_count", F.coalesce(F.col("daily_order_count"), F.lit(0)))
    )

    # Running totals within the window.
    w = Window.partitionBy("user_id").orderBy("clv_date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    dense = dense.withColumn("_run_rev", F.sum("daily_revenue").over(w)).withColumn(
        "_run_cnt", F.sum("daily_order_count").over(w)
    )

    # Seed with the baseline cumulative entering the window.
    if baseline is not None:
        dense = (
            dense.join(baseline, "user_id", "left")
            .withColumn("base_clv", F.coalesce(F.col("base_clv"), F.lit(0.0)))
            .withColumn("base_orders", F.coalesce(F.col("base_orders"), F.lit(0)))
        )
    else:
        dense = dense.withColumn("base_clv", F.lit(0.0)).withColumn("base_orders", F.lit(0))

    return (
        dense.withColumn("cumulative_clv", F.col("base_clv") + F.col("_run_rev"))
        .withColumn("cumulative_order_count", F.col("base_orders") + F.col("_run_cnt"))
        .withColumn("clv_month", F.date_format(F.col("clv_date"), "yyyy-MM"))
        .select(
            "user_id",
            "clv_date",
            "clv_month",
            "first_order_date",
            "daily_revenue",
            "daily_order_count",
            "cumulative_clv",
            "cumulative_order_count",
        )
    )


def _month_floor(date_str: str) -> str:
    """First day of the month containing date_str (e.g. 2023-06-14 -> 2023-06-01)."""
    return date_str[:7] + "-01"


def run(spark, bronze, full, batch_min_order_date, as_of):
    orders = spark.read.parquet(f"{bronze}/silver/facts/orders").select(
        "user_id", "order_revenue", "order_date"
    )
    date_dim = spark.read.parquet(f"{bronze}/silver/current/date_dim")

    if as_of is None:
        as_of = str(orders.agg(F.max("order_date")).first()[0])

    gold_path = f"{bronze}/{GOLD_RELPATH}"
    baseline = None

    if full or batch_min_order_date is None:
        floor = str(orders.agg(F.min("order_date")).first()[0])
    else:
        floor = _month_floor(batch_min_order_date)
        # Seed baseline from the existing table at floor-1 (skip if not present).
        try:
            prev = spark.read.parquet(gold_path)
            floor_minus_1 = F.date_sub(F.to_date(F.lit(floor)), 1)
            baseline = prev.filter(F.col("clv_date") == floor_minus_1).select(
                "user_id",
                F.col("cumulative_clv").alias("base_clv"),
                F.col("cumulative_order_count").alias("base_orders"),
            )
        except Exception:
            print("No existing gold table; running full compute.")
            floor = str(orders.agg(F.min("order_date")).first()[0])

    out = compute_clv_daily(orders, date_dim, floor, as_of, baseline)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    out.write.mode("overwrite").partitionBy("clv_month").parquet(gold_path)
    print(f"[clv_daily] window [{floor} .. {as_of}] rows = {out.count()} -> {gold_path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Gold — daily customer CLV")
    p.add_argument("--bronze", required=True)
    p.add_argument("--full", action="store_true", help="Full backfill (recompute everything)")
    p.add_argument("--batch-min-order-date", help="Min order_date of the current ingest batch")
    p.add_argument("--as-of", help="Window end (default: max order_date)")
    args, _ = p.parse_known_args(argv)  # ignore Glue-injected args (--JOB_NAME etc.)

    spark = SparkSession.builder.appName("gpb-gold-clv-daily").getOrCreate()
    run(spark, args.bronze, args.full, args.batch_min_order_date, args.as_of)
    spark.stop()


if __name__ == "__main__":
    main()
