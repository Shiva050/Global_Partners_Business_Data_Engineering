"""Gold — churn activity indicators (descriptive, no prediction).

Per customer, a marketing-facing activity profile:
  days_since_last_order : as_of - last order
  avg_gap_days          : mean days between consecutive orders = span/(orders-1)
  recent/prior_spend    : spend in the last period vs the preceding period
  spend_change_pct      : % change between those two periods (trend signal)
  churn_status          : Active / At Risk / Dormant from inactivity thresholds

The BRD's ">45 days = at risk" is the Active/At-Risk boundary; a second
threshold (default 90) marks Dormant. All thresholds and the period length are
parameters. Point-in-time snapshot, full recompute each run.

Reads  <bronze>/silver/facts/orders
Writes <bronze>/gold/churn_indicators/

Run:
    spark-submit jobs/gold/churn_indicators.py --bronze s3://... \
        [--at-risk-days 45] [--dormant-days 90] [--period-days 30] [--as-of 2023-12-31]
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GOLD_RELPATH = "gold/churn_indicators"


def compute_churn_indicators(
    orders: DataFrame,
    as_of: str,
    at_risk_days: int = 45,
    dormant_days: int = 90,
    period_days: int = 30,
) -> DataFrame:
    """orders: user_id, order_revenue, order_date (date). One row per customer."""
    as_of_c = F.to_date(F.lit(as_of))

    base = (
        orders.groupBy("user_id")
        .agg(
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.count(F.lit(1)).alias("total_orders"),
        )
        .withColumn("days_since_last_order", F.datediff(as_of_c, F.col("last_order_date")))
        .withColumn(
            "avg_gap_days",
            F.when(
                F.col("total_orders") > 1,
                F.datediff(F.col("last_order_date"), F.col("first_order_date"))
                / (F.col("total_orders") - 1),
            ),
        )
    )

    # Recent vs prior period spend (trend).
    p = period_days
    recent = (
        orders.filter(
            (F.col("order_date") > F.date_sub(as_of_c, p)) & (F.col("order_date") <= as_of_c)
        )
        .groupBy("user_id")
        .agg(F.sum(F.col("order_revenue").cast("double")).alias("recent_spend"))
    )
    prior = (
        orders.filter(
            (F.col("order_date") > F.date_sub(as_of_c, 2 * p))
            & (F.col("order_date") <= F.date_sub(as_of_c, p))
        )
        .groupBy("user_id")
        .agg(F.sum(F.col("order_revenue").cast("double")).alias("prior_spend"))
    )

    out = (
        base.join(recent, "user_id", "left")
        .join(prior, "user_id", "left")
        .withColumn("recent_spend", F.coalesce(F.col("recent_spend"), F.lit(0.0)))
        .withColumn("prior_spend", F.coalesce(F.col("prior_spend"), F.lit(0.0)))
        .withColumn(
            "spend_change_pct",
            F.when(
                F.col("prior_spend") > 0,
                (F.col("recent_spend") - F.col("prior_spend")) / F.col("prior_spend") * 100,
            ),
        )
    )

    days = F.col("days_since_last_order")
    status = (
        F.when(days <= at_risk_days, F.lit("Active"))
        .when(days <= dormant_days, F.lit("At Risk"))
        .otherwise(F.lit("Dormant"))
    )
    return (
        out.withColumn("churn_status", status)
        .withColumn("at_risk", days > at_risk_days)
        .withColumn("as_of_date", as_of_c)
        .select(
            "user_id", "as_of_date", "first_order_date", "last_order_date", "total_orders",
            "days_since_last_order", "avg_gap_days", "recent_spend", "prior_spend",
            "spend_change_pct", "churn_status", "at_risk",
        )
    )


def run(spark, bronze, as_of, at_risk_days, dormant_days, period_days):
    orders = spark.read.parquet(f"{bronze}/silver/facts/orders").select(
        "user_id", "order_revenue", "order_date"
    ).filter(F.col("user_id").isNotNull())  # exclude anonymous/guest orders
    if as_of is None:
        as_of = str(orders.agg(F.max("order_date")).first()[0])
    out = compute_churn_indicators(orders, as_of, at_risk_days, dormant_days, period_days)
    path = f"{bronze}/{GOLD_RELPATH}"
    out.write.mode("overwrite").parquet(path)
    print(f"[churn] as_of={as_of} rows={out.count()} -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Gold — churn activity indicators")
    p.add_argument("--bronze", required=True)
    p.add_argument("--as-of", help="Reference date (default: max order_date)")
    p.add_argument("--at-risk-days", type=int, default=45)
    p.add_argument("--dormant-days", type=int, default=90)
    p.add_argument("--period-days", type=int, default=30)
    args, _ = p.parse_known_args(argv)  # ignore Glue-injected args (--JOB_NAME etc.)

    spark = SparkSession.builder.appName("gpb-gold-churn").getOrCreate()
    run(spark, args.bronze, args.as_of, args.at_risk_days, args.dormant_days, args.period_days)
    spark.stop()


if __name__ == "__main__":
    main()
