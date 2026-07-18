"""Silver — conformed date dimension (generated, full order range).

The SOURCE date_dim only covers 2023 (365 rows), but orders span 2020-2024, so
using it directly leaves most orders with no calendar row (breaking the CLV date
spine and calendar joins). We therefore GENERATE a complete daily calendar over
the actual order range and enrich it with the source's holiday flags (accurate
for 2023; is_holiday defaults to false elsewhere).

  date_key, year, month, week, day_of_week, is_weekend  -> computed from the date
  is_holiday, holiday_name                              -> from source date_dim

Reads  <bronze>/snapshots/dt=<latest>/gpb/{date_dim, order_items}/
Writes <bronze>/silver/current/date_dim/

Run:
    spark-submit jobs/silver/dim_date.py --bronze s3://... [--start 2020-01-01] [--end 2024-12-31]
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from jobs.common.config import TABLES, DMS_META_COLS
    from jobs.bronze.snapshot_diff import normalize_columns, source_columns, list_snapshot_dates
except ModuleNotFoundError:  # spark-submit ships files flat
    from common.config import TABLES, DMS_META_COLS  # type: ignore
    from bronze.snapshot_diff import normalize_columns, source_columns, list_snapshot_dates  # type: ignore

# Types for the source date_dim (used only for holiday enrichment).
SOURCE_TYPES = {"date_key": "date", "is_holiday": "boolean", "holiday_name": "string"}


def typed_source_holidays(snapshot_df: DataFrame) -> DataFrame:
    """Normalize + type the source date_dim, keep the holiday columns."""
    df = normalize_columns(snapshot_df)
    df = df.select(*source_columns(df, DMS_META_COLS))
    for col, typ in SOURCE_TYPES.items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(typ))
    return df.select("date_key", "is_holiday", "holiday_name").dropDuplicates(["date_key"])


def date_attributes(cal: DataFrame) -> DataFrame:
    """Add calendar attributes derived purely from date_key."""
    return (
        cal.withColumn("year", F.year("date_key"))
        .withColumn("month", F.month("date_key"))
        .withColumn("week", F.weekofyear("date_key"))
        .withColumn("day_of_week", F.date_format("date_key", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7))  # 1=Sun, 7=Sat
    )


def build_date_dim(calendar: DataFrame, source_holidays: DataFrame) -> DataFrame:
    """Enrich a bare calendar (date_key rows) with attributes + source holidays."""
    cal = date_attributes(calendar)
    out = cal.join(source_holidays, "date_key", "left").withColumn(
        "is_holiday", F.coalesce(F.col("is_holiday"), F.lit(False))
    )
    return out.select(
        "date_key", "year", "month", "week", "day_of_week",
        "is_weekend", "is_holiday", "holiday_name",
    )


def _calendar(spark: SparkSession, start: str, end: str) -> DataFrame:
    return spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date_key"
    )


def run(spark, bronze, start, end):
    dates = list_snapshot_dates(spark, f"{bronze}/snapshots")
    if not dates:
        raise SystemExit(f"No snapshots under {bronze}/snapshots")
    snap = dates[-1]
    spec = TABLES["date_dim"]
    src = typed_source_holidays(spark.read.parquet(f"{bronze}/{spec.snapshot_relpath(snap)}"))

    # Derive the range from the order data (padded to whole years) unless overridden.
    if start is None or end is None:
        oi = spark.read.parquet(f"{bronze}/snapshots/dt={snap}/gpb/order_items")
        oi = oi.withColumn("_d", F.to_date("creation_time_utc"))
        b = oi.agg(F.min("_d").alias("mn"), F.max("_d").alias("mx")).first()
        start = start or f"{b['mn'].year}-01-01"
        end = end or f"{b['mx'].year}-12-31"

    out = build_date_dim(_calendar(spark, start, end), src)
    path = f"{bronze}/silver/current/date_dim"
    out.write.mode("overwrite").parquet(path)
    print(f"[dim_date] {start}..{end} rows={out.count()} -> {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Silver — conformed date dimension")
    p.add_argument("--bronze", required=True)
    p.add_argument("--start", help="Calendar start (default: Jan 1 of earliest order year)")
    p.add_argument("--end", help="Calendar end (default: Dec 31 of latest order year)")
    args, _ = p.parse_known_args(argv)  # ignore Glue-injected args (--JOB_NAME etc.)

    spark = SparkSession.builder.appName("gpb-silver-dim-date").getOrCreate()
    run(spark, args.bronze, args.start, args.end)
    spark.stop()


if __name__ == "__main__":
    main()
