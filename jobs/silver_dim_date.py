"""Silver — load the static date dimension (no CDC).

date_dim is a static calendar dimension, so it skips the CDC machinery entirely.
This job reads the latest bronze snapshot, normalizes column names, enforces an
explicit typed schema (the raw types drift from the spec — e.g. month arrives as
an int, date_key as a string), de-duplicates on the key, and writes a conformed
dimension.

Reads  <bronze>/snapshots/dt=<latest>/gpb/date_dim/
Writes <bronze>/silver/current/date_dim/

Run:
    spark-submit jobs/silver_dim_date.py --bronze s3://dms-...-bronze
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from jobs.cdc_config import TABLES, DMS_META_COLS
    from jobs.cdc_snapshot_diff import normalize_columns, source_columns, list_snapshot_dates
except ModuleNotFoundError:  # spark-submit ships files flat
    from cdc_config import TABLES, DMS_META_COLS  # type: ignore
    from cdc_snapshot_diff import normalize_columns, source_columns, list_snapshot_dates  # type: ignore

# Enforced target schema for the conformed dimension (Spark cast types).
DATE_DIM_TYPES = {
    "date_key": "date",
    "year": "int",
    "month": "int",
    "week": "int",
    "day_of_week": "string",
    "is_weekend": "boolean",
    "is_holiday": "boolean",
    "holiday_name": "string",
}


def build_dim_date(snapshot_df: DataFrame) -> DataFrame:
    """Normalize casing, drop DMS metadata, cast to the target types, dedupe on key."""
    df = normalize_columns(snapshot_df)
    df = df.select(*source_columns(df, DMS_META_COLS))  # drop DMS op / ingested_at
    for col, typ in DATE_DIM_TYPES.items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(typ))
    return df.dropDuplicates(["date_key"])


def main(argv=None):
    p = argparse.ArgumentParser(description="Silver — typed static date dimension")
    p.add_argument("--bronze", required=True)
    p.add_argument("--snapshot-date", help="Snapshot to load (default: latest)")
    args = p.parse_args(argv)

    spark = SparkSession.builder.appName("gpb-silver-dim-date").getOrCreate()

    spec = TABLES["date_dim"]
    date = args.snapshot_date
    if date is None:
        dates = list_snapshot_dates(spark, f"{args.bronze}/snapshots")
        if not dates:
            raise SystemExit(f"No snapshots under {args.bronze}/snapshots")
        date = dates[-1]

    snap = spark.read.parquet(f"{args.bronze}/{spec.snapshot_relpath(date)}")
    dim = build_dim_date(snap)
    out = f"{args.bronze}/silver/current/date_dim"
    dim.write.mode("overwrite").parquet(out)
    print(f"[dim_date] rows = {dim.count()} (snapshot {date}) -> {out}")

    spark.stop()


if __name__ == "__main__":
    main()
