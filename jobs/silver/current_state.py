"""Silver — collapse the CDC change log into current-state tables.

Applies the append-only I/U/D log per key using the LSN-style `cdc_seq`:
**latest sequence wins, tombstones (Op=D) removed** — the canonical log-based
CDC merge (identical to what you'd run against real DMS/Debezium CDC).

- Keyed tables (order_items, date_dim): collapse by natural key.
- Keyless order_item_options (multiset): collapse by net count over the full
  change history (Σ +1 for I, -1 for D per distinct row).

Reads  <bronze>/cdc/<table>/            (all dt= partitions = full history)
Writes <bronze>/silver/current/<table>/ (one row per current key)

Run:
    spark-submit jobs/silver/current_state.py --bronze s3://dms-...-bronze
"""
import argparse
from typing import List

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

try:
    from jobs.common.config import TABLES, CDC_TABLES, CDC_HEADER_COLS, TableSpec
except ModuleNotFoundError:  # spark-submit ships files flat
    from common.config import TABLES, CDC_TABLES, CDC_HEADER_COLS, TableSpec  # type: ignore

# Hive-style partition column Spark infers from the cdc/<table>/dt=<date>/ layout.
PARTITION_COLS = ["dt"]


def source_columns(df: DataFrame) -> List[str]:
    """Change-log columns minus CDC headers and partition columns -> the source row."""
    drop = set(CDC_HEADER_COLS) | set(PARTITION_COLS)
    return [c for c in df.columns if c not in drop]


def collapse_keyed(log_df: DataFrame, keys: List[str]) -> DataFrame:
    """Latest change per key wins; drop rows whose latest change is a delete."""
    src = source_columns(log_df)
    w = Window.partitionBy(*keys).orderBy(F.col("cdc_seq").desc())
    latest = log_df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1)
    return latest.filter(F.col("Op") != F.lit("D")).select(*src)


def collapse_multiset(log_df: DataFrame) -> DataFrame:
    """Net count per distinct row over the whole history; emit that many copies.

    A keyless row present in `k` inserts and `d` deletes survives `k-d` times.
    """
    src = source_columns(log_df)
    net = (
        log_df.groupBy(*src)
        .agg(F.sum(F.when(F.col("Op") == F.lit("I"), 1).otherwise(-1)).alias("_net"))
        .filter(F.col("_net") > 0)
        .withColumn("_copy", F.explode(F.sequence(F.lit(1), F.col("_net"))))
    )
    return net.select(*src)


def collapse(log_df: DataFrame, spec: TableSpec) -> DataFrame:
    if spec.keys:
        return collapse_keyed(log_df, spec.keys)
    return collapse_multiset(log_df)


def run_table(spark: SparkSession, bronze: str, spec: TableSpec) -> int:
    log = spark.read.parquet(f"{bronze}/cdc/{spec.name}")
    for pc in PARTITION_COLS:
        if pc in log.columns:
            log = log.drop(pc)
    current = collapse(log, spec)
    out = f"{bronze}/silver/current/{spec.name}"
    current.write.mode("overwrite").parquet(out)
    n = current.count()
    print(f"[current_state] {spec.name}: rows = {n} -> {out}")
    return n


def main(argv=None):
    p = argparse.ArgumentParser(description="Silver — collapse CDC to current state")
    p.add_argument("--bronze", required=True)
    p.add_argument("--tables", help="Comma-separated subset (default: all)")
    args = p.parse_args(argv)

    spark = SparkSession.builder.appName("gpb-silver-current-state").getOrCreate()
    names = args.tables.split(",") if args.tables else list(CDC_TABLES)
    for name in names:
        spec = TABLES.get(name.strip())
        if spec is None or spec.static:
            continue
        run_table(spark, args.bronze, spec)
    spark.stop()


if __name__ == "__main__":
    main()
