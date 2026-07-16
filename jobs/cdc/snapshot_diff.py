"""Snapshot-diff CDC job.

Derives an append-only I/U/D change log by comparing two consecutive full
snapshots of a table:

    today  FULL OUTER JOIN  yesterday   ON <natural key>

      key only in today             -> I (insert)
      key only in yesterday         -> D (delete)  (emits the last-known row)
      key in both, row hash differs -> U (update)
      key in both, row hash equal   -> unchanged (dropped)

The first snapshot (no prior) is emitted entirely as inserts.

Output shape matches what DMS CDC would have produced: the source columns plus
`Op` (I/U/D) and audit columns `cdc_snapshot_dt` / `cdc_processed_at`.

Run (EMR / spark-submit):
    spark-submit jobs/cdc/snapshot_diff.py \
        --bronze s3://dms-global-partne-brusiness-bronze \
        [--snapshot-date 2026-07-16] [--prev-date 2026-07-15] \
        [--tables order_items,order_item_options,date_dim]

If dates are omitted the job auto-detects the two most recent `dt=` snapshot
partitions under <bronze>/snapshots/.
"""
import argparse
import sys
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Support both `python -m jobs.cdc.snapshot_diff` and `spark-submit snapshot_diff.py`
try:
    from jobs.cdc.config import TABLES, DMS_META_COLS, TableSpec
except ModuleNotFoundError:  # spark-submit ships the file without the package root
    from config import TABLES, DMS_META_COLS, TableSpec  # type: ignore

NULL_SENTINEL = "__NULL__"  # sentinel so NULL != empty string when hashing


# ---------------------------------------------------------------------------
# Pure transformation helpers (unit-tested without S3)
# ---------------------------------------------------------------------------
def normalize_columns(df: DataFrame) -> DataFrame:
    """Lowercase every column name.

    Source tables arrive with inconsistent casing (order_* UPPERCASE, date_dim
    lowercase). Normalizing here keeps the derived change log — and everything
    downstream — on a single, predictable lowercase schema.
    """
    return df.toDF(*[c.lower() for c in df.columns])


def source_columns(df: DataFrame, meta_cols: List[str] = DMS_META_COLS) -> List[str]:
    """Business columns only — DMS metadata (op, ingested_at) removed."""
    return [c for c in df.columns if c not in meta_cols]


def row_hash(cols: List[str]):
    """A single SHA-256 over the ordered source columns.

    NULLs map to a sentinel so NULL != '' and hashing is deterministic.
    """
    ordered = sorted(cols)
    parts = [F.coalesce(F.col(c).cast("string"), F.lit(NULL_SENTINEL)) for c in ordered]
    return F.sha2(F.concat_ws("||", *parts), 256)


def _prep(df: DataFrame, src_cols: List[str], keys: List[str], side: str) -> DataFrame:
    """Reduce a snapshot to: key columns + a struct of all source columns + its hash."""
    return df.select(
        *keys,
        F.struct(*[F.col(c) for c in src_cols]).alias(f"{side}_row"),
        row_hash(src_cols).alias(f"{side}_hash"),
    )


def _add_audit(df: DataFrame, snapshot_date: str) -> DataFrame:
    return df.withColumn("cdc_snapshot_dt", F.lit(snapshot_date)).withColumn(
        "cdc_processed_at", F.current_timestamp()
    )


def _keyed_diff(cur_df, prev_df, keys, src_cols, snapshot_date) -> DataFrame:
    """I/U/D via full-outer join on a unique natural key + row hash."""
    cur = _prep(cur_df, src_cols, keys, "cur")
    prev = _prep(prev_df, src_cols, keys, "prev")
    joined = cur.join(prev, on=keys, how="full_outer")

    op = (
        F.when(F.col("prev_hash").isNull(), F.lit("I"))
        .when(F.col("cur_hash").isNull(), F.lit("D"))
        .when(F.col("cur_hash") != F.col("prev_hash"), F.lit("U"))
        .otherwise(F.lit("N"))
    )
    # Deletes emit the last-known (previous) row; inserts/updates emit current.
    payload = F.when(F.col("cur_hash").isNull(), F.col("prev_row")).otherwise(F.col("cur_row"))

    changes = (
        joined.withColumn("Op", op)
        .filter(F.col("Op") != F.lit("N"))
        .withColumn("_payload", payload)
        .select("_payload.*", "Op")
    )
    return _add_audit(changes, snapshot_date)


def _multiset_diff(cur_df, prev_df, src_cols, snapshot_date) -> DataFrame:
    """I/D via count comparison of full rows, for tables with no unique key.

    A row present `n` times now and `m` times before yields `n-m` net inserts
    (Op=I) when positive, or `m-n` net deletes (Op=D) when negative. Exact
    multiplicity is preserved by exploding the net delta into that many rows.
    There is no U — an in-place change of a keyless row is a delete + insert.
    """
    cc = cur_df.groupBy(*src_cols).count().withColumnRenamed("count", "cnt_cur")
    pc = prev_df.groupBy(*src_cols).count().withColumnRenamed("count", "cnt_prev")

    joined = (
        cc.join(pc, on=src_cols, how="full_outer")
        .withColumn("cnt_cur", F.coalesce("cnt_cur", F.lit(0)))
        .withColumn("cnt_prev", F.coalesce("cnt_prev", F.lit(0)))
        .withColumn("delta", F.col("cnt_cur") - F.col("cnt_prev"))
        .filter(F.col("delta") != 0)
        .withColumn("Op", F.when(F.col("delta") > 0, F.lit("I")).otherwise(F.lit("D")))
        .withColumn("_copies", F.explode(F.sequence(F.lit(1), F.abs(F.col("delta")))))
    )
    changes = joined.select(*src_cols, "Op")
    return _add_audit(changes, snapshot_date)


def compute_cdc(
    cur_df: DataFrame,
    prev_df: Optional[DataFrame],
    spec: TableSpec,
    snapshot_date: str,
    meta_cols: List[str] = DMS_META_COLS,
) -> DataFrame:
    """Return the I/U/D change log between two snapshots of one table.

    `prev_df=None` means baseline (first snapshot) -> everything is an insert.
    Keyed tables (spec.keys set) use a join-based diff; keyless tables
    (spec.keys is None) use a count-based multiset diff.
    """
    cur_df = normalize_columns(cur_df)
    if prev_df is not None:
        prev_df = normalize_columns(prev_df)

    src_cols = source_columns(cur_df, meta_cols)

    if prev_df is None:
        # Baseline: emit every current row as an insert (duplicates preserved).
        baseline = cur_df.select(*src_cols).withColumn("Op", F.lit("I"))
        return _add_audit(baseline, snapshot_date)

    if spec.keys:
        return _keyed_diff(cur_df, prev_df, spec.keys, src_cols, snapshot_date)
    return _multiset_diff(cur_df, prev_df, src_cols, snapshot_date)


# ---------------------------------------------------------------------------
# I/O + orchestration (needs a real filesystem)
# ---------------------------------------------------------------------------
def list_snapshot_dates(spark: SparkSession, snapshots_root: str) -> List[str]:
    """List `dt=YYYY-MM-DD` partitions under the snapshots root, sorted ascending."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(snapshots_root)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(hpath):
        return []
    dates = []
    for st in fs.listStatus(hpath):
        name = st.getPath().getName()
        if name.startswith("dt="):
            dates.append(name[len("dt="):])
    return sorted(dates)


def resolve_dates(
    spark: SparkSession, bronze: str, snapshot_date: Optional[str], prev_date: Optional[str]
):
    """Pick (current, previous) snapshot dates, auto-detecting when not given."""
    available = list_snapshot_dates(spark, f"{bronze}/snapshots")
    if snapshot_date is None:
        if not available:
            raise SystemExit(f"No snapshots found under {bronze}/snapshots")
        snapshot_date = available[-1]
    if prev_date is None and snapshot_date in available:
        idx = available.index(snapshot_date)
        prev_date = available[idx - 1] if idx > 0 else None
    return snapshot_date, prev_date


def run_table(spark: SparkSession, bronze: str, spec: TableSpec, cur_date: str, prev_date: Optional[str]):
    cur_path = f"{bronze}/{spec.snapshot_relpath(cur_date)}"
    cur_df = spark.read.parquet(cur_path)

    prev_df = None
    if prev_date is not None:
        prev_df = spark.read.parquet(f"{bronze}/{spec.snapshot_relpath(prev_date)}")

    changes = compute_cdc(cur_df, prev_df, spec, cur_date)

    out_path = f"{bronze}/{spec.cdc_relpath(cur_date)}"
    # Overwrite this snapshot's partition so re-runs are idempotent.
    changes.write.mode("overwrite").parquet(out_path)

    counts = {r["Op"]: r["n"] for r in changes.groupBy("Op").agg(F.count("*").alias("n")).collect()}
    print(f"[{spec.name}] {cur_date} (prev={prev_date}) -> {out_path} :: {counts}")
    return counts


def main(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(description="Snapshot-diff CDC")
    p.add_argument("--bronze", required=True, help="Bronze root, e.g. s3://dms-...-bronze")
    p.add_argument("--snapshot-date", help="Current snapshot date (default: latest)")
    p.add_argument("--prev-date", help="Previous snapshot date (default: the one before current)")
    p.add_argument("--tables", help="Comma-separated subset (default: all)")
    args = p.parse_args(argv)

    spark = SparkSession.builder.appName("gpb-cdc-snapshot-diff").getOrCreate()

    cur_date, prev_date = resolve_dates(spark, args.bronze, args.snapshot_date, args.prev_date)
    names = args.tables.split(",") if args.tables else list(TABLES)

    for name in names:
        spec = TABLES.get(name.strip())
        if spec is None:
            print(f"WARN: unknown table '{name}', skipping", file=sys.stderr)
            continue
        run_table(spark, args.bronze, spec, cur_date, prev_date)

    spark.stop()


if __name__ == "__main__":
    main()
