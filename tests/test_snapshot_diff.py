"""Unit tests for the snapshot-diff CDC logic.

Runs a local SparkSession (no S3). Exercises baseline, insert, update, delete,
and unchanged handling, plus the delete-emits-last-known-row behaviour.
"""
import pytest
from pyspark.sql import SparkSession

from jobs.cdc.config import TableSpec
from jobs.cdc.snapshot_diff import compute_cdc, source_columns

SPEC = TableSpec(name="order_items", keys=["order_id", "lineitem_id"])
META = ["op", "ingested_at"]
# DMS snapshot columns (lowercased on read by the job); "op" here is the DMS
# source-side operation flag, dropped and re-derived by the diff.
COLS = ["order_id", "lineitem_id", "item_price", "ingested_at", "op"]


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.appName("cdc-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _snap(spark, rows):
    """rows: list of (order_id, lineitem_id, item_price). ingested_at/Op are DMS metadata."""
    data = [(o, li, pr, "2026-07-16T00:00:00", "I") for (o, li, pr) in rows]
    return spark.createDataFrame(data, COLS)


def _by_key(df):
    return {(r["order_id"], r["lineitem_id"]): r for r in df.collect()}


def test_source_columns_excludes_dms_metadata(spark):
    df = _snap(spark, [("o1", "l1", 5.0)])
    assert set(source_columns(df, META)) == {"order_id", "lineitem_id", "item_price"}


def test_baseline_all_inserts(spark):
    cur = _snap(spark, [("o1", "l1", 5.0), ("o1", "l2", 3.0)])
    out = compute_cdc(cur, None, SPEC, "2026-07-16")
    ops = [r["Op"] for r in out.collect()]
    assert ops == ["I", "I"]
    # payload carries source columns, not DMS metadata
    assert "ingested_at" not in out.columns
    assert {"order_id", "lineitem_id", "item_price", "Op", "cdc_snapshot_dt"} <= set(out.columns)


def test_insert_update_delete_unchanged(spark):
    prev = _snap(spark, [
        ("o1", "l1", 5.0),   # will stay unchanged
        ("o1", "l2", 3.0),   # will be updated (price change)
        ("o1", "l3", 9.0),   # will be deleted
    ])
    cur = _snap(spark, [
        ("o1", "l1", 5.0),   # unchanged -> dropped
        ("o1", "l2", 4.0),   # updated
        ("o1", "l4", 1.0),   # new -> insert
    ])
    out = _by_key(compute_cdc(cur, prev, SPEC, "2026-07-16"))

    assert ("o1", "l1") not in out                     # unchanged row is not emitted
    assert out[("o1", "l2")]["Op"] == "U"
    assert out[("o1", "l2")]["item_price"] == 4.0      # update emits the NEW value
    assert out[("o1", "l4")]["Op"] == "I"
    assert out[("o1", "l3")]["Op"] == "D"
    assert out[("o1", "l3")]["item_price"] == 9.0      # delete emits last-known value


def test_cdc_seq_orders_later_batch_after_earlier(spark):
    # The LSN mimic: a change in a later batch must sort AFTER the same row's
    # change in an earlier batch, so Silver's "latest cdc_seq wins" collapse works.
    cur = _snap(spark, [("o1", "l1", 5.0)])
    early = compute_cdc(cur, None, SPEC, "2026-07-15", batch_ts="2026-07-15").collect()[0]
    late = compute_cdc(cur, None, SPEC, "2026-07-16", batch_ts="2026-07-16").collect()[0]
    assert "cdc_seq" in early and "cdc_commit_ts" in early
    assert late["cdc_seq"] > early["cdc_seq"]          # string compare, zero-padded -> chronological


def test_uppercase_source_columns_are_normalized(spark):
    # order_* snapshots arrive UPPERCASE; the job must lowercase them so the
    # config's lowercase keys align and downstream schema is uniform.
    up_cols = ["ORDER_ID", "LINEITEM_ID", "ITEM_PRICE", "ingested_at", "Op"]
    prev = spark.createDataFrame([("o1", "l1", 5.0, "2026-07-15T00:00:00", "I")], up_cols)
    cur = spark.createDataFrame([("o1", "l1", 7.0, "2026-07-16T00:00:00", "I")], up_cols)
    out = compute_cdc(cur, prev, SPEC, "2026-07-16")
    assert "order_id" in out.columns and "ORDER_ID" not in out.columns
    assert "item_price" in out.columns
    rows = out.collect()
    assert len(rows) == 1 and rows[0]["Op"] == "U" and rows[0]["item_price"] == 7.0


def test_null_change_is_detected_as_update(spark):
    prev = _snap(spark, [("o1", "l1", 5.0)])
    # Reuse prev's schema so the all-NULL item_price column has a defined type
    # (Spark can't infer a type for a column that is NULL in every row).
    cur_data = [("o1", "l1", None, "2026-07-16T00:00:00", "I")]
    cur = spark.createDataFrame(cur_data, prev.schema)
    out = _by_key(compute_cdc(cur, prev, SPEC, "2026-07-16"))
    assert out[("o1", "l1")]["Op"] == "U"
    assert out[("o1", "l1")]["item_price"] is None


# --- multiset (keyless) diff, for order_item_options ---------------------------
OPT_SPEC = TableSpec(name="order_item_options", keys=None)
OPT_COLS = ["order_id", "lineitem_id", "option_name", "option_price", "ingested_at", "op"]


def _opt(spark, rows):
    data = [(o, li, nm, pr, "2026-07-16T00:00:00", "I") for (o, li, nm, pr) in rows]
    return spark.createDataFrame(data, OPT_COLS)


def test_multiset_baseline_preserves_duplicates(spark):
    # Two fully-identical rows must both survive as inserts (multiplicity matters).
    cur = _opt(spark, [("o1", "l1", "Cilantro", 0.0), ("o1", "l1", "Cilantro", 0.0)])
    out = compute_cdc(cur, None, OPT_SPEC, "2026-07-16").collect()
    assert len(out) == 2
    assert all(r["Op"] == "I" for r in out)


def test_multiset_diff_net_insert_and_delete(spark):
    prev = _opt(spark, [
        ("o1", "l1", "Cilantro", 0.0),   # count 2 -> 3  => 1 net insert
        ("o1", "l1", "Cilantro", 0.0),
        ("o2", "l1", "Onion", 0.5),      # count 2 -> 1  => 1 net delete
        ("o2", "l1", "Onion", 0.5),
    ])
    cur = _opt(spark, [
        ("o1", "l1", "Cilantro", 0.0),
        ("o1", "l1", "Cilantro", 0.0),
        ("o1", "l1", "Cilantro", 0.0),
        ("o2", "l1", "Onion", 0.5),
    ])
    out = compute_cdc(cur, prev, OPT_SPEC, "2026-07-16").collect()
    ops = sorted(r["Op"] for r in out)
    assert ops == ["D", "I"]            # exactly one insert, one delete
    ins = [r for r in out if r["Op"] == "I"][0]
    assert ins["option_name"] == "Cilantro"
    dele = [r for r in out if r["Op"] == "D"][0]
    assert dele["option_name"] == "Onion"
