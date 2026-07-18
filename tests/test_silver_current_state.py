"""Unit tests for the Silver current-state collapse (log-based CDC merge)."""
import pytest
from pyspark.sql import SparkSession

from jobs.common.config import TableSpec
from jobs.silver.current_state import collapse, collapse_keyed, collapse_multiset

KEYED = TableSpec(name="order_items", keys=["order_id", "lineitem_id"])
MULTI = TableSpec(name="order_item_options", keys=None)

# change-log columns = source cols + CDC headers
LOG_COLS = ["order_id", "lineitem_id", "item_price", "Op", "cdc_seq", "cdc_commit_ts",
            "cdc_snapshot_dt", "cdc_processed_at"]


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.appName("silver-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _log(spark, rows):
    """rows: (order_id, lineitem_id, item_price, Op, cdc_seq)."""
    data = [(o, li, pr, op, seq, "2026-07-16 00:00:00", "2026-07-16", "2026-07-16 01:00:00")
            for (o, li, pr, op, seq) in rows]
    return spark.createDataFrame(data, LOG_COLS)


def _keys(df):
    return {(r["order_id"], r["lineitem_id"]): r for r in df.collect()}


def test_keyed_latest_sequence_wins(spark):
    # l1 inserted then updated (higher seq) -> current price is the update's.
    log = _log(spark, [
        ("o1", "l1", 5.0, "I", "00000000000000100000000000000000001"),
        ("o1", "l1", 8.0, "U", "00000000000000200000000000000000001"),
    ])
    out = _keys(collapse_keyed(log, KEYED.keys))
    assert len(out) == 1
    assert out[("o1", "l1")]["item_price"] == 8.0
    assert "Op" not in out[("o1", "l1")]  # CDC headers stripped


def test_keyed_delete_tombstone_removed(spark):
    # l1 inserted then deleted (higher seq) -> not in current state.
    log = _log(spark, [
        ("o1", "l1", 5.0, "I", "00000000000000100000000000000000001"),
        ("o1", "l1", 5.0, "D", "00000000000000200000000000000000001"),
        ("o1", "l2", 3.0, "I", "00000000000000100000000000000000002"),
    ])
    out = _keys(collapse_keyed(log, KEYED.keys))
    assert ("o1", "l1") not in out
    assert ("o1", "l2") in out


def test_keyed_reinsert_after_delete_survives(spark):
    # delete then a later re-insert -> row is present again.
    log = _log(spark, [
        ("o1", "l1", 5.0, "I", "00000000000000100000000000000000001"),
        ("o1", "l1", 5.0, "D", "00000000000000200000000000000000001"),
        ("o1", "l1", 9.0, "I", "00000000000000300000000000000000001"),
    ])
    out = _keys(collapse_keyed(log, KEYED.keys))
    assert out[("o1", "l1")]["item_price"] == 9.0


OPT_LOG_COLS = ["order_id", "lineitem_id", "option_name", "Op", "cdc_seq", "cdc_commit_ts",
                "cdc_snapshot_dt", "cdc_processed_at"]


def _optlog(spark, rows):
    data = [(o, li, nm, op, seq, "2026-07-16 00:00:00", "2026-07-16", "2026-07-16 01:00:00")
            for (o, li, nm, op, seq) in rows]
    return spark.createDataFrame(data, OPT_LOG_COLS)


def test_multiset_net_count_survives(spark):
    # "Cilantro" inserted 3x, deleted 1x  -> net 2 copies in current state.
    log = _optlog(spark, [
        ("o1", "l1", "Cilantro", "I", "s1"),
        ("o1", "l1", "Cilantro", "I", "s1"),
        ("o1", "l1", "Cilantro", "I", "s2"),
        ("o1", "l1", "Cilantro", "D", "s3"),
        ("o2", "l1", "Onion", "I", "s1"),
    ])
    out = collapse_multiset(log).collect()
    cilantro = [r for r in out if r["option_name"] == "Cilantro"]
    onion = [r for r in out if r["option_name"] == "Onion"]
    assert len(cilantro) == 2
    assert len(onion) == 1
    assert "Op" not in out[0]


def test_collapse_dispatches_on_spec(spark):
    keyed_log = _log(spark, [("o1", "l1", 5.0, "I", "s1")])
    assert collapse(keyed_log, KEYED).count() == 1
    multi_log = _optlog(spark, [("o1", "l1", "Cilantro", "I", "s1")])
    assert collapse(multi_log, MULTI).count() == 1
