"""Unit tests for RFM scoring + segmentation."""
import datetime

import pytest
from pyspark.sql import SparkSession

from jobs.gold.customer_rfm import compute_rfm

D = datetime.date


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.appName("rfm-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _orders(spark, rows):
    return spark.createDataFrame(rows, "user_id string, order_revenue double, order_date date")


# Five customers engineered so ntile(5) puts each in its own quintile and one
# lands in each target segment. Recency ranks by lifetime last order; F/M by
# window totals.
ROWS = [
    # cVIP: 5 orders, most recent, highest spend -> r5 f5 m5
    ("cVIP", 200.0, D(2023, 1, 10)), ("cVIP", 200.0, D(2023, 4, 10)),
    ("cVIP", 200.0, D(2023, 7, 10)), ("cVIP", 200.0, D(2023, 10, 10)),
    ("cVIP", 200.0, D(2023, 12, 30)),
    # cReg2: 4 orders -> r2 f4
    ("cReg2", 50.0, D(2023, 1, 5)), ("cReg2", 50.0, D(2023, 5, 5)),
    ("cReg2", 50.0, D(2023, 8, 5)), ("cReg2", 50.0, D(2023, 11, 21)),
    # cReg1: 3 orders -> r3 f3
    ("cReg1", 30.0, D(2023, 6, 1)), ("cReg1", 30.0, D(2023, 9, 1)),
    ("cReg1", 40.0, D(2023, 12, 11)),
    # cChurn: 2 orders, stale -> r1 f2
    ("cChurn", 10.0, D(2023, 8, 1)), ("cChurn", 10.0, D(2023, 9, 22)),
    # cNew: 1 order, recent -> r4 f1
    ("cNew", 10.0, D(2023, 12, 29)),
]


def _rfm(spark):
    out = compute_rfm(_orders(spark, ROWS), as_of="2023-12-31", lookback_months=12)
    return {r["user_id"]: r for r in out.collect()}


def test_segments(spark):
    r = _rfm(spark)
    assert r["cVIP"]["rfm_segment"] == "VIP"
    assert r["cNew"]["rfm_segment"] == "New"
    assert r["cChurn"]["rfm_segment"] == "Churn Risk"
    assert r["cReg1"]["rfm_segment"] == "Regular"
    assert r["cReg2"]["rfm_segment"] == "Regular"


def test_scores_and_measures(spark):
    r = _rfm(spark)
    vip = r["cVIP"]
    assert vip["r_score"] == 5 and vip["f_score"] == 5 and vip["m_score"] == 5
    assert vip["rfm_score"] == 15
    assert vip["frequency"] == 5
    assert vip["monetary"] == 1000.0
    assert vip["recency_days"] == 1            # as_of - last order (2023-12-30)
    assert vip["last_order_date"] == D(2023, 12, 30)

    assert r["cNew"]["r_score"] == 4 and r["cNew"]["f_score"] == 1
    assert r["cChurn"]["r_score"] == 1 and r["cChurn"]["f_score"] == 2


def test_out_of_window_orders_excluded_from_frequency(spark):
    # An order older than the lookback must not count toward frequency/monetary,
    # but the customer still exists (recency reflects the lifetime last order).
    rows = [
        ("u1", 100.0, D(2020, 1, 1)),   # far outside a 12-month window
        ("u1", 50.0, D(2023, 12, 1)),   # inside
    ]
    out = compute_rfm(_orders(spark, rows), as_of="2023-12-31", lookback_months=12)
    row = out.collect()[0]
    assert row["frequency"] == 1                 # only the in-window order
    assert row["monetary"] == 50.0
    assert row["last_order_date"] == D(2023, 12, 1)
