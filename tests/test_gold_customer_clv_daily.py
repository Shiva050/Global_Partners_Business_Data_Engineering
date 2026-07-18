"""Unit tests for daily CLV: dense fill, first-order bounding, and the
late-arriving-order recompute (baseline-seeded window)."""
import datetime

import pytest
from pyspark.sql import SparkSession

from jobs.gold_customer_clv_daily import compute_clv_daily

D = datetime.date


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.appName("clv-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _orders(spark, rows):
    return spark.createDataFrame(rows, "user_id string, order_revenue double, order_date date")


def _date_dim(spark, start, end):
    rows, d = [], start
    while d <= end:
        rows.append((d,))
        d += datetime.timedelta(days=1)
    return spark.createDataFrame(rows, "date_key date")


def _by(df):
    return {(r["user_id"], r["clv_date"]): r for r in df.collect()}


def test_full_dense_and_cumulative(spark):
    orders = _orders(spark, [
        ("u1", 10.0, D(2023, 1, 5)),
        ("u1", 20.0, D(2023, 3, 1)),
        ("u2", 50.0, D(2023, 2, 10)),
    ])
    dd = _date_dim(spark, D(2023, 1, 1), D(2023, 3, 5))
    out = _by(compute_clv_daily(orders, dd, floor="2023-01-05", as_of="2023-03-05"))

    # u1 dense from first order (Jan 5); nothing before it
    assert ("u1", D(2023, 1, 4)) not in out
    assert out[("u1", D(2023, 1, 5))]["daily_revenue"] == 10.0
    assert out[("u1", D(2023, 1, 5))]["cumulative_clv"] == 10.0
    # carry-forward on a no-order day
    assert out[("u1", D(2023, 2, 15))]["daily_revenue"] == 0.0
    assert out[("u1", D(2023, 2, 15))]["cumulative_clv"] == 10.0
    # second order accumulates
    assert out[("u1", D(2023, 3, 1))]["cumulative_clv"] == 30.0
    assert out[("u1", D(2023, 3, 1))]["cumulative_order_count"] == 2
    assert out[("u1", D(2023, 3, 5))]["cumulative_clv"] == 30.0

    # u2 bounded to its own first order (Feb 10)
    assert ("u2", D(2023, 2, 9)) not in out
    assert out[("u2", D(2023, 2, 10))]["cumulative_clv"] == 50.0
    assert out[("u2", D(2023, 3, 5))]["cumulative_clv"] == 50.0


def test_late_order_recompute_shifts_downstream(spark):
    # Prior state: u1 had only Jan 5 = 10  -> baseline cumulative at Jan 31 = 10.
    # A late Feb 15 order (+5) arrives with a Mar 1 order (+20).
    orders = _orders(spark, [
        ("u1", 10.0, D(2023, 1, 5)),    # < floor: represented by baseline
        ("u1", 5.0, D(2023, 2, 15)),    # late arrival inside window
        ("u1", 20.0, D(2023, 3, 1)),
    ])
    dd = _date_dim(spark, D(2023, 1, 1), D(2023, 3, 5))
    baseline = spark.createDataFrame(
        [("u1", 10.0, 1)], "user_id string, base_clv double, base_orders int"
    )
    out = _by(compute_clv_daily(orders, dd, floor="2023-02-01", as_of="2023-03-05", baseline=baseline))

    # window starts at floor carrying the baseline forward
    assert out[("u1", D(2023, 2, 1))]["cumulative_clv"] == 10.0
    # late order lands
    assert out[("u1", D(2023, 2, 15))]["daily_revenue"] == 5.0
    assert out[("u1", D(2023, 2, 15))]["cumulative_clv"] == 15.0
    # the KEY property: the later order's cumulative reflects the late one (35, not 30)
    assert out[("u1", D(2023, 3, 1))]["cumulative_clv"] == 35.0
    assert out[("u1", D(2023, 3, 1))]["cumulative_order_count"] == 3
    # nothing emitted before the window floor
    assert ("u1", D(2023, 1, 20)) not in out


def test_new_customer_in_window_has_zero_baseline(spark):
    # u3 first orders inside the window -> no baseline row -> starts from 0.
    orders = _orders(spark, [("u3", 40.0, D(2023, 2, 20))])
    dd = _date_dim(spark, D(2023, 1, 1), D(2023, 3, 5))
    baseline = spark.createDataFrame([], "user_id string, base_clv double, base_orders int")
    out = _by(compute_clv_daily(orders, dd, floor="2023-02-01", as_of="2023-03-05", baseline=baseline))
    assert ("u3", D(2023, 2, 19)) not in out
    assert out[("u3", D(2023, 2, 20))]["cumulative_clv"] == 40.0
