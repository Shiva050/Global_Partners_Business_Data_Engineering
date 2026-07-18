"""Unit tests for portfolio CLV trends aggregation."""
import datetime

import pytest
from pyspark.sql import SparkSession

from jobs.gold.clv_trends import compute_clv_trends

D = datetime.date


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.appName("clv-trends-tests").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def test_daily_portfolio(spark):
    # u1 first orders 1/1; u2 first orders 1/2. Dense rows to 1/2.
    rows = [
        ("u1", D(2023, 1, 1), D(2023, 1, 1), 10.0, 10.0),
        ("u1", D(2023, 1, 2), D(2023, 1, 1), 0.0, 10.0),
        ("u2", D(2023, 1, 2), D(2023, 1, 2), 5.0, 5.0),
    ]
    df = spark.createDataFrame(
        rows, "user_id string, clv_date date, first_order_date date, daily_revenue double, cumulative_clv double")
    out = {r["clv_date"]: r for r in compute_clv_trends(df).collect()}

    d1 = out[D(2023, 1, 1)]
    assert d1["total_cumulative_clv"] == 10.0 and d1["active_customers"] == 1
    assert d1["new_customers"] == 1 and d1["daily_revenue"] == 10.0

    d2 = out[D(2023, 1, 2)]
    assert d2["total_cumulative_clv"] == 15.0   # u1 10 + u2 5
    assert d2["active_customers"] == 2
    assert d2["new_customers"] == 1             # only u2 is new on 1/2
    assert d2["daily_revenue"] == 5.0
