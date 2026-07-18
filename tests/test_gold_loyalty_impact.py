"""Unit tests for loyalty impact comparison."""
import pytest
from pyspark.sql import SparkSession

from jobs.gold.loyalty_impact import compute_loyalty_impact


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.appName("loyalty-tests").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def test_loyalty_vs_non(spark):
    orders = spark.createDataFrame([
        ("u1", 100.0, True), ("u1", 50.0, True),   # loyalty, 2 orders -> repeat
        ("u2", 40.0, True),                         # loyalty, 1 order
        ("u3", 20.0, False),                        # non, 1 order
        ("u4", 30.0, False), ("u4", 30.0, False), ("u4", 30.0, False),  # non, 3 orders
    ], "user_id string, order_revenue double, is_loyalty boolean")

    out = {r["segment"]: r for r in compute_loyalty_impact(orders).collect()}

    loy = out["Loyalty"]
    assert loy["customers"] == 2 and loy["total_orders"] == 3
    assert loy["total_revenue"] == 190.0
    assert loy["avg_order_value"] == pytest.approx(190 / 3)
    assert loy["avg_clv"] == pytest.approx(95.0)
    assert loy["repeat_rate"] == pytest.approx(0.5)   # u1 repeats, u2 doesn't

    non = out["Non-Loyalty"]
    assert non["customers"] == 2 and non["total_orders"] == 4
    assert non["avg_order_value"] == pytest.approx(27.5)
    assert non["repeat_rate"] == pytest.approx(0.5)   # u4 repeats, u3 doesn't
