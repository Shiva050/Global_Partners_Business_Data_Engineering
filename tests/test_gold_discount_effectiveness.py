"""Unit tests for discount effectiveness comparison."""
import pytest
from pyspark.sql import SparkSession

from jobs.gold.discount_effectiveness import compute_discount_effectiveness


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.appName("discount-tests").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def test_discounted_vs_full_price(spark):
    orders = spark.createDataFrame([
        (True, 100.0, 90.0, -10.0),    # discounted
        (True, 50.0, 45.0, -5.0),      # discounted
        (False, 30.0, 30.0, 0.0),      # full price
    ], "is_discounted boolean, order_gross_revenue double, order_revenue double, order_discount_amount double")

    out = {r["segment"]: r for r in compute_discount_effectiveness(orders).collect()}

    disc = out["Discounted"]
    assert disc["orders"] == 2
    assert disc["total_gross_revenue"] == 150.0
    assert disc["total_net_revenue"] == 135.0
    assert disc["total_discount"] == -15.0
    assert disc["avg_order_value_net"] == pytest.approx(67.5)
    assert disc["discount_depth_pct"] == pytest.approx(10.0)   # 15 / 150

    full = out["Full-price"]
    assert full["orders"] == 1
    assert full["discount_depth_pct"] == 0.0
