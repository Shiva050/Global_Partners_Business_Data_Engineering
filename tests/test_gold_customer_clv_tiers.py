"""Unit tests for CLV value tiers (top 20% / mid 60% / bottom 20%)."""
import pytest
from pyspark.sql import SparkSession

from jobs.gold.customer_clv_tiers import compute_clv_tiers


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.appName("clv-tiers-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _orders(spark, rows):
    return spark.createDataFrame(rows, "user_id string, order_revenue double")


def test_tiers_20_60_20(spark):
    # 5 customers, distinct totals -> one per quintile: High / Medium×3 / Low.
    rows = [
        ("c1", 500.0), ("c2", 400.0), ("c3", 300.0), ("c4", 200.0), ("c5", 100.0),
    ]
    out = {r["user_id"]: r for r in compute_clv_tiers(_orders(spark, rows)).collect()}
    assert out["c1"]["clv_tier"] == "High"     # top 20%
    assert out["c5"]["clv_tier"] == "Low"      # bottom 20%
    for u in ("c2", "c3", "c4"):
        assert out[u]["clv_tier"] == "Medium"  # middle 60%


def test_total_clv_and_orders_aggregate(spark):
    rows = [
        ("c1", 100.0), ("c1", 50.0),   # 2 orders, 150
        ("c2", 25.0),                  # 1 order, 25
    ]
    out = {r["user_id"]: r for r in compute_clv_tiers(_orders(spark, rows)).collect()}
    assert out["c1"]["total_clv"] == 150.0
    assert out["c1"]["total_orders"] == 2
    assert out["c2"]["total_clv"] == 25.0
