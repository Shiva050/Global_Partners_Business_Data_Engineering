"""Unit tests for location performance ranking."""
import pytest
from pyspark.sql import SparkSession

from jobs.gold.location_performance import compute_location_performance


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.appName("location-tests").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def test_ranking_and_metrics(spark):
    orders = spark.createDataFrame([
        ("r1", "u1", 100.0), ("r1", "u2", 50.0),   # 150, 2 orders, 2 customers
        ("r2", "u1", 200.0),                        # 200, 1 order
        ("r3", "u3", 50.0),                         # 50, 1 order
    ], "restaurant_id string, user_id string, order_revenue double")

    out = {r["restaurant_id"]: r for r in compute_location_performance(orders).collect()}

    assert out["r2"]["revenue_rank"] == 1          # highest revenue
    assert out["r1"]["revenue_rank"] == 2
    assert out["r3"]["revenue_rank"] == 3
    assert out["r1"]["total_revenue"] == 150.0
    assert out["r1"]["unique_customers"] == 2
    assert out["r1"]["avg_order_value"] == 75.0
