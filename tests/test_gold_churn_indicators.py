"""Unit tests for churn activity indicators."""
import datetime

import pytest
from pyspark.sql import SparkSession

from jobs.gold.churn_indicators import compute_churn_indicators

D = datetime.date


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.appName("churn-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _orders(spark, rows):
    return spark.createDataFrame(rows, "user_id string, order_revenue double, order_date date")


def _run(spark, rows):
    out = compute_churn_indicators(_orders(spark, rows), as_of="2023-12-31",
                                   at_risk_days=45, dormant_days=90, period_days=30)
    return {r["user_id"]: r for r in out.collect()}


def test_status_thresholds(spark):
    rows = [
        ("act", 10.0, D(2023, 12, 20)),   # 11 days -> Active
        ("risk", 10.0, D(2023, 11, 1)),   # 60 days -> At Risk
        ("dorm", 10.0, D(2023, 8, 1)),    # 152 days -> Dormant
    ]
    r = _run(spark, rows)
    assert r["act"]["churn_status"] == "Active" and r["act"]["at_risk"] is False
    assert r["risk"]["churn_status"] == "At Risk" and r["risk"]["at_risk"] is True
    assert r["dorm"]["churn_status"] == "Dormant" and r["dorm"]["at_risk"] is True
    assert r["act"]["days_since_last_order"] == 11


def test_avg_gap_days(spark):
    # 3 orders 10 days apart -> span 20 / (3-1) = 10.0
    rows = [
        ("g", 10.0, D(2023, 1, 1)),
        ("g", 10.0, D(2023, 1, 11)),
        ("g", 10.0, D(2023, 1, 21)),
    ]
    r = _run(spark, rows)
    assert r["g"]["avg_gap_days"] == 10.0
    assert r["g"]["total_orders"] == 3


def test_single_order_has_null_gap(spark):
    r = _run(spark, [("s", 10.0, D(2023, 12, 1))])
    assert r["s"]["avg_gap_days"] is None


def test_spend_change_pct(spark):
    # recent period (Dec 1-31): 150 ; prior period (Nov 1 - Dec 1): 100 -> +50%
    rows = [
        ("c", 150.0, D(2023, 12, 15)),
        ("c", 100.0, D(2023, 11, 15)),
    ]
    r = _run(spark, rows)
    assert r["c"]["recent_spend"] == 150.0
    assert r["c"]["prior_spend"] == 100.0
    assert r["c"]["spend_change_pct"] == 50.0
