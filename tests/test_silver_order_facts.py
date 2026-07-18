"""Unit tests for revenue-enriched order facts (gross/discount/net + rollup)."""
import datetime

import pytest
from pyspark.sql import SparkSession

from jobs.silver.order_facts import line_item_facts, order_facts

# Explicit schemas (avoid NULL-only type-inference failures in CI).
OI_SCHEMA = (
    "order_id string, lineitem_id string, user_id string, restaurant_id string, "
    "app_name string, currency string, is_loyalty boolean, creation_time_utc string, "
    "item_category string, item_name string, item_price double, item_quantity int"
)
OO_SCHEMA = (
    "order_id string, lineitem_id string, option_group_name string, option_name string, "
    "option_price double, option_quantity int"
)


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.appName("facts-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _oi(spark, rows):
    return spark.createDataFrame(rows, OI_SCHEMA)


def _oo(spark, rows):
    return spark.createDataFrame(rows, OO_SCHEMA)


def _li_row(spark, oi_rows, oo_rows):
    li = line_item_facts(_oi(spark, oi_rows), _oo(spark, oo_rows))
    return {(r["order_id"], r["lineitem_id"]): r for r in li.collect()}


def test_line_item_with_addon_and_discount(spark):
    # item 10 x2 = 20 ; add-on +1.5 x1 = +1.5 ; discount -2.0 x1 = -2.0
    oi = [("o1", "l1", "u1", "r1", "web", "USD", True, "2023-05-10 12:00:00",
           "Entree", "Burrito", 10.0, 2)]
    oo = [
        ("o1", "l1", "Extras", "Guac", 1.5, 1),
        ("o1", "l1", "Promo", "Coupon", -2.0, 1),
    ]
    r = _li_row(spark, oi, oo)[("o1", "l1")]
    assert r["item_amount"] == 20.0
    assert r["gross_revenue"] == 21.5           # 20 + 1.5 add-on
    assert r["discount_amount"] == -2.0
    assert r["net_revenue"] == 19.5             # 20 + (1.5 - 2.0)
    assert r["is_discounted"] is True


def test_multiple_options_do_not_fan_out(spark):
    # One line item with THREE options must stay ONE row (options summed), not
    # three — otherwise item revenue is multiplied and CLV is corrupted.
    oi = [("o1", "l1", "u1", "r1", "web", "USD", True, "2023-05-10 12:00:00",
           "Entree", "Bowl", 10.0, 1)]
    oo = [
        ("o1", "l1", "Extras", "Guac", 1.5, 1),
        ("o1", "l1", "Extras", "Cheese", 1.0, 1),
        ("o1", "l1", "Promo", "Coupon", -2.0, 1),
    ]
    li = line_item_facts(_oi(spark, oi), _oo(spark, oo))
    assert li.count() == 1                       # no fan-out
    r = li.collect()[0]
    assert r["item_amount"] == 10.0
    assert r["gross_revenue"] == 12.5            # 10 + 1.5 + 1.0
    assert r["net_revenue"] == 10.5              # 10 + (1.5 + 1.0 - 2.0)


def test_line_item_without_options(spark):
    oi = [("o1", "l1", "u1", "r1", "web", "USD", False, "2023-05-10 12:00:00",
           "Drink", "Soda", 3.0, 4)]
    r = _li_row(spark, oi, [])[("o1", "l1")]
    assert r["item_amount"] == 12.0
    assert r["gross_revenue"] == 12.0 and r["net_revenue"] == 12.0
    assert r["discount_amount"] == 0.0
    assert r["is_discounted"] is False


def test_outlier_flag(spark):
    oi = [
        ("oBig", "l1", "u1", "r1", "web", "USD", False, "2023-05-10 12:00:00", "Entree", "A", 5000.0, 500),
        ("oNorm", "l1", "u2", "r1", "web", "USD", False, "2023-05-10 12:00:00", "Drink", "B", 3.0, 1),
    ]
    li = line_item_facts(_oi(spark, oi), _oo(spark, []))
    of = {r["order_id"]: r for r in order_facts(li, outlier_threshold=10000.0).collect()}
    assert of["oBig"]["order_revenue"] == 2_500_000.0 and of["oBig"]["is_outlier"] is True
    assert of["oNorm"]["is_outlier"] is False


def test_order_rollup_and_date(spark):
    oi = [
        ("o1", "l1", "u1", "r1", "web", "USD", True, "2023-05-10 12:00:00", "Entree", "A", 10.0, 1),
        ("o1", "l2", "u1", "r1", "web", "USD", True, "2023-05-10 12:00:00", "Drink", "B", 3.0, 2),
    ]
    oo = [("o1", "l1", "Promo", "Coupon", -1.0, 1)]
    li = line_item_facts(_oi(spark, oi), _oo(spark, oo))
    of = {r["order_id"]: r for r in order_facts(li).collect()}["o1"]

    # net: l1 = 10-1 = 9 ; l2 = 6 ; order = 15
    assert of["order_revenue"] == 15.0
    assert of["order_gross_revenue"] == 16.0    # 10 + 6
    assert of["order_discount_amount"] == -1.0
    assert of["line_item_count"] == 2
    assert of["total_item_quantity"] == 3.0
    assert of["order_date"] == datetime.date(2023, 5, 10)
    assert of["is_discounted"] is True
    assert of["user_id"] == "u1"
