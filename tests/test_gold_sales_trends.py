"""Unit tests for the daily sales-trends fact."""
import pytest
from pyspark.sql import SparkSession

from jobs.gold.sales_trends import compute_sales_daily

LI_SCHEMA = ("order_id string, restaurant_id string, item_category string, "
             "creation_time_utc string, item_quantity int, net_revenue double, "
             "gross_revenue double, discount_amount double")
DD_SCHEMA = ("date_key date, year int, month int, week int, day_of_week string, "
             "is_weekend boolean, is_holiday boolean, holiday_name string")


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.appName("sales-tests").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def test_daily_aggregate_and_holiday_join(spark):
    import datetime
    li = spark.createDataFrame([
        ("o1", "r1", "Entree", "2023-12-25 12:00:00", 2, 20.0, 22.0, -2.0),
        ("o1", "r1", "Entree", "2023-12-25 12:05:00", 1, 10.0, 10.0, 0.0),
        ("o2", "r1", "Drink", "2023-12-26 09:00:00", 1, 5.0, 5.0, 0.0),
    ], LI_SCHEMA)
    dd = spark.createDataFrame([
        (datetime.date(2023, 12, 25), 2023, 12, 52, "Monday", False, True, "Christmas"),
        (datetime.date(2023, 12, 26), 2023, 12, 52, "Tuesday", False, False, None),
    ], DD_SCHEMA)

    out = {(r["order_date"], r["restaurant_id"], r["item_category"]): r
           for r in compute_sales_daily(li, dd).collect()}

    entree = out[(datetime.date(2023, 12, 25), "r1", "Entree")]
    assert entree["revenue"] == 30.0            # 20 + 10
    assert entree["gross_revenue"] == 32.0
    assert entree["quantity"] == 3.0
    assert entree["order_count"] == 1           # distinct orders (both rows are o1)
    assert entree["line_item_count"] == 2
    assert entree["is_holiday"] is True and entree["holiday_name"] == "Christmas"

    drink = out[(datetime.date(2023, 12, 26), "r1", "Drink")]
    assert drink["is_holiday"] is False
