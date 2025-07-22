import pandas as pd
import pytest
from etl.transform_funcs import (
    parse_dates,
    clean_basic,
    cap_extremes,
    derive_fields,
    rename_columns,
)


# 1) Test date parsing
def test_parse_dates():
    df = pd.DataFrame(
        {
            "Order Date": ["7/27/2012", "invalid"],
            "Ship Date": ["07/28/2012", "7/29/2012"],
        }
    )
    out = parse_dates(df)
    assert out["Order Date"].dtype == "datetime64[ns]"
    assert pd.notnull(out.loc[0, "Order Date"])
    assert pd.isna(out.loc[1, "Order Date"])


# 2) Test basic cleaning
def test_clean_basic():
    df = pd.DataFrame(
        {
            "Quantity": [1, 0, -1],
            "Sales": [100, 200, 300],
            "Order Date": pd.to_datetime(["2025-01-01", None, "2025-01-03"]),
            "Ship Date": pd.to_datetime(["2025-01-02", "2025-01-02", None]),
        }
    )
    out = clean_basic(df)
    assert len(out) == 1  # only the valid row


# 3) Test capping extremes
def test_cap_extremes():
    df = pd.DataFrame(
        {
            "Quantity": [1, 1],
            "Sales": [500, 20000],
            "Profit": [50, 200],
        }
    )
    out = cap_extremes(df)
    # Only keep rows with Sales <= threshold
    assert len(out) == 1
    assert out["Sales"].iloc[0] == 500


# 4) Test derived fields
def test_derive_fields():
    df = pd.DataFrame({"Quantity": [2], "Sales": [50], "Profit": [10]})
    out = derive_fields(df)
    assert out.loc[0, "unit_price"] == 25
    assert out.loc[0, "profit_margin"] == pytest.approx(0.2)


# 5) Test renaming
def test_rename_columns():
    df = pd.DataFrame(
        {
            "Region": ["X"],
            "Country": ["Y"],
            "Order ID": ["1"],
            "Order Date": [pd.Timestamp("2025-01-01")],
            "Ship Date": [pd.Timestamp("2025-01-02")],
            "Customer ID": ["C"],
            "Product ID": ["P"],
            "Category": ["Cat"],
            "Quantity": [1],
            "Sales": [100],
            "Profit": [10],
        }
    )
    out = rename_columns(df)
    expected = [
        "region",
        "country",
        "order_id",
        "order_date",
        "ship_date",
        "customer_id",
        "product_id",
        "category",
        "quantity",
        "total_sales",
        "profit",
    ]
    assert list(out.columns) == expected
