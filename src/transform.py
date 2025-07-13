import pandas as pd
from config import SALES_THRESHOLD


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse Order Date and Ship Date from strings to datetime,
    coercing invalids to NaT.
    """
    df = df.copy()
    df['Order Date'] = pd.to_datetime(
        df['Order Date'],
        dayfirst=True,
        errors='coerce'
    )
    df['Ship Date'] = pd.to_datetime(
        df['Ship Date'],
        dayfirst=True,
        errors='coerce'
    )
    return df


def clean_basic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only rows with positive Quantity & Sales and non-null dates.
    """
    return df.query(
        'Quantity > 0 and Sales > 0 '
        'and `Order Date`.notnull() '
        'and `Ship Date`.notnull()'
    ).copy()


def cap_extremes(df: pd.DataFrame, threshold: float = SALES_THRESHOLD) -> pd.DataFrame:
    """
    Null out Sales above the given threshold.
    """
    df = df.copy()
    df.loc[df['Sales'] > threshold, 'Sales'] = pd.NA
    return df


def derive_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add unit_price and profit_margin columns.
    """
    df = df.copy()
    df['unit_price']    = df['Sales'] / df['Quantity']
    df['profit_margin'] = df['Profit'] / df['Sales']
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to match SQL schema (snake_case).
    """
    return df.rename(columns={
        'Region':      'region',
        'Country':     'country',
        'Order ID':    'order_id',
        'Order Date':  'order_date',
        'Ship Date':   'ship_date',
        'Customer ID': 'customer_id',
        'Product ID':  'product_id',
        'Category':    'category',
        'Quantity':    'quantity',
        'Sales':       'total_sales',
        'Profit':      'profit'
    })

if __name__ == "__main__":
    # quick sanity check
    df = pd.DataFrame({
        'Quantity': [1, 0],
        'Sales':    [100, 200],
        'Profit':   [10, 20],
        'Order Date': ['7/27/2012', 'invalid'],
        'Ship Date':  ['7/28/2012', '7/29/2012']
    })
    parsed = parse_dates(df)
    cleaned = clean_basic(parsed)
    capped = cap_extremes(cleaned)
    derived = derive_fields(capped)
    renamed = rename_columns(derived)
    print("Transform pipeline result:\n", renamed)