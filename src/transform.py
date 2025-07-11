import pandas as pd
#from config import sales_threshold

def clean_basic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Core filters: positive quantity & sales, valid dates.
    """
    return df.query(
        'Quantity > 0 and Sales > 0 '
        'and `Order Date`.notnull() '
        'and `Ship Date`.notnull()'
    ).copy()

def cap_extremes(df, threshold=10000):
    """ Null out any Sales > threshold. """
    df = df.copy()
    df.loc[df['Sales']>threshold, 'Sales'] = pd.NA
    return df

def derive_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived fields: unit_price and profit_margin.
    """
    df = df.copy()
    df['unit_price']    = df['Sales'] / df['Quantity']
    df['profit_margin'] = df['Profit'] / df['Sales']
    return df
