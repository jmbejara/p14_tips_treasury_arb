import pandas as pd
import requests
from io import BytesIO
from pathlib import Path

from settings import config
DATA_DIR = config('DATA_DIR')

# Define the URL for the TIPS yield data
TIPS_URL = "https://www.federalreserve.gov/data/yield-curve-tables/feds200805.csv"

def pull_fed_tips_yield_curve():
    """
    Download and process the latest zero-coupon TIPS yield curve from the Federal Reserve.

    Expected CSV structure:
    - Metadata in the first 19 rows (to be skipped).
    - 'Date' column in YMD format.
    - TIPS yield columns named 'TIPSY02', 'TIPSY05', 'TIPSY10', 'TIPSY20'.
    - Yield values are in percentage terms and must be converted to decimals.

    Returns:
        pd.DataFrame: Processed TIPS yield data.
    """
    # Fetch the data from the Federal Reserve
    response = requests.get(TIPS_URL)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch TIPS data: HTTP {response.status_code}")
    
    # Read CSV while skipping the first 19 rows (metadata)
    df = pd.read_csv(BytesIO(response.content), skiprows=19)
    
    # Convert 'Date' column to datetime format
    #df.rename(columns={'Date': 'date'}, inplace=True)
    #df['date'] = pd.to_datetime(df['date'], format="%Y%m%d", errors='coerce')

    # List of relevant columns (for 2y, 5y, 10y, 20y TIPS yields)
    #maturity_cols = ['TIPSY02', 'TIPSY05', 'TIPSY10', 'TIPSY20']
    
    # Select necessary columns
    #df_yields = df[['date'] + maturity_cols].copy()

    # Convert yields from percentage to decimal
    #for col in maturity_cols:
    #    df_yields[col] = pd.to_numeric(df_yields[col], errors='coerce') / 100.0
    
    # Drop missing date rows
    #df_yields.dropna(subset=['date'], inplace=True)

    # Sort by date for time series consistency
    #df_yields.sort_values(by='date', inplace=True)
    
    return df

def save_tips_yield_curve(df, data_dir):
    """
    Save the TIPS yield curve DataFrame to a parquet file.
    """
    path = Path(data_dir) / "fed_tips_yield_curve.parquet"
    df.to_parquet(path)

def load_tips_yield_curve(data_dir):
    """
    Load the TIPS yield curve DataFrame from a parquet file.
    """
    path = Path(data_dir) / "fed_tips_yield_curve.parquet"
    return pd.read_parquet(path)

# Example usage
if __name__ == "__main__":
    tips_df = pull_fed_tips_yield_curve()
    save_tips_yield_curve(tips_df, DATA_DIR)
    # To load the data later
    #loaded_tips_df = load_tips_yield_curve(DATA_DIR)