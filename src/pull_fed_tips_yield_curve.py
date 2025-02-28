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
    
    The CSV (feds200805.csv) is expected to:
      - Have introductory metadata in the first 19 rows.
      - Contain a date column in YMD format.
      - Include yield columns named "tipsy02", "tipsy05", "tipsy10", and "tipsy20".
      - Provide yield values in percentage terms (to be converted to decimals).
    """
    response = requests.get(TIPS_URL)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch TIPS data: HTTP {response.status_code}")
    
    # Skip the first 19 rows to read data directly
    df = pd.read_csv(BytesIO(response.content), skiprows=19)
    
    # Convert the date column; adjust format if necessary
    # Assuming the date column is named 'date' and in YMD (e.g., "20230101")
    df['date'] = pd.to_datetime(df['date'], format="%Y%m%d", errors='coerce')
    
    # Define the columns corresponding to the desired maturities
    maturity_cols = ['tipsy' + str(t).zfill(2) for t in [2, 5, 10, 20]]
    
    # Select the necessary columns
    df_yields = df[['date'] + maturity_cols].copy()
    
    # Convert yields from percentage to decimals
    for col in maturity_cols:
        df_yields[col] = pd.to_numeric(df_yields[col], errors='coerce') / 100.0
    
    # Set the date as the index for easier time-series handling
    df_yields.set_index('date', inplace=True)
    
    return df_yields

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
    data_directory = "path/to/your/data/directory"
    tips_df = pull_fed_tips_yield_curve()
    save_tips_yield_curve(tips_df, data_directory)
    # To load the data later
    loaded_tips_df = load_tips_yield_curve(data_directory)