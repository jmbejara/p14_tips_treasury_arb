import os
import pandas as pd
import pytest

# Import the data-loading functions from your repository.
from pull_fed_yield_curve import load_fed_yield_curve_all
from pull_fed_tips_yield_curve import load_tips_yield_curve
import load_bases_data

# Get the DATA_DIR from the environment. Ensure this env variable is set.
DATA_DIR = os.environ.get("DATA_DIR")
if DATA_DIR is None:
    raise EnvironmentError("DATA_DIR environment variable is not set.")

def load_predicted_data():
    """
    Load the three data sources and combine them column-wise.
    It is assumed that the dataframes share a common index (e.g. date or CUSIP).
    """
    # Load treasury inflation swaps from CSV
    swaps_path = os.path.join(DATA_DIR, "treasury_inflation_swaps.csv")
    df_swaps = pd.read_csv(swaps_path)
    
    # Load fed yield curve data
    df_yield = load_fed_yield_curve_all()
    
    # Load fed TIPS yield data
    df_tips = load_tips_yield_curve()
    
    # Combine dataframes side by side.
    # (Adjust the join if the dataframes do not share the same index.)
    df_predicted = pd.concat([df_swaps, df_yield, df_tips], axis=1)
    return df_predicted

def test_data_closeness():
    """
    Test that for every column the relative differences between the predicted
    data (from our three sources) and the base reliable data are within tolerances.
    
    For columns not starting with "TIPS_", we require:
         |Predicted - Actual| < 5%
    For columns starting with "TIPS_", we require:
         |Predicted - GSW| < 2%
    """
    # Load predicted data from our sources.
    predicted = load_predicted_data()
    
    # Load the reliable base data.
    base_data = load_bases_data.load_combined_spreads_wide(data_dir=DATA_DIR)
    
    # Check that every column in the base data is available in the predicted data.
    missing_cols = [col for col in base_data.columns if col not in predicted.columns]
    if missing_cols:
        pytest.skip(f"The following columns are missing in predicted data: {missing_cols}")

    # Loop over each column in the base data and compute the percentage difference.
    # The assumption here is that the "difference" is computed as a relative error:
    #     (predicted - base) / base
    for col in base_data.columns:
        # Compute the relative difference.
        diff = (predicted[col] - base_data[col]) / base_data[col]
        
        # Determine tolerance based on the column group.
        if col.startswith("TIPS_"):
            tolerance = 0.02
            tolerance_label = "Predicted - GSW %"
        else:
            tolerance = 0.05
            tolerance_label = "Predicted - Actual %"
        
        # Assert that the absolute difference for every entry is below the tolerance.
        assert (diff.abs() < tolerance).all(), (
            f"Column {col}: {tolerance_label} differences exceed tolerance of {tolerance*100:.0f}%."
        )

if __name__ == "__main__":
    # This allows the test to be run standalone.
    pytest.main([__file__])
