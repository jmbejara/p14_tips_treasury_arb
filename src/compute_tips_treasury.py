import os
import pandas as pd
import numpy as np
from decouple import config

DATA_DIR = config('DATA_DIR')
OUTPUT_DIR = config("OUTPUT_DIR")


# ------------------------------------------------------------------------------
# Import inflation swap data
# ------------------------------------------------------------------------------
def import_inflation_swap_data():
	# Point to the CSV file instead of an Excel file
	swaps_path = os.path.join(DATA_DIR, "treasury_inflation_swaps.csv")

	# Read CSV; explicitly parse the "Dates" column as datetime
	swaps = pd.read_csv(swaps_path, parse_dates=["Dates"])

	# Create a column mapping based on the schema you provided
	column_map = {
		"Dates": "date",
		"USSWITA BGN Curncy": "inf_swap_1m",
		"USSWITC BGN Curncy": "inf_swap_3m",
		"USSWITF BGN Curncy": "inf_swap_6m",
		"USSWIT1 BGN Curncy": "inf_swap_1y",
		"USSWIT2 BGN Curncy": "inf_swap_2y",
		"USSWIT3 BGN Curncy": "inf_swap_3y",
		"USSWIT4 BGN Curncy": "inf_swap_4y",
		"USSWIT5 BGN Curncy": "inf_swap_5y",
		"USSWIT10 BGN Curncy": "inf_swap_10y",
		"USSWIT20 BGN Curncy": "inf_swap_20y",
		"USSWIT30 BGN Curncy": "inf_swap_30y"
	}

	# Rename columns using the mapping
	swaps = swaps.rename(columns=column_map)

	# Convert relevant columns to numeric and divide by 100
	inf_cols = [
		"inf_swap_1y", "inf_swap_2y", "inf_swap_3y",
		"inf_swap_4y", "inf_swap_5y", "inf_swap_10y",
		"inf_swap_20y", "inf_swap_30y"
	]
	for col in inf_cols:
		swaps[col] = pd.to_numeric(swaps[col], errors="coerce") / 100.0

	# Select only the date and inflation swap columns, in a clean order
	swaps = swaps[["date"] + inf_cols]

	return swaps


# ------------------------------------------------------------------------------
# Read in zero-coupon TIPS and Treasury yields
# ------------------------------------------------------------------------------
def import_treasury_yields():
    # Define the path to the parquet file
    nom_path = os.path.join(DATA_DIR, "fed_yield_curve.parquet")

    # Read the parquet file; date is assumed to be in the index
    nom = pd.read_parquet(nom_path)

    if not pd.api.types.is_datetime64_any_dtype(nom.index):
        nom.index = pd.to_datetime(nom.index, format="%m/%d/%Y")

    # If the index has no name or is named "Date", set it to "date"
    if nom.index.name is None or nom.index.name == "Date":
        nom.index.name = "date"

    # For each tenor (2, 5, 10, 20), compute the nominal zero-coupon yield (in basis points)
    for t in [2, 5, 10, 20]:
        col = f"SVENY{'0' + str(t) if t < 10 else str(t)}"
        nom[f"nom_zc{t}"] = 1e4 * (np.exp(nom[col] / 100) - 1)

    # Convert the date index to a column and rename it to "date" if necessary
    nom = nom.reset_index()
    nom = nom.rename(columns={'Date': 'date'})

    # Subset the DataFrame to include the date column plus the computed 'nom' columns
    nom = nom[["date"] + [col for col in nom.columns if col.startswith("nom")]]

    return nom


def import_tips_yields():
	real_path = os.path.join(DATA_DIR, "fed_tips_yield_curve.parquet")
	real = pd.read_parquet(real_path)

	if not pd.api.types.is_datetime64_any_dtype(real['Date']):
		real.rename(columns={'Date': 'date'}, inplace=True)
		real['date'] = pd.to_datetime(real['date'], format="%Y-%m-%d")

	for t in [2, 5, 10, 20]:
		col = f"TIPSY{'0' + str(t) if t < 10 else str(t)}"
		real[f"real_cc{t}"] = real[col] / 100

	real = real[["date"] + [col for col in real.columns if col.startswith("real")]]

	return real


# ------------------------------------------------------------------------------
# Merge all data, compute implied riskless rate from TIPS
# ------------------------------------------------------------------------------
def compute_tips_treasury():
	"""
	Create Constant-Maturity TIPS-Treasury Arbitrage Series

	This script calculates implied risk-free rates and arbitrage opportunities
	between TIPS (Treasury Inflation-Protected Securities) and regular Treasury bonds.
	It combines three data sources:
	1. Inflation swap data from Excel
	2. Zero-coupon Treasury yields (nominal rates)
	3. TIPS yields (real rates)

	The script merges these datasets and computes:
	- Implied risk-free rates derived from TIPS yields plus inflation expectations
	- Arbitrage opportunities between TIPS-derived rates and nominal Treasury rates
	- Missing value indicators to ensure data quality

	Output is saved as a parquet file with all relevant series for analysis.
	"""
	real = import_tips_yields()
	nom = import_treasury_yields()
	swaps = import_inflation_swap_data()

	merged = pd.merge(real, nom, on="date", how="inner")
	merged = pd.merge(merged, swaps, on="date", how="inner")

	# Compute implied riskless rate from TIPS
	missing_indicators = []
	for t in [2, 5, 10, 20]:
		merged[f"tips_treas_{t}_rf"] = 1e4 * (np.exp(merged[f"real_cc{t}"] +
													 np.log(1 + merged[f"inf_swap_{t}y"])) - 1)

		merged[f"mi_{t}"] = merged[f"tips_treas_{t}_rf"].isna().astype(int)
		missing_indicators.append(f"mi_{t}")

		merged[f"arb{t}"] = merged[f"tips_treas_{t}_rf"] - merged[f"nom_zc{t}"]

	merged["miss_count"] = merged[missing_indicators].sum(axis=1)
	merged = merged[merged["miss_count"] < 4]

	merged = merged.drop(missing_indicators + ["miss_count"], axis=1)

	cols_to_keep = (["date"] +
					[col for col in merged.columns if col.startswith("real_")] +
					[col for col in merged.columns if col.startswith("nom_")] +
					[col for col in merged.columns if col.startswith("tips_")])

	merged = merged[cols_to_keep]

	output_path = os.path.join(DATA_DIR, "tips_treasury_implied_rf.parquet")
	merged.to_parquet(output_path, compression="snappy")

	print(f"Data saved to {output_path}")


if __name__ == "__main__":
	compute_tips_treasury()