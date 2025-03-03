import os
from datetime import datetime
from xbbg import blp

def main():
    """
    Pull daily historical 'PX_LAST' data for the given TIS tickers from 1/1/2000
    to today's date. Save the results in a single CSV file

    load in columns as : 
    "Treasury_Swap_01Y",
    "Treasury_Swap_10Y",
     "Treasury_Swap_02Y",
    "Treasury_Swap_20Y",
    "Treasury_Swap_03Y",
    "Treasury_Swap_30Y",
    "Treasury_Swap_05Y",
    """
    tickers = [
        "USSWITA BGN Curncy",   # 1M
        "USSWITC BGN Curncy",   # 3M
        "USSWITF BGN Curncy",   # 6M
        "USSWIT1 BGN Curncy",   # 1Y
        "USSWIT2 BGN Curncy",   # 2Y
        "USSWIT3 BGN Curncy",   # 3Y
        "USSWIT4 BGN Curncy",   # 4Y
        "USSWIT5 BGN Curncy",   # 5Y
        "USSWIT10 BGN Curncy",  # 10Y
        "USSWIT20 BGN Curncy",  # 20Y
        "USSWIT30 BGN Curncy",  # 30Y
    ]

    fields = ["PX_LAST"]

    start_date = "2000-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    df = blp.bdh(
        tickers=tickers,
        flds=fields,
        start_date=start_date,
        end_date=end_date
    )
    # df has a Date index and a MultiIndex for columns: (ticker, field).
    # Because fields=["PX_LAST"], we can drop that extra field level for simplicity:
    df.columns = df.columns.droplevel(level=1)  # Remove the "PX_LAST" level

    raw_dir = r"{path_here}\raw"
    os.makedirs(raw_dir, exist_ok=True)

    out_path = os.path.join(raw_dir, "treasury_inflation_swaps.csv")
    df.to_csv(out_path, index=True)

    print(f"Treasury Inflation Swaps data saved to: {out_path}")


if __name__ == "__main__":
    main()
