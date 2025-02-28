import pandas as pd
import polars as pl

from matplotlib import pyplot as plt
import seaborn as sns
sns.set()

from settings import config

DATA_DIR = config("DATA_DIR")


name_map = {
    "raw_box_12m": "Box_06m",
    "raw_box_18m": "Box_12m",
    "raw_box_6m": "Box_18m",
    "raw_cal_dow": "Eq_SF_Dow",
    "raw_cal_ndaq": "Eq_SF_NDAQ",
    "raw_cal_spx": "Eq_SF_SPX",
    "raw_cds_bond_hy": "CDS_Bond_HY",
    "raw_cds_bond_ig": "CDS_Bond_IG",
    "raw_cip_aud": "CIP_AUD",
    "raw_cip_cad": "CIP_CAD",
    "raw_cip_chf": "CIP_CHF",
    "raw_cip_eur": "CIP_EUR",
    "raw_cip_gbp": "CIP_GBP",
    "raw_cip_jpy": "CIP_JPY",
    "raw_cip_nzd": "CIP_NZD",
    "raw_cip_sek": "CIP_SEK",
    "raw_tfut_10": "Treasury_SF_10Y",
    "raw_tfut_2": "Treasury_SF_02Y",
    "raw_tfut_20": "Treasury_SF_20Y",
    "raw_tfut_30": "Treasury_SF_30Y",
    "raw_tfut_5": "Treasury_SF_05Y",
    "raw_tips_treas_10": "TIPS_Treasury_10Y",
    "raw_tips_treas_2": "TIPS_Treasury_02Y",
    "raw_tips_treas_20": "TIPS_Treasury_20Y",
    "raw_tips_treas_5": "TIPS_Treasury_05Y",
    "raw_tswap_1": "Treasury_Swap_01Y",
    "raw_tswap_10": "Treasury_Swap_10Y",
    "raw_tswap_2": "Treasury_Swap_02Y",
    "raw_tswap_20": "Treasury_Swap_20Y",
    "raw_tswap_3": "Treasury_Swap_03Y",
    "raw_tswap_30": "Treasury_Swap_30Y",
    "raw_tswap_5": "Treasury_Swap_05Y",
}

def load_combined_spreads_wide(data_dir=DATA_DIR, raw=False, rename=True):
    """
    Wide include extra variables.
    In the raw data, variables labeled "raw" are the raw spreads.
    Without raw means the absolute value has been applied.
    """
    data_dir = Path(data_dir)
    # filepath = (
    #     data_dir / "arbitrage_spread_wide.dta"
    # )
    filepath = "https://www.dropbox.com/scl/fi/81jm3dbe856i7p17rjy87/arbitrage_spread_wide.dta?rlkey=ke78u464vucmn43zt27nzkxya&st=59g2n7dt&dl=1"
    df = pd.read_stata(filepath).set_index("date")
    if raw:
        ret = df.copy()
    else:
        ret = df[list(name_map.keys())]
        if rename:
            ret = ret.rename(columns=name_map)
        ret = ret.reindex(sorted(ret.columns), axis=1)
    return ret


if __name__ == "__main__":
    df = load_combined_spreads_wide(data_dir=DATA_DIR)
    print(df.head())
    