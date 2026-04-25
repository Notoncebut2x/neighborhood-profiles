"""
fetch_census.py
---------------
Downloads ACS 5-year and Decennial Census data via the Census Bureau REST API.
Saves raw CSVs to data/raw/ for reproducibility.

Usage (standalone):
    python fetch_census.py
"""

import requests
import pandas as pd
from pathlib import Path
from config import (
    CENSUS_API_KEY, STATE_FIPS, COUNTY_FIPS,
    ACS_CURRENT_YEAR, ACS_PRIOR_YEAR, RAW_DIR
)

# ---------------------------------------------------------------------------
# ACS variable definitions
# ---------------------------------------------------------------------------

# Each entry: (variable_name_in_api, friendly_name, table)
# We fetch the full bracket tables needed to interpolate medians properly.

ACS_VARIABLES = {
    # --- Total population ---
    "B01003_001E": "pop_total",

    # --- Age (B01001: Sex by Age) ---
    # We sum relevant detail lines into age bins after fetching
    "B01001_007E": "male_18_19", "B01001_008E": "male_20",
    "B01001_009E": "male_21", "B01001_010E": "male_22_24",
    "B01001_011E": "male_25_29", "B01001_012E": "male_30_34",
    "B01001_031E": "female_18_19", "B01001_032E": "female_20",
    "B01001_033E": "female_21", "B01001_034E": "female_22_24",
    "B01001_035E": "female_25_29", "B01001_036E": "female_30_34",

    "B01001_013E": "male_35_39", "B01001_014E": "male_40_44",
    "B01001_015E": "male_45_49", "B01001_016E": "male_50_54",
    "B01001_017E": "male_55_59", "B01001_018E": "male_60_61",
    "B01001_019E": "male_62_64",
    "B01001_037E": "female_35_39", "B01001_038E": "female_40_44",
    "B01001_039E": "female_45_49", "B01001_040E": "female_50_54",
    "B01001_041E": "female_55_59", "B01001_042E": "female_60_61",
    "B01001_043E": "female_62_64",

    "B01001_020E": "male_65_66", "B01001_021E": "male_67_69",
    "B01001_022E": "male_70_74", "B01001_023E": "male_75_79",
    "B01001_024E": "male_80_84", "B01001_025E": "male_85plus",
    "B01001_044E": "female_65_66", "B01001_045E": "female_67_69",
    "B01001_046E": "female_70_74", "B01001_047E": "female_75_79",
    "B01001_048E": "female_80_84", "B01001_049E": "female_85plus",

    # --- Education (B15003: 25+) ---
    "B15003_001E": "edu_total_25plus",
    "B15003_022E": "edu_bachelors",
    "B15003_023E": "edu_masters",
    "B15003_024E": "edu_professional",
    "B15003_025E": "edu_doctorate",

    # --- Household income brackets (B19001) for median interpolation ---
    "B19001_001E": "hhinc_total_hh",
    "B19001_002E": "hhinc_lt10k",
    "B19001_003E": "hhinc_10_15k",
    "B19001_004E": "hhinc_15_20k",
    "B19001_005E": "hhinc_20_25k",
    "B19001_006E": "hhinc_25_30k",
    "B19001_007E": "hhinc_30_35k",
    "B19001_008E": "hhinc_35_40k",
    "B19001_009E": "hhinc_40_45k",
    "B19001_010E": "hhinc_45_50k",
    "B19001_011E": "hhinc_50_60k",
    "B19001_012E": "hhinc_60_75k",
    "B19001_013E": "hhinc_75_100k",
    "B19001_014E": "hhinc_100_125k",
    "B19001_015E": "hhinc_125_150k",
    "B19001_016E": "hhinc_150_200k",
    "B19001_017E": "hhinc_200k_plus",

    # --- Gross rent brackets (B25063) for median interpolation ---
    "B25063_001E": "rent_total_units",
    "B25063_003E": "rent_lt100",
    "B25063_004E": "rent_100_149",
    "B25063_005E": "rent_150_199",
    "B25063_006E": "rent_200_249",
    "B25063_007E": "rent_250_299",
    "B25063_008E": "rent_300_349",
    "B25063_009E": "rent_350_399",
    "B25063_010E": "rent_400_449",
    "B25063_011E": "rent_450_499",
    "B25063_012E": "rent_500_549",
    "B25063_013E": "rent_550_599",
    "B25063_014E": "rent_600_649",
    "B25063_015E": "rent_650_699",
    "B25063_016E": "rent_700_749",
    "B25063_017E": "rent_750_799",
    "B25063_018E": "rent_800_899",
    "B25063_019E": "rent_900_999",
    "B25063_020E": "rent_1000_1249",
    "B25063_021E": "rent_1250_1499",
    "B25063_022E": "rent_1500_1999",
    "B25063_023E": "rent_2000_2499",
    "B25063_024E": "rent_2500_2999",
    "B25063_025E": "rent_3000_3499",
    "B25063_026E": "rent_3500_plus",

    # --- Home value brackets (B25075) for median interpolation ---
    "B25075_001E": "homeval_total_units",
    "B25075_002E": "homeval_lt10k",
    "B25075_003E": "homeval_10_15k",
    "B25075_004E": "homeval_15_20k",
    "B25075_005E": "homeval_20_25k",
    "B25075_006E": "homeval_25_30k",
    "B25075_007E": "homeval_30_35k",
    "B25075_008E": "homeval_35_40k",
    "B25075_009E": "homeval_40_50k",
    "B25075_010E": "homeval_50_60k",
    "B25075_011E": "homeval_60_70k",
    "B25075_012E": "homeval_70_80k",
    "B25075_013E": "homeval_80_90k",
    "B25075_014E": "homeval_90_100k",
    "B25075_015E": "homeval_100_125k",
    "B25075_016E": "homeval_125_150k",
    "B25075_017E": "homeval_150_175k",
    "B25075_018E": "homeval_175_200k",
    "B25075_019E": "homeval_200_250k",
    "B25075_020E": "homeval_250_300k",
    "B25075_021E": "homeval_300_400k",
    "B25075_022E": "homeval_400_500k",
    "B25075_023E": "homeval_500_750k",
    "B25075_024E": "homeval_750k_1m",
    "B25075_025E": "homeval_1m_1500k",
    "B25075_026E": "homeval_1500k_2m",
    "B25075_027E": "homeval_2m_plus",

    # --- Tenure (B25003) ---
    "B25003_001E": "tenure_total",
    "B25003_002E": "tenure_owner",
    "B25003_003E": "tenure_renter",

    # --- Vacancy (B25002) ---
    "B25002_001E": "housing_total_units",
    "B25002_002E": "housing_occupied",
    "B25002_003E": "housing_vacant",

    # --- Vacancy status breakdown (B25004) ---
    "B25004_002E": "vacant_for_rent",
    "B25004_003E": "vacant_rented_not_occ",
    "B25004_004E": "vacant_for_sale",
    "B25004_005E": "vacant_sold_not_occ",
    "B25004_006E": "vacant_seasonal",
    "B25004_007E": "vacant_migrant",
    "B25004_008E": "vacant_other",

    # --- Average household size (B25010) ---
    "B25010_001E": "avg_hh_size",

    # --- Year built brackets (B25036) ---
    "B25036_001E": "yrbuilt_total",
    "B25036_002E": "yrbuilt_owner_2014plus",
    "B25036_003E": "yrbuilt_owner_2010_13",
    "B25036_004E": "yrbuilt_owner_2000_09",
    "B25036_005E": "yrbuilt_owner_1990_99",
    "B25036_006E": "yrbuilt_owner_1980_89",
    "B25036_007E": "yrbuilt_owner_1970_79",
    "B25036_008E": "yrbuilt_owner_1960_69",
    "B25036_009E": "yrbuilt_owner_1950_59",
    "B25036_010E": "yrbuilt_owner_1940_49",
    "B25036_011E": "yrbuilt_owner_pre1940",
    "B25036_012E": "yrbuilt_renter_2014plus",
    "B25036_013E": "yrbuilt_renter_2010_13",
    "B25036_014E": "yrbuilt_renter_2000_09",
    "B25036_015E": "yrbuilt_renter_1990_99",
    "B25036_016E": "yrbuilt_renter_1980_89",
    "B25036_017E": "yrbuilt_renter_1970_79",
    "B25036_018E": "yrbuilt_renter_1960_69",
    "B25036_019E": "yrbuilt_renter_1950_59",
    "B25036_020E": "yrbuilt_renter_1940_49",
    "B25036_021E": "yrbuilt_renter_pre1940",

    # --- Commuting (B08301) ---
    "B08301_001E": "commute_total",
    "B08301_003E": "commute_drive_alone",
    "B08301_004E": "commute_carpool",
    "B08301_010E": "commute_transit",
    "B08301_019E": "commute_walk",
    "B08301_021E": "commute_wfh",

    # --- Geographic mobility in past year (B07003) ---
    "B07003_001E": "mobility_total",
    "B07003_004E": "mobility_same_house",
    "B07003_007E": "mobility_moved_same_county",
    "B07003_010E": "mobility_moved_diff_county",
    "B07003_013E": "mobility_moved_diff_state",
    "B07003_016E": "mobility_moved_abroad",

    # --- Poverty (B17001) ---
    "B17001_001E": "poverty_total_denom",
    "B17001_002E": "poverty_below",
}

# ---------------------------------------------------------------------------
# Bracket definitions for median interpolation
# Format: list of (lower_bound, upper_bound) — upper is exclusive or open-ended
# ---------------------------------------------------------------------------

INCOME_BRACKETS = [
    (0, 10000), (10000, 15000), (15000, 20000), (20000, 25000),
    (25000, 30000), (30000, 35000), (35000, 40000), (40000, 45000),
    (45000, 50000), (50000, 60000), (60000, 75000), (75000, 100000),
    (100000, 125000), (125000, 150000), (150000, 200000), (200000, 250000),
]
INCOME_BRACKET_COLS = [
    "hhinc_lt10k", "hhinc_10_15k", "hhinc_15_20k", "hhinc_20_25k",
    "hhinc_25_30k", "hhinc_30_35k", "hhinc_35_40k", "hhinc_40_45k",
    "hhinc_45_50k", "hhinc_50_60k", "hhinc_60_75k", "hhinc_75_100k",
    "hhinc_100_125k", "hhinc_125_150k", "hhinc_150_200k", "hhinc_200k_plus",
]

RENT_BRACKETS = [
    (0, 100), (100, 150), (150, 200), (200, 250), (250, 300),
    (300, 350), (350, 400), (400, 450), (450, 500), (500, 550),
    (550, 600), (600, 650), (650, 700), (700, 750), (750, 800),
    (800, 900), (900, 1000), (1000, 1250), (1250, 1500), (1500, 2000),
    (2000, 2500), (2500, 3000), (3000, 3500), (3500, 4500),
]
RENT_BRACKET_COLS = [
    "rent_lt100", "rent_100_149", "rent_150_199", "rent_200_249",
    "rent_250_299", "rent_300_349", "rent_350_399", "rent_400_449",
    "rent_450_499", "rent_500_549", "rent_550_599", "rent_600_649",
    "rent_650_699", "rent_700_749", "rent_750_799", "rent_800_899",
    "rent_900_999", "rent_1000_1249", "rent_1250_1499", "rent_1500_1999",
    "rent_2000_2499", "rent_2500_2999", "rent_3000_3499", "rent_3500_plus",
]

HOMEVAL_BRACKETS = [
    (0, 10000), (10000, 15000), (15000, 20000), (20000, 25000),
    (25000, 30000), (30000, 35000), (35000, 40000), (40000, 50000),
    (50000, 60000), (60000, 70000), (70000, 80000), (80000, 90000),
    (90000, 100000), (100000, 125000), (125000, 150000), (150000, 175000),
    (175000, 200000), (200000, 250000), (250000, 300000), (300000, 400000),
    (400000, 500000), (500000, 750000), (750000, 1000000),
    (1000000, 1500000), (1500000, 2000000), (2000000, 2500000),
]
HOMEVAL_BRACKET_COLS = [
    "homeval_lt10k", "homeval_10_15k", "homeval_15_20k", "homeval_20_25k",
    "homeval_25_30k", "homeval_30_35k", "homeval_35_40k", "homeval_40_50k",
    "homeval_50_60k", "homeval_60_70k", "homeval_70_80k", "homeval_80_90k",
    "homeval_90_100k", "homeval_100_125k", "homeval_125_150k", "homeval_150_175k",
    "homeval_175_200k", "homeval_200_250k", "homeval_250_300k", "homeval_300_400k",
    "homeval_400_500k", "homeval_500_750k", "homeval_750k_1m",
    "homeval_1m_1500k", "homeval_1500k_2m", "homeval_2m_plus",
]


# ---------------------------------------------------------------------------
# Census API helpers
# ---------------------------------------------------------------------------

def _build_acs_url(year: int, variables: list[str], state: str, county: str) -> str:
    base = f"https://api.census.gov/data/{year}/acs/acs5"
    var_str = "NAME," + ",".join(variables)
    geo = f"block+group:*&in=state:{state}+county:{county}+tract:*"
    return f"{base}?get={var_str}&for={geo}&key={CENSUS_API_KEY}"


def fetch_acs(year: int, variables: dict[str, str], state: str, county: str) -> pd.DataFrame:
    """
    Fetch ACS 5-year data for all block groups in a county.

    Parameters
    ----------
    year : ACS end year (e.g. 2021 for 2017–2021)
    variables : dict mapping Census variable codes to friendly column names
    state, county : FIPS codes

    Returns
    -------
    DataFrame with friendly column names + GEOID column
    """
    api_vars = list(variables.keys())

    # Census API allows max ~50 variables per call — chunk if needed
    chunk_size = 48
    chunks = [api_vars[i:i+chunk_size] for i in range(0, len(api_vars), chunk_size)]

    frames = []
    for chunk in chunks:
        url = _build_acs_url(year, chunk, state, county)
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data[1:], columns=data[0])
        frames.append(df)

    # Merge all chunks on geo identifiers
    geo_cols = ["NAME", "state", "county", "tract", "block group"]
    merged = frames[0]
    for df in frames[1:]:
        merged = merged.merge(df, on=geo_cols)

    # Build 12-character GEOID: state + county + tract + block_group
    merged["GEOID"] = (
        merged["state"] + merged["county"] + merged["tract"] + merged["block group"]
    )

    # Rename and cast numeric columns
    merged = merged.rename(columns=variables)
    numeric_cols = list(variables.values())
    merged[numeric_cols] = merged[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # Replace Census sentinel values (-666666666, -999999999, etc.) with NaN
    sentinel_mask = merged[numeric_cols] < -1
    merged[numeric_cols] = merged[numeric_cols].where(~sentinel_mask, other=float("nan"))

    keep_cols = ["GEOID"] + numeric_cols
    return merged[keep_cols]


def save_acs(year: int, df: pd.DataFrame, label: str = "current") -> Path:
    path = RAW_DIR / f"acs_{year}_{label}.csv"
    df.to_csv(path, index=False)
    print(f"  Saved {len(df):,} block groups → {path}")
    return path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Fetching ACS {ACS_CURRENT_YEAR} (current period)...")
    df_current = fetch_acs(
        ACS_CURRENT_YEAR, ACS_VARIABLES, STATE_FIPS, COUNTY_FIPS
    )
    save_acs(ACS_CURRENT_YEAR, df_current, "current")

    print(f"\nFetching ACS {ACS_PRIOR_YEAR} (prior period for trends)...")
    # For trends fetch all variables needed for change calculations
    TREND_VARIABLES = {
        k: v for k, v in ACS_VARIABLES.items()
        if v.startswith((
            "pop_total", "hhinc_", "rent_", "tenure_",
            "homeval_",                   # for home value change
            "housing_", "vacant_",        # for vacancy rate change
            "edu_",                       # for education change
        ))
    }
    df_prior = fetch_acs(
        ACS_PRIOR_YEAR, TREND_VARIABLES, STATE_FIPS, COUNTY_FIPS
    )
    save_acs(ACS_PRIOR_YEAR, df_prior, "prior")

    print("\nDone. Raw ACS data saved to data/raw/")
