"""
profiles.py
-----------
Output functions: clean summary DataFrames, per-NSA profile tables,
CSV export, and GeoJSON export with all metrics attached.

Public API
----------
  build_summary_table(metrics)          → master DataFrame, all NSAs
  profile_for_nsa(metrics, nsa_id)      → dict / formatted table for one NSA
  export_csvs(metrics, nsas)            → writes per-NSA CSVs + master CSV
  export_geospatial(metrics, nsas)      → writes GeoJSON + Shapefile
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
from config import NSA_ID_COL, NSA_NAME_COL, OUTPUT_DIR, CRS_GEOGRAPHIC


# ---------------------------------------------------------------------------
# Column groups for the final summary table
# ---------------------------------------------------------------------------

TOP_PANEL = [
    NSA_ID_COL, NSA_NAME_COL,
    "pop_total",
    "med_hh_income",
    "pct_hh_below50k",
    "pct_hh_50_100k",
    "pct_hh_100k_plus",
    "med_gross_rent",
    "pct_renters",
    "pop_change_pct",
]

CUSTOMER_BASE = [
    "pop_density_per_km2",
    "pct_18_34",
    "pct_35_64",
    "pct_65plus",
    "pct_bachelors_plus",
    "avg_hh_size",
    "pct_commute_drive",
    "pct_commute_transit",
    "pct_commute_walk",
    "pct_wfh",
]

HOUSING_MARKET = [
    "med_home_value",
    "pct_vacant",
    "pct_vacant_for_rent",
    "pct_vacant_for_sale",
    "pct_vacant_distressed",
    "pct_owners",
    "pct_renters",
    "pct_pre1950",
    "pct_moved_past_year",
    "pct_poverty",
]

TRENDS = [
    "pop_total_prior",
    "pop_change_abs",
    "pop_change_pct",
    "pop_change_pct_vs_city",
    "med_hh_income_prior_adj",
    "income_change_pct",
    "income_change_pct_vs_city",
    "med_gross_rent_prior",
    "rent_change_pct",
    "rent_change_pct_vs_city",
    "med_home_value_prior",
    "home_value_change_pct",
    "home_value_change_pct_vs_city",
    "pct_vacant_prior",
    "vacancy_change_pp",
    "vacancy_change_pp_vs_city",
    "pct_bachelors_plus_prior",
    "edu_change_pp",
    "edu_change_pp_vs_city",
]

MARKET_SIGNALS = ["market_signals"]

# All ordered columns (skipping those not present due to missing data)
ALL_COLS = (
    TOP_PANEL +
    [c for c in CUSTOMER_BASE if c not in TOP_PANEL] +
    [c for c in HOUSING_MARKET if c not in TOP_PANEL + CUSTOMER_BASE] +
    [c for c in TRENDS if c not in TOP_PANEL + CUSTOMER_BASE + HOUSING_MARKET] +
    [c for c in MARKET_SIGNALS if c not in TOP_PANEL + CUSTOMER_BASE + HOUSING_MARKET + TRENDS]
)


# ---------------------------------------------------------------------------
# Human-readable column labels
# ---------------------------------------------------------------------------

COLUMN_LABELS = {
    NSA_ID_COL: "NSA ID",
    NSA_NAME_COL: "NSA Name",
    "pop_total": "Total Population",
    "pop_density_per_km2": "Pop Density (per km²)",
    "med_hh_income": "Median HH Income ($)",
    "med_gross_rent": "Median Gross Rent ($/mo)",
    "med_home_value": "Median Home Value ($)",
    "pct_renters": "% Renter-Occupied",
    "pct_owners": "% Owner-Occupied",
    "pct_vacant": "Vacancy Rate (%)",
    "pct_18_34": "% Age 18–34",
    "pct_35_64": "% Age 35–64",
    "pct_65plus": "% Age 65+",
    "pct_bachelors_plus": "% Bachelor's or Higher",
    "avg_hh_size": "Avg Household Size",
    "pct_commute_drive": "% Drive Alone",
    "pct_commute_transit": "% Public Transit",
    "pct_commute_walk": "% Walk",
    "pct_wfh": "% Work from Home",
    "pct_poverty": "Poverty Rate (%)",
    "pct_pre1950": "% Units Built Pre-1950",
    "pct_moved_past_year": "% Moved in Past Year",
    "pop_total_prior": "Prior Period Population",
    "pop_change_abs": "Population Change (abs)",
    # Top panel additions
    "pct_hh_below50k": "% HH Income Below $50k",
    "pct_hh_50_100k": "% HH Income $50k–$100k",
    "pct_hh_100k_plus": "% HH Income $100k+",
    # Vacancy breakdown
    "pct_vacant_for_rent": "% Vacant — For Rent",
    "pct_vacant_for_sale": "% Vacant — For Sale",
    "pct_vacant_distressed": "% Vacant — Other/Distressed",
    # Trends
    "pop_change_pct": "Population Change (%)",
    "pop_change_pct_vs_city": "Pop Change vs City Avg (pp)",
    "med_hh_income_prior_adj": "Prior Income (CPI-adj $)",
    "income_change_pct": "Income Change (%, inflation-adj)",
    "income_change_pct_vs_city": "Income Change vs City Avg (pp)",
    "med_gross_rent_prior": "Prior Median Rent ($/mo)",
    "rent_change_pct": "Rent Change (%)",
    "rent_change_pct_vs_city": "Rent Change vs City Avg (pp)",
    "med_home_value_prior": "Prior Median Home Value ($)",
    "home_value_change_pct": "Home Value Change (%)",
    "home_value_change_pct_vs_city": "Home Value Change vs City Avg (pp)",
    "pct_vacant_prior": "Prior Vacancy Rate (%)",
    "vacancy_change_pp": "Vacancy Rate Change (pp)",
    "vacancy_change_pp_vs_city": "Vacancy Change vs City Avg (pp)",
    "pct_bachelors_plus_prior": "Prior % Bachelor's+",
    "edu_change_pp": "Education Change (pp)",
    "edu_change_pp_vs_city": "Education Change vs City Avg (pp)",
    # Signals
    "market_signals": "Market Signals",
}


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def build_summary_table(metrics: pd.DataFrame) -> pd.DataFrame:
    """
    Return a clean summary DataFrame with all NSAs, in column order.
    Only includes columns that are present in `metrics`.
    """
    cols = [c for c in ALL_COLS if c in metrics.columns]
    # Add any remaining columns not in the predefined order
    extra = [c for c in metrics.columns if c not in cols and not c.startswith("geometry")]
    return metrics[cols + extra].sort_values(NSA_NAME_COL).reset_index(drop=True)


def profile_for_nsa(metrics: pd.DataFrame, nsa_id) -> pd.DataFrame:
    """
    Return a formatted two-column profile table (Metric | Value) for one NSA.

    Parameters
    ----------
    nsa_id : value matching nsa_id column (int or str)

    Returns
    -------
    DataFrame with columns ['Metric', 'Value'] — suitable for display or CSV.
    """
    row = metrics[metrics[NSA_ID_COL] == nsa_id]
    if row.empty:
        raise ValueError(f"NSA ID {nsa_id!r} not found in metrics")

    row = row.iloc[0]
    records = []

    for col in ALL_COLS:
        if col not in metrics.columns:
            continue
        label = COLUMN_LABELS.get(col, col)
        value = row[col]

        # Format values nicely
        if pd.isna(value):
            formatted = "N/A"
        elif col in ("pop_total", "pop_total_prior", "pop_change_abs"):
            formatted = f"{int(value):,}"
        elif col in ("med_hh_income", "med_home_value", "med_hh_income_prior_adj",
                     "med_gross_rent", "med_gross_rent_prior"):
            formatted = f"${value:,.0f}"
        elif col in ("pop_density_per_km2", "avg_hh_size"):
            formatted = f"{value:.1f}"
        elif "pct" in col or "change" in col:
            formatted = f"{value:.1f}%"
        else:
            formatted = str(value)

        records.append({"Metric": label, "Value": formatted})

    return pd.DataFrame(records)


def export_csvs(
    metrics: pd.DataFrame,
    output_dir: Path = OUTPUT_DIR,
) -> None:
    """
    Write:
      - output/nsa_profiles_all.csv         : master table, all NSAs
      - output/nsa_profiles/<nsa_id>.csv    : individual profile per NSA
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    per_nsa_dir = output_dir / "nsa_profiles"
    per_nsa_dir.mkdir(exist_ok=True)

    summary = build_summary_table(metrics)

    # Master CSV
    master_path = output_dir / "nsa_profiles_all.csv"
    summary.to_csv(master_path, index=False)
    print(f"  Master CSV → {master_path}")

    # Per-NSA profile CSVs
    for _, row in metrics.iterrows():
        nsa_id = row[NSA_ID_COL]
        nsa_name = str(row.get(NSA_NAME_COL, nsa_id)).replace("/", "_").replace(" ", "_")
        profile = profile_for_nsa(metrics, nsa_id)
        path = per_nsa_dir / f"{nsa_id}_{nsa_name}.csv"
        profile.to_csv(path, index=False)

    print(f"  Per-NSA CSVs ({len(metrics)}) → {per_nsa_dir}/")


def export_geospatial(
    metrics: pd.DataFrame,
    nsas: gpd.GeoDataFrame,
    output_dir: Path = OUTPUT_DIR,
) -> None:
    """
    Write NSA polygons with all metrics attached:
      - output/nsa_profiles.geojson    (WGS84)
      - output/nsa_profiles.shp        (State Plane)

    Columns with lists or complex types are dropped (GeoJSON/SHP compatible only).
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Join metrics to NSA geometries
    summary = build_summary_table(metrics)
    geo = nsas[[NSA_ID_COL, "geometry"]].merge(summary, on=NSA_ID_COL, how="left")
    gdf = gpd.GeoDataFrame(geo, geometry="geometry", crs=nsas.crs)

    # GeoJSON in WGS84
    geojson_path = output_dir / "nsa_profiles.geojson"
    gdf.to_crs(CRS_GEOGRAPHIC).to_file(geojson_path, driver="GeoJSON")
    print(f"  GeoJSON → {geojson_path}")

    # Shapefile (column names truncated to 10 chars by fiona)
    shp_path = output_dir / "nsa_profiles.shp"
    gdf.to_file(shp_path)
    print(f"  Shapefile → {shp_path}")
