"""
load.py
-------
Load, validate, and reproject all spatial and tabular inputs.
Returns clean GeoDataFrames ready for intersection.
"""

import geopandas as gpd
import pandas as pd
from pathlib import Path
from config import (
    NSA_PATH, BLOCK_GROUP_PATH, BLOCK_PATH,
    NSA_ID_COL, NSA_NAME_COL, BG_GEOID_COL,
    BLOCK_GEOID_COL, BLOCK_POP_COL,
    STATE_FIPS, COUNTY_FIPS,
    CRS_PROJECTED, RAW_DIR,
    ACS_CURRENT_YEAR, ACS_PRIOR_YEAR,
)


# ---------------------------------------------------------------------------
# Spatial loaders
# ---------------------------------------------------------------------------

def load_nsas(
    path: Path = NSA_PATH,
    id_col: str = NSA_ID_COL,
    name_col: str = NSA_NAME_COL,
) -> gpd.GeoDataFrame:
    """
    Load neighborhood boundary polygons.

    If `id_col` is not present in the file, a sequential integer ID is created
    automatically — useful for shapefiles that only have a name field.
    Reprojected to Maryland State Plane (EPSG:26985).
    """
    gdf = gpd.read_file(path)

    # Auto-create numeric ID if the specified column doesn't exist
    if id_col not in gdf.columns:
        print(f"  NOTE: '{id_col}' not found — creating sequential ID from row index")
        gdf[id_col] = range(1, len(gdf) + 1)

    _require_columns(gdf, [name_col], "boundary shapefile")

    gdf = gdf[[id_col, name_col, "geometry"]].copy()
    gdf = gdf.to_crs(CRS_PROJECTED)
    gdf = gdf[gdf.geometry.is_valid & ~gdf.geometry.is_empty].reset_index(drop=True)

    print(f"  Boundaries loaded: {len(gdf)} polygons")
    return gdf


def load_block_groups(path: Path = BLOCK_GROUP_PATH,
                      state: str = STATE_FIPS,
                      county: str = COUNTY_FIPS) -> gpd.GeoDataFrame:
    """
    Load TIGER/Line block group shapefile, filtered to the study county.

    The TIGER file may cover an entire state — we filter to Baltimore City
    (county FIPS 510) before returning.
    """
    gdf = gpd.read_file(path)

    # TIGER columns: STATEFP, COUNTYFP, TRACTCE, BLKGRPCE, GEOID
    state_col = _find_col(gdf, ["STATEFP", "STATEFP10", "STATEFP20"])
    county_col = _find_col(gdf, ["COUNTYFP", "COUNTYFP10", "COUNTYFP20"])
    geoid_col = _find_col(gdf, ["GEOID", "GEOID10", "GEOID20"])

    gdf = gdf.rename(columns={geoid_col: BG_GEOID_COL})
    gdf = gdf[(gdf[state_col] == state) & (gdf[county_col] == county)].copy()
    gdf = gdf[[BG_GEOID_COL, "geometry"]].to_crs(CRS_PROJECTED)
    gdf = gdf[gdf.geometry.is_valid & ~gdf.geometry.is_empty].reset_index(drop=True)

    print(f"  Block groups loaded: {len(gdf)}")
    return gdf


def load_blocks(path: Path = BLOCK_PATH,
                state: str = STATE_FIPS,
                county: str = COUNTY_FIPS) -> gpd.GeoDataFrame:
    """
    Load Census 2020 block shapefile with population counts.

    The 2020 TIGER block file is state-level; we filter to Baltimore City
    (county FIPS 510) before returning.
    GEOID20 is a 15-character FIPS code (state+county+tract+block).
    """
    gdf = gpd.read_file(path)

    # Filter to Baltimore City
    state_col = _find_col(gdf, ["STATEFP20", "STATEFP10", "STATEFP"])
    county_col = _find_col(gdf, ["COUNTYFP20", "COUNTYFP10", "COUNTYFP"])
    gdf = gdf[(gdf[state_col] == state) & (gdf[county_col] == county)].copy()

    geoid_col = _find_col(gdf, ["GEOID20", "GEOID10", "GEOID"])
    pop_col = _find_col(gdf, ["POP20", "POP10", "POPULATION"])

    gdf = gdf.rename(columns={geoid_col: BLOCK_GEOID_COL, pop_col: BLOCK_POP_COL})
    gdf = gdf[[BLOCK_GEOID_COL, BLOCK_POP_COL, "geometry"]].to_crs(CRS_PROJECTED)

    gdf[BLOCK_POP_COL] = pd.to_numeric(gdf[BLOCK_POP_COL], errors="coerce").fillna(0)
    gdf = gdf[gdf.geometry.is_valid & ~gdf.geometry.is_empty].reset_index(drop=True)

    # Derive 12-char block group GEOID from the 15-char block GEOID
    # Block GEOID: SSCCCTTTTTBBBB (2+3+6+4=15); BG GEOID: SSCCCTTTTTB (first 12)
    gdf[BG_GEOID_COL] = gdf[BLOCK_GEOID_COL].str[:12]

    print(f"  Blocks loaded: {len(gdf):,}  |  total pop: {gdf[BLOCK_POP_COL].sum():,.0f}")
    return gdf


# ---------------------------------------------------------------------------
# Tabular loaders
# ---------------------------------------------------------------------------

def load_acs(year: int, label: str = "current") -> pd.DataFrame:
    """Load a previously fetched ACS CSV from data/raw/."""
    path = RAW_DIR / f"acs_{year}_{label}.csv"
    df = pd.read_csv(path, dtype={BG_GEOID_COL: str})
    print(f"  ACS {year} loaded: {len(df):,} block groups, {df.shape[1]} columns")
    return df


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _require_columns(df: pd.DataFrame, cols: list, name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"{name} is missing required columns: {missing}\n"
            f"Available columns: {list(df.columns)}"
        )


def _find_col(gdf: gpd.GeoDataFrame, candidates: list) -> str:
    for col in candidates:
        if col in gdf.columns:
            return col
    raise KeyError(f"None of {candidates} found in columns: {list(gdf.columns)}")
