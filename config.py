"""
config.py
---------
All file paths, Census API settings, and constants for the NSA profiling workflow.
Edit this file to point to your local data and set your Census API key.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Directory layout
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = BASE_DIR / "output"

for _d in [RAW_DIR, PROCESSED_DIR, OUTPUT_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Input file paths (edit to match your local files)
# ---------------------------------------------------------------------------

# Baltimore NSA polygon shapefile or GeoJSON
NSA_PATH = RAW_DIR / "baltimore_nsas.geojson"

# TIGER/Line Block Group shapefile for Maryland (state FIPS 24, county 510 = Baltimore City)
BLOCK_GROUP_PATH = RAW_DIR / "tl_2021_24_bg.shp"

# TIGER/Line Block shapefile (2020 decennial) — full Maryland state file, filtered to county 510
BLOCK_PATH = RAW_DIR / "tl_2020_24_tabblock20.shp"

# ---------------------------------------------------------------------------
# Census API
# ---------------------------------------------------------------------------

CENSUS_API_KEY = "e48103f2d06c7ff60c8c04541efa09907eff10cb"

# FIPS codes
STATE_FIPS = "24"        # Maryland
COUNTY_FIPS = "510"      # Baltimore City (independent city)

# ACS vintages (current and prior 5-year period for trend analysis)
ACS_CURRENT_YEAR = 2023   # 2019–2023 ACS 5-year (most recent, released Dec 2024)
ACS_PRIOR_YEAR = 2018     # 2014–2018 ACS 5-year (5-year-prior comparison period)

# Decennial census year for block populations
DECENNIAL_YEAR = 2020

# ---------------------------------------------------------------------------
# Coordinate reference systems
# ---------------------------------------------------------------------------

# Maryland State Plane (meters) — best for area/distance accuracy in Baltimore
CRS_PROJECTED = "EPSG:26985"

# WGS84 — for GeoJSON output
CRS_GEOGRAPHIC = "EPSG:4326"

# ---------------------------------------------------------------------------
# Column name conventions
# ---------------------------------------------------------------------------

NSA_ID_COL = "OBJECTID"      # unique NSA identifier in the NSA shapefile
NSA_NAME_COL = "Name"        # human-readable NSA name
BG_GEOID_COL = "GEOID"       # block group GEOID (12-char: state+county+tract+bg)
BLOCK_GEOID_COL = "GEOID20"  # Census 2020 block GEOID (15-char)
BLOCK_POP_COL = "POP20"      # total population column in the block shapefile

# ---------------------------------------------------------------------------
# Inflation adjustment (CPI-U, to convert ACS_PRIOR_YEAR dollars to ACS_CURRENT_YEAR)
# Adjust as needed from BLS CPI data
# ---------------------------------------------------------------------------

CPI_ADJUSTMENT = {
    2016: 1.109,   # 2016 → 2021 dollars
    2018: 1.138,   # 2018 → 2023 dollars (BLS CPI-U: 251.1 / 220.8 ≈ 1.138)
}

# ---------------------------------------------------------------------------
# QA tolerance
# ---------------------------------------------------------------------------

# Allow up to this fraction difference between summed NSA population and
# the Census-reported city total before raising a warning
QA_POPULATION_TOLERANCE = 0.02   # 2 %
