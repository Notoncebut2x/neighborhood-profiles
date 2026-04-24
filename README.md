# Baltimore NSA Demographic Profiles
## Population-Weighted Areal Interpolation — Census → NSA

---

## Project Structure

```
neighborhood_profiles/
├── config.py          # All paths, FIPS codes, CRS, API key
├── fetch_census.py    # Download ACS data via Census API → data/raw/
├── load.py            # Load + reproject shapefiles and ACS CSVs
├── weights.py         # Compute block-based population weights
├── allocate.py        # Apply weights to distribute BG data into NSAs
├── aggregate.py       # Derive metrics; interpolate medians from brackets
├── profiles.py        # Export: master CSV, per-NSA CSVs, GeoJSON, SHP
├── qa.py              # QA checks: balance, coverage, plausible ranges
├── main.py            # Orchestration — run the full pipeline
├── requirements.txt
└── data/
    ├── raw/           # Input files + downloaded ACS CSVs
    └── processed/     # Intermediate outputs (weights, allocations)
output/
    ├── nsa_profiles_all.csv
    ├── nsa_profiles.geojson
    ├── nsa_profiles.shp
    └── nsa_profiles/
        └── <id>_<name>.csv   (one per NSA)
```

---

## Setup

```bash
pip install -r requirements.txt
```

Get a free Census API key: https://api.census.gov/data/key_signup.html

Edit `config.py`:
- Set `CENSUS_API_KEY`
- Set paths to your local shapefiles under `DATA_DIR / "raw/"`
- Confirm `STATE_FIPS = "24"`, `COUNTY_FIPS = "510"` for Baltimore City

---

## Data Inputs Required

| File | Source | Notes |
|------|--------|-------|
| `baltimore_nsas.geojson` | Baltimore City Open Data | Must have `nsa_id`, `nsa_name` columns |
| `tl_2021_24_bg.shp` | Census TIGER/Line BGs | Maryland state file, filtered to county 510 |
| `tl_2020_24510_tabblock20.shp` | Census TIGER/Line Blocks | 2020 blocks for Baltimore City |

Download TIGER files:
- Block Groups: https://www.census.gov/cgi-bin/geo/shapefiles/index.php (select Block Groups, 2021, Maryland)
- Blocks: https://www.census.gov/cgi-bin/geo/shapefiles/index.php (select Blocks, 2020, Maryland, Baltimore City)
- Baltimore NSAs: https://data.baltimorecity.gov/

---

## Running the Pipeline

### Step 1 — Fetch ACS data

```bash
python fetch_census.py
```

Downloads ACS 5-year data for all block groups in Baltimore City for both
the current (2021) and prior (2016) periods. Saves CSVs to `data/raw/`.

### Step 2 — Run the full pipeline

```bash
python main.py

# With Census city population for QA:
python main.py --city-pop 585708
```

---

## Method: Population-Weighted Areal Interpolation

### Why not simple areal interpolation?

Simple areal interpolation assumes uniform population density within each
block group. In Baltimore, density varies sharply within BGs (parks, water,
industrial zones have zero residents). Population-weighted interpolation
uses block-level Census populations to place the weight where people
actually live.

### Weight Calculation

```
For each (NSA, Block Group) pair that spatially overlaps:

  weight(NSA_j, BG_i) = Σ pop(block_k in NSA_j ∩ BG_i)
                         ─────────────────────────────────
                         Σ pop(block_k in BG_i)

  Interpretation: "what fraction of BG_i's population lives in NSA_j"
  Constraint: Σ_j weight(NSA_j, BG_i) = 1.0  for all i
```

### Allocating Count Variables

```
value(NSA_j, variable_v) = Σ_i weight(NSA_j, BG_i) × BG_value(BG_i, v)
```

This applies to: population, household counts, age bins, education counts,
commute counts, bracket counts for income/rent/home value, etc.

### Recalculating Medians (Critical)

**Never average pre-computed Census medians across geographies.**

Instead:
1. Allocate the bracket counts (e.g., B19001 income brackets) to NSA level
2. After summing, interpolate the median from the resulting bracket distribution

```python
# Linear interpolation within the median-containing bracket:
# median = L + [(n/2 - F) / f] × h
# L = lower bound, F = cumulative freq below bracket, f = bracket freq, h = width
```

This is the same method the Census Bureau uses internally.

---

## Output Column Groups

### Top Panel
| Column | Description |
|--------|-------------|
| `pop_total` | Total population |
| `med_hh_income` | Median household income (interpolated) |
| `med_gross_rent` | Median gross rent (interpolated) |
| `pct_renters` | % renter-occupied units |
| `pop_change_pct` | 5-year population change % |

### Customer Base
`pop_density_per_km2`, `pct_18_34`, `pct_35_64`, `pct_65plus`,
`pct_bachelors_plus`, `avg_hh_size`, `pct_commute_drive`,
`pct_commute_transit`, `pct_commute_walk`, `pct_wfh`

### Housing & Market
`med_home_value`, `pct_vacant`, `pct_owners`, `pct_renters`,
`pct_pre1950`, `pct_moved_past_year`, `pct_poverty`

### Trends
`pop_change_abs`, `pop_change_pct`, `income_change_pct` (CPI-adjusted),
`rent_change_pct`, `edu_change_pp`

---

## QA Checks

- Population balance: NSA sum vs Census city total (±2% tolerance)
- Block group coverage: all BGs represented in weight table
- No negative counts in allocated data
- Bracket totals ≈ bracket sum (internal consistency)
- NSA geometry: minimal self-overlap
- Median values within plausible ranges

---

## CPI Inflation Adjustment

Prior-period income/rent is multiplied by `CPI_ADJUSTMENT[year]` in
`config.py` before computing percent change. Update this value from
BLS CPI-U data for the appropriate years.

Default: `CPI_ADJUSTMENT[2016] = 1.109` (2016 → 2021 dollars)

---

## Extending the Workflow

**Add a new ACS variable:**
1. Add the Census variable code + friendly name to `ACS_VARIABLES` in `fetch_census.py`
2. If it's a bracket-based median, add bracket bounds + column list constants
3. Re-run `fetch_census.py` to download the new variable
4. Add derivation logic in `aggregate.py::build_nsa_metrics()`

**Change geography (e.g., use Planning Districts instead of NSAs):**
1. Update `NSA_PATH`, `NSA_ID_COL`, `NSA_NAME_COL` in `config.py`
2. All other code is geography-agnostic
