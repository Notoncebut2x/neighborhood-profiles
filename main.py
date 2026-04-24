"""
main.py
-------
Orchestrates the full NSA demographic profiling workflow:

  1. Load spatial and tabular inputs
  2. Compute population-weighted spatial weights
  3. Allocate ACS block group data to NSAs (current + prior period)
  4. Derive metrics, interpolate medians, compute trends
  5. Run QA checks
  6. Export CSVs and GeoJSON

Usage:
    # First, fetch ACS data (requires Census API key in config.py):
    python fetch_census.py

    # Then run the full pipeline:
    python main.py

    # Or with an optional city population benchmark for QA:
    python main.py --city-pop 585708
"""

import argparse
import sys
import pandas as pd
import geopandas as gpd

from config import (
    ACS_CURRENT_YEAR, ACS_PRIOR_YEAR,
    CPI_ADJUSTMENT, PROCESSED_DIR, NSA_ID_COL,
)
from load import load_nsas, load_block_groups, load_blocks, load_acs
from weights import compute_weights
from allocate import allocate, allocate_weighted_average
from aggregate import build_nsa_metrics
from profiles import build_summary_table, export_csvs, export_geospatial
from qa import run_all_checks


def run(city_total_pop: int | None = None) -> pd.DataFrame:
    """
    Execute the full pipeline and return the final metrics DataFrame.

    Parameters
    ----------
    city_total_pop : optional Census city total population for QA check
    """

    # ==========================================================================
    # STEP 1: Load data
    # ==========================================================================
    print("\n=== Step 1: Loading data ===")

    nsas = load_nsas()
    block_groups = load_block_groups()
    blocks = load_blocks()

    acs_current = load_acs(ACS_CURRENT_YEAR, label="current")
    acs_prior = load_acs(ACS_PRIOR_YEAR, label="prior")

    # ==========================================================================
    # STEP 2: Compute spatial weights
    # ==========================================================================
    print("\n=== Step 2: Computing population weights ===")
    # Intersect blocks with NSAs, derive what fraction of each block group's
    # population falls within each NSA.
    weights = compute_weights(blocks=blocks, nsas=nsas)

    # Persist weights for inspection / reuse
    weights.to_csv(PROCESSED_DIR / "weights.csv", index=False)
    print(f"  Weights saved → {PROCESSED_DIR / 'weights.csv'}")

    # ==========================================================================
    # STEP 3: Allocate ACS data to NSAs
    # ==========================================================================
    print("\n=== Step 3: Allocating ACS data to NSAs ===")

    # --- Current period ---
    allocated_current = allocate(weights, acs_current)

    # Weighted average household size (weighted by housing units, not population)
    if "avg_hh_size" in acs_current.columns and "tenure_total" in acs_current.columns:
        wtd_hh_size = allocate_weighted_average(
            weights, acs_current,
            value_col="avg_hh_size",
            weight_col="tenure_total",
        )
        allocated_current = allocated_current.merge(
            wtd_hh_size.rename("wtd_avg_hh_size").reset_index(),
            on=NSA_ID_COL, how="left"
        )

    # --- Prior period ---
    # Use only the columns available in the prior ACS CSV
    prior_cols = [c for c in acs_current.columns if c in acs_prior.columns]
    allocated_prior = allocate(weights, acs_prior[prior_cols])

    # Persist allocations
    allocated_current.to_csv(PROCESSED_DIR / f"allocated_{ACS_CURRENT_YEAR}.csv", index=False)
    allocated_prior.to_csv(PROCESSED_DIR / f"allocated_{ACS_PRIOR_YEAR}.csv", index=False)

    # ==========================================================================
    # STEP 4: Derive metrics
    # ==========================================================================
    print("\n=== Step 4: Deriving NSA metrics ===")

    cpi_adj = CPI_ADJUSTMENT.get(ACS_PRIOR_YEAR, 1.0)
    metrics = build_nsa_metrics(
        allocated=allocated_current,
        nsas=nsas,
        allocated_prior=allocated_prior,
        cpi_adj=cpi_adj,
    )

    print(f"  Built metrics for {len(metrics)} NSAs")

    # ==========================================================================
    # STEP 5: QA checks
    # ==========================================================================
    print("\n=== Step 5: Quality assurance ===")
    qa_results = run_all_checks(
        weights=weights,
        acs=acs_current,
        allocated=allocated_current,
        metrics=metrics,
        blocks=blocks,
        nsas=nsas,
        city_total_pop=city_total_pop,
    )
    qa_results.summary().to_csv(PROCESSED_DIR / "qa_results.csv", index=False)

    if not qa_results.all_passed():
        print("  WARNING: Some QA checks failed — review qa_results.csv before using outputs")

    # ==========================================================================
    # STEP 6: Export outputs
    # ==========================================================================
    print("\n=== Step 6: Exporting outputs ===")
    export_csvs(metrics)
    export_geospatial(metrics, nsas)

    # Print a preview of the top panel
    summary = build_summary_table(metrics)
    top_cols = [
        "nsa_name", "pop_total", "med_hh_income",
        "med_gross_rent", "pct_renters", "pop_change_pct"
    ]
    preview_cols = [c for c in top_cols if c in summary.columns]
    print("\n=== Preview: Top Panel ===")
    print(summary[preview_cols].to_string(index=False))

    print("\n=== Pipeline complete ===")
    print(f"  Outputs in: output/")
    return metrics


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="NSA Demographic Profiling Pipeline"
    )
    parser.add_argument(
        "--city-pop",
        type=int,
        default=None,
        help="Census city total population for QA benchmark (optional)",
    )
    args = parser.parse_args()

    metrics = run(city_total_pop=args.city_pop)
    sys.exit(0 if metrics is not None else 1)
