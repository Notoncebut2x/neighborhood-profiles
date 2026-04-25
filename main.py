"""
main.py
-------
Orchestrates the full demographic profiling workflow for any neighborhood
boundary set. Works with Baltimore NSAs (279) or any other polygon layer.

Usage:
    # Default: Baltimore NSAs
    python main.py --city-pop 585708

    # Any other boundary shapefile:
    python main.py --boundaries path/to/file.shp --name-col Name --output-dir output/why_baltimore
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import geopandas as gpd

from config import (
    ACS_CURRENT_YEAR, ACS_PRIOR_YEAR,
    CPI_ADJUSTMENT, PROCESSED_DIR, NSA_ID_COL, NSA_NAME_COL,
    NSA_PATH, OUTPUT_DIR,
)
from load import load_nsas, load_block_groups, load_blocks, load_acs
from weights import compute_weights
from allocate import allocate, allocate_weighted_average
from aggregate import build_nsa_metrics
from profiles import build_summary_table, export_csvs, export_geospatial
from qa import run_all_checks


def run(
    city_total_pop: int | None = None,
    boundaries_path: Path | None = None,
    id_col: str = NSA_ID_COL,
    name_col: str = NSA_NAME_COL,
    output_dir: Path = OUTPUT_DIR,
) -> pd.DataFrame:
    """
    Execute the full pipeline for a given boundary layer.

    Parameters
    ----------
    city_total_pop    : Census city total population for QA (optional)
    boundaries_path   : path to boundary shapefile/GeoJSON; defaults to config NSA_PATH
    id_col            : unique ID column in the boundary file (auto-created if missing)
    name_col          : human-readable name column in the boundary file
    output_dir        : where to write CSVs, GeoJSON, and shapefiles
    """
    boundaries_path = boundaries_path or NSA_PATH
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    processed_dir = PROCESSED_DIR / output_dir.name if output_dir != OUTPUT_DIR else PROCESSED_DIR
    processed_dir.mkdir(parents=True, exist_ok=True)

    # ==========================================================================
    # STEP 1: Load data
    # ==========================================================================
    print("\n=== Step 1: Loading data ===")

    boundaries = load_nsas(path=boundaries_path, id_col=id_col, name_col=name_col)
    block_groups = load_block_groups()
    blocks = load_blocks()

    acs_current = load_acs(ACS_CURRENT_YEAR, label="current")
    acs_prior = load_acs(ACS_PRIOR_YEAR, label="prior")

    # ==========================================================================
    # STEP 2: Compute spatial weights
    # ==========================================================================
    print("\n=== Step 2: Computing population weights ===")
    weights = compute_weights(blocks=blocks, nsas=boundaries)
    weights.to_csv(processed_dir / "weights.csv", index=False)
    print(f"  Weights saved → {processed_dir / 'weights.csv'}")

    # ==========================================================================
    # STEP 3: Allocate ACS data to boundaries
    # ==========================================================================
    print("\n=== Step 3: Allocating ACS data ===")

    allocated_current = allocate(weights, acs_current)

    if "avg_hh_size" in acs_current.columns and "tenure_total" in acs_current.columns:
        wtd_hh_size = allocate_weighted_average(
            weights, acs_current,
            value_col="avg_hh_size",
            weight_col="tenure_total",
        )
        allocated_current = allocated_current.merge(
            wtd_hh_size.rename("wtd_avg_hh_size").reset_index(),
            on=id_col, how="left"
        )

    prior_cols = [c for c in acs_current.columns if c in acs_prior.columns]
    allocated_prior = allocate(weights, acs_prior[prior_cols])

    allocated_current.to_csv(processed_dir / f"allocated_{ACS_CURRENT_YEAR}.csv", index=False)
    allocated_prior.to_csv(processed_dir / f"allocated_{ACS_PRIOR_YEAR}.csv", index=False)

    # ==========================================================================
    # STEP 4: Derive metrics
    # ==========================================================================
    print("\n=== Step 4: Deriving metrics ===")

    cpi_adj = CPI_ADJUSTMENT.get(ACS_PRIOR_YEAR, 1.0)
    metrics = build_nsa_metrics(
        allocated=allocated_current,
        nsas=boundaries,
        allocated_prior=allocated_prior,
        cpi_adj=cpi_adj,
    )
    print(f"  Built metrics for {len(metrics)} neighborhoods")

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
        nsas=boundaries,
        city_total_pop=city_total_pop,
    )
    qa_results.summary().to_csv(processed_dir / "qa_results.csv", index=False)

    if not qa_results.all_passed():
        print("  WARNING: Some QA checks failed — review qa_results.csv before using outputs")

    # ==========================================================================
    # STEP 6: Export outputs
    # ==========================================================================
    print("\n=== Step 6: Exporting outputs ===")
    export_csvs(metrics, output_dir=output_dir)
    export_geospatial(metrics, boundaries, output_dir=output_dir)

    summary = build_summary_table(metrics)
    preview_cols = [c for c in [name_col, "pop_total", "med_hh_income",
                                "med_gross_rent", "pct_renters", "pop_change_pct"]
                   if c in summary.columns]
    print("\n=== Preview: Top Panel ===")
    print(summary[preview_cols].to_string(index=False))

    print(f"\n=== Pipeline complete — outputs in: {output_dir}/ ===")
    return metrics


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Neighborhood Demographic Profiling Pipeline"
    )
    parser.add_argument(
        "--city-pop", type=int, default=None,
        help="Census city total population for QA benchmark (optional)",
    )
    parser.add_argument(
        "--boundaries", type=Path, default=None,
        help="Path to boundary shapefile or GeoJSON (default: Baltimore NSAs from config)",
    )
    parser.add_argument(
        "--id-col", type=str, default=NSA_ID_COL,
        help=f"Unique ID column in boundary file (default: {NSA_ID_COL}; auto-created if missing)",
    )
    parser.add_argument(
        "--name-col", type=str, default=NSA_NAME_COL,
        help=f"Name column in boundary file (default: {NSA_NAME_COL})",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=OUTPUT_DIR,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    metrics = run(
        city_total_pop=args.city_pop,
        boundaries_path=args.boundaries,
        id_col=args.id_col,
        name_col=args.name_col,
        output_dir=args.output_dir,
    )
    sys.exit(0 if metrics is not None else 1)
