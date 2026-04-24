"""
weights.py
----------
Compute population-based weights for allocating ACS block group data to NSAs.

Method:
  For each (NSA, Block Group) pair that spatially overlaps:
    weight = sum(block population in NSA ∩ BG) / sum(all block population in BG)

  This weight represents "what fraction of this block group's population
  falls inside this NSA" — used to proportionally distribute count variables.

Returns a DataFrame with one row per (nsa_id, bg_geoid) overlap, with:
  - weight        : fraction of BG population in this NSA (sums to 1.0 per BG)
  - pop_in_nsa    : raw block population allocated to the NSA piece
  - bg_total_pop  : total block population for the entire BG (denominator)
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from config import NSA_ID_COL, BG_GEOID_COL, BLOCK_GEOID_COL, BLOCK_POP_COL


def compute_weights(
    blocks: gpd.GeoDataFrame,
    nsas: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    Intersect Census blocks with NSAs and derive population weights.

    Parameters
    ----------
    blocks : GeoDataFrame with GEOID20, POP20, GEOID (bg_geoid), geometry
    nsas   : GeoDataFrame with nsa_id, geometry

    Returns
    -------
    DataFrame columns: nsa_id, GEOID (bg), weight, pop_in_nsa, bg_total_pop
    """
    print("  Step 1: Intersecting blocks with NSAs...")

    # Spatial intersection — each block fragment gets the NSA id it falls in.
    # We use a spatial join (centroid-based for blocks that straddle a boundary
    # is not ideal; a true overlay keeps the geometry for area-based fallback).
    # For blocks (small), overlay is reliable.
    blocks_small = blocks[[BLOCK_GEOID_COL, BG_GEOID_COL, BLOCK_POP_COL, "geometry"]].copy()
    nsas_small = nsas[[NSA_ID_COL, "geometry"]].copy()

    # overlay = each block clipped to each NSA it intersects
    intersected = gpd.overlay(
        blocks_small, nsas_small, how="intersection", keep_geom_type=False
    )

    print(f"    Intersection produced {len(intersected):,} block×NSA fragments")

    # -----------------------------------------------------------------------
    # For blocks that straddle an NSA boundary, we need to split population
    # proportionally by area (area-weighted fallback within a single block).
    # First compute the area of each fragment relative to the full block area.
    # -----------------------------------------------------------------------

    # Compute original block areas (before intersection)
    block_areas = (
        blocks_small.set_index(BLOCK_GEOID_COL)["geometry"]
        .area.rename("block_area")
    )
    intersected = intersected.merge(
        block_areas.reset_index(),
        on=BLOCK_GEOID_COL,
        how="left"
    )
    intersected["fragment_area"] = intersected.geometry.area

    # Area fraction of each fragment relative to its parent block
    intersected["area_frac"] = (
        intersected["fragment_area"] / intersected["block_area"]
    ).clip(0, 1)  # numerical guard

    # Population allocated to this NSA fragment of the block
    intersected["frag_pop"] = intersected[BLOCK_POP_COL] * intersected["area_frac"]

    # -----------------------------------------------------------------------
    # Aggregate: sum fragment populations by (NSA, Block Group)
    # -----------------------------------------------------------------------
    print("  Step 2: Aggregating fragment populations by (NSA, Block Group)...")

    weights = (
        intersected
        .groupby([NSA_ID_COL, BG_GEOID_COL])["frag_pop"]
        .sum()
        .reset_index()
        .rename(columns={"frag_pop": "pop_in_nsa"})
    )

    # -----------------------------------------------------------------------
    # Compute block group total populations (denominator)
    # -----------------------------------------------------------------------
    bg_totals = (
        blocks_small
        .groupby(BG_GEOID_COL)[BLOCK_POP_COL]
        .sum()
        .reset_index()
        .rename(columns={BLOCK_POP_COL: "bg_total_pop"})
    )

    weights = weights.merge(bg_totals, on=BG_GEOID_COL, how="left")

    # -----------------------------------------------------------------------
    # Calculate weight: fraction of BG population in this NSA
    # -----------------------------------------------------------------------
    weights["weight"] = np.where(
        weights["bg_total_pop"] > 0,
        weights["pop_in_nsa"] / weights["bg_total_pop"],
        0.0,
    )

    # Normalize so weights sum exactly to 1.0 per block group
    # (floating point errors from area fractions can cause slight drift)
    bg_weight_sums = weights.groupby(BG_GEOID_COL)["weight"].transform("sum")
    weights["weight"] = np.where(
        bg_weight_sums > 0,
        weights["weight"] / bg_weight_sums,
        0.0,
    )

    # Drop zero-weight rows (blocks entirely outside all NSAs)
    weights = weights[weights["weight"] > 0].reset_index(drop=True)

    print(f"    Weight table: {len(weights):,} (NSA, BG) pairs")
    _validate_weights(weights)

    return weights[[NSA_ID_COL, BG_GEOID_COL, "weight", "pop_in_nsa", "bg_total_pop"]]


def _validate_weights(weights: pd.DataFrame) -> None:
    """
    Sanity check: weights per block group should sum to ≤1.0
    (they sum to exactly 1.0 for BGs fully covered by NSAs, less if partially outside).
    """
    bg_sums = weights.groupby(BG_GEOID_COL)["weight"].sum()
    over = (bg_sums > 1.001).sum()
    if over > 0:
        print(f"  WARNING: {over} block groups have weights summing >1.0 — check overlay")
    under = (bg_sums < 0.5).sum()
    if under > 0:
        print(f"  NOTE: {under} block groups have <50% coverage within NSAs (edge BGs expected)")
    print(f"    Weight validation OK — BG coverage: min={bg_sums.min():.3f}, max={bg_sums.max():.3f}")
