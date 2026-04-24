"""
allocate.py
-----------
Apply population weights to distribute ACS block group data into NSAs.

For COUNT variables (populations, household counts, etc.):
    NSA_value = Σ (weight_i × BG_value_i)   over all BGs intersecting the NSA

For MEDIAN variables (income, rent, home value):
    - Do NOT use this file to allocate the pre-computed Census median directly.
    - Instead, allocate the underlying bracket COUNTS here, then interpolate
      the median from aggregated brackets in aggregate.py.

The avg_hh_size is a weighted average (weighted by number of households).
"""

import pandas as pd
import numpy as np
from config import NSA_ID_COL, BG_GEOID_COL
from fetch_census import (
    INCOME_BRACKET_COLS, RENT_BRACKET_COLS, HOMEVAL_BRACKET_COLS,
)


# ---------------------------------------------------------------------------
# All count columns we will allocate (exclude raw medians — use brackets only)
# ---------------------------------------------------------------------------

# Columns to SKIP during allocation (will be derived later from brackets)
SKIP_COLS = {"avg_hh_size"}   # needs household-weighted mean, handled separately


def allocate(
    weights: pd.DataFrame,
    acs: pd.DataFrame,
    count_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Multiply each block group's ACS counts by population weight,
    then sum to NSA level.

    Parameters
    ----------
    weights   : output of weights.compute_weights()
                columns: nsa_id, GEOID, weight, pop_in_nsa, bg_total_pop
    acs       : ACS DataFrame with GEOID + count columns
    count_cols: list of column names to allocate; defaults to all numeric
                columns in `acs` except GEOID and skip set

    Returns
    -------
    DataFrame indexed by nsa_id with allocated (pre-aggregation) sums.
    """
    # Identify numeric columns to allocate
    if count_cols is None:
        count_cols = [
            c for c in acs.columns
            if c != BG_GEOID_COL and acs[c].dtype in (float, int, "float64", "int64")
            and c not in SKIP_COLS
        ]

    # Merge weights with ACS data on block group GEOID
    merged = weights.merge(
        acs[[BG_GEOID_COL] + count_cols],
        on=BG_GEOID_COL,
        how="left",
    )

    # Multiply each count column by the population weight
    for col in count_cols:
        merged[col] = merged[col] * merged["weight"]

    # Allocate population-in-nsa for use as sub-weight in averages
    merged["pop_in_nsa"] = merged["pop_in_nsa"]  # already present from weights

    # Sum weighted values to NSA level
    agg_dict = {col: "sum" for col in count_cols}
    agg_dict["pop_in_nsa"] = "sum"   # total allocated population per NSA

    allocated = merged.groupby(NSA_ID_COL).agg(agg_dict).reset_index()

    print(f"  Allocated {len(count_cols)} variables to {len(allocated)} NSAs")
    return allocated


def allocate_weighted_average(
    weights: pd.DataFrame,
    acs: pd.DataFrame,
    value_col: str,
    weight_col: str,
) -> pd.Series:
    """
    Compute a population-weighted average of a ratio variable (e.g., avg_hh_size).

    avg = Σ(value_i × pop_weight_i) / Σ(pop_weight_i)

    Parameters
    ----------
    value_col  : column in `acs` holding the per-BG ratio
    weight_col : count column to use as weight (e.g., 'tenure_total' for hh size)

    Returns
    -------
    Series indexed by nsa_id
    """
    merged = weights.merge(
        acs[[BG_GEOID_COL, value_col, weight_col]],
        on=BG_GEOID_COL,
        how="left",
    )

    # Weighted numerator: value × allocated count
    merged["numerator"] = merged[value_col] * merged[weight_col] * merged["weight"]
    merged["denominator"] = merged[weight_col] * merged["weight"]

    result = merged.groupby(NSA_ID_COL).apply(
        lambda g: g["numerator"].sum() / g["denominator"].sum()
        if g["denominator"].sum() > 0 else float("nan")
    )
    result.name = f"wtd_{value_col}"
    return result
