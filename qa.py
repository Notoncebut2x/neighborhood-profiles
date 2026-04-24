"""
qa.py
-----
Quality assurance checks for the NSA profiling workflow.

Checks:
  1. Population balance: sum of NSA populations ≈ city total (within tolerance)
  2. Weight completeness: all block groups covered by at least one NSA
  3. No negative counts in allocated data
  4. Bracket totals match column totals (internal consistency)
  5. Geometry validity of NSA union vs city boundary
"""

import numpy as np
import pandas as pd
import geopandas as gpd
from config import NSA_ID_COL, BG_GEOID_COL, QA_POPULATION_TOLERANCE
from fetch_census import INCOME_BRACKET_COLS, RENT_BRACKET_COLS, HOMEVAL_BRACKET_COLS


class QAResults:
    """Collects and reports QA check results."""

    def __init__(self):
        self.checks: list[dict] = []

    def add(self, name: str, passed: bool, detail: str = ""):
        status = "PASS" if passed else "FAIL"
        self.checks.append({"check": name, "status": status, "detail": detail})
        icon = "✓" if passed else "✗"
        print(f"  [{icon}] {name}: {detail}")

    def summary(self) -> pd.DataFrame:
        return pd.DataFrame(self.checks)

    def all_passed(self) -> bool:
        return all(c["status"] == "PASS" for c in self.checks)


def run_all_checks(
    weights: pd.DataFrame,
    acs: pd.DataFrame,
    allocated: pd.DataFrame,
    metrics: pd.DataFrame,
    blocks: gpd.GeoDataFrame,
    nsas: gpd.GeoDataFrame,
    city_total_pop: int | None = None,
) -> QAResults:
    """
    Run all QA checks and return results.

    Parameters
    ----------
    city_total_pop : expected total city population (from Decennial Census);
                     if None, skips the absolute population check.
    """
    results = QAResults()

    print("\n--- QA Checks ---")

    # ------------------------------------------------------------------
    # 1. Population balance
    # ------------------------------------------------------------------
    nsa_pop_total = metrics["pop_total"].sum()

    if city_total_pop is not None:
        diff_frac = abs(nsa_pop_total - city_total_pop) / city_total_pop
        results.add(
            "Population balance (vs Census total)",
            diff_frac <= QA_POPULATION_TOLERANCE,
            f"NSA sum={nsa_pop_total:,.0f}, Census={city_total_pop:,}, "
            f"diff={diff_frac:.2%} (tolerance {QA_POPULATION_TOLERANCE:.0%})",
        )
    else:
        results.add(
            "Population balance (internal)",
            True,
            f"NSA total population = {nsa_pop_total:,.0f} (no external benchmark provided)",
        )

    # ------------------------------------------------------------------
    # 2. Weight coverage — every BG should be represented
    # ------------------------------------------------------------------
    bg_in_acs = set(acs[BG_GEOID_COL].unique())
    bg_in_weights = set(weights[BG_GEOID_COL].unique())
    uncovered = bg_in_acs - bg_in_weights
    results.add(
        "Block group coverage",
        len(uncovered) == 0,
        f"{len(bg_in_weights)} / {len(bg_in_acs)} BGs have weights "
        + (f"({len(uncovered)} uncovered)" if uncovered else "(all covered)"),
    )

    # ------------------------------------------------------------------
    # 3. No negative count columns in allocated data
    # ------------------------------------------------------------------
    count_cols = [c for c in allocated.columns if c not in [NSA_ID_COL, "pop_in_nsa"]]
    neg_counts = {c: (allocated[c] < -0.5).sum() for c in count_cols if c in allocated.columns}
    neg_counts = {k: v for k, v in neg_counts.items() if v > 0}
    results.add(
        "No negative counts in allocated data",
        len(neg_counts) == 0,
        "All OK" if not neg_counts else f"Negatives in: {neg_counts}",
    )

    # ------------------------------------------------------------------
    # 4. Internal bracket consistency — bracket sums ≈ total
    # ------------------------------------------------------------------
    # Note: B25063_001 (rent_total_units) includes "no cash rent" units (B25063_002)
    # which are not in any price bracket — so bracket sums will always be < total.
    # We check rent separately below. Income and home value brackets are exhaustive.
    for bracket_cols, total_col, label, tolerance in [
        (INCOME_BRACKET_COLS, "hhinc_total_hh", "Income", 0.05),
        (HOMEVAL_BRACKET_COLS, "homeval_total_units", "Home value", 0.05),
    ]:
        avail = [c for c in bracket_cols if c in allocated.columns]
        if total_col in allocated.columns and avail:
            bracket_sum = allocated[avail].sum(axis=1)
            total = allocated[total_col]
            max_diff = ((bracket_sum - total) / total.replace(0, np.nan)).abs().max()
            results.add(
                f"{label} bracket consistency",
                max_diff < tolerance,
                f"Max relative diff between bracket sum and total: {max_diff:.2%}",
            )

    # Rent: informational only — B25063_001 total includes no-cash-rent units
    if "rent_total_units" in allocated.columns:
        avail_rent = [c for c in RENT_BRACKET_COLS if c in allocated.columns]
        city_bracket = allocated[avail_rent].sum().sum()
        city_total = allocated["rent_total_units"].sum()
        no_cash_gap = city_total - city_bracket
        results.add(
            "Rent bracket consistency (city-level)",
            True,   # informational — gap is always expected
            f"City bracket sum={city_bracket:,.0f}, total={city_total:,.0f}, "
            f"no-cash-rent gap={no_cash_gap:,.0f} ({no_cash_gap/city_total:.1%}) — expected",
        )

    # ------------------------------------------------------------------
    # 5. NSA geometry: no significant overlap between NSAs
    # ------------------------------------------------------------------
    try:
        union = nsas.geometry.unary_union
        nsa_total_area = nsas.geometry.area.sum()
        overlap_ratio = (nsa_total_area - union.area) / union.area
        results.add(
            "NSA geometry — no significant overlap",
            overlap_ratio < 0.01,
            f"Overlap ratio: {overlap_ratio:.4f} (should be ~0)",
        )
    except Exception as e:
        results.add("NSA geometry check", False, f"Error: {e}")

    # ------------------------------------------------------------------
    # 6. No NSA with zero population (industrial/park NSAs expected to be zero)
    # ------------------------------------------------------------------
    zero_pop = (metrics["pop_total"] == 0).sum()
    zero_names = metrics.loc[metrics["pop_total"] == 0, "Name"].tolist() if zero_pop > 0 else []
    results.add(
        "NSAs with zero population",
        True,  # zero-pop NSAs like industrial areas are expected — informational only
        f"{zero_pop} zero-pop NSAs (expected for industrial/park areas): {zero_names}"
        if zero_pop > 0 else "All NSAs have population > 0",
    )

    # ------------------------------------------------------------------
    # 7. Median values in plausible ranges
    # ------------------------------------------------------------------
    def _check_range(col, lo, hi, label):
        if col in metrics.columns:
            out = ((metrics[col] < lo) | (metrics[col] > hi)).sum()
            results.add(
                f"{label} plausible range [{lo:,}–{hi:,}]",
                out == 0,
                f"{out} NSAs out of range" if out > 0 else "All within range",
            )

    _check_range("med_hh_income", 5_000, 500_000, "Median HH income")
    _check_range("med_gross_rent", 100, 10_000, "Median gross rent")
    _check_range("med_home_value", 10_000, 5_000_000, "Median home value")

    print(f"\n  {'PASSED' if results.all_passed() else 'SOME CHECKS FAILED'} "
          f"({sum(1 for c in results.checks if c['status']=='PASS')} / "
          f"{len(results.checks)} checks passed)\n")

    return results
