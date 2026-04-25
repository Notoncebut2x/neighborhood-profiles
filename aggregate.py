"""
aggregate.py
------------
Post-allocation aggregation: derive clean metrics from the allocated counts.

Key principle: NEVER average pre-computed medians across geographies.
Instead, re-interpolate medians from the allocated bracket counts.

Steps per NSA:
  1. Sum bracket counts  (done in allocate.py)
  2. Interpolate median from cumulative frequency within sorted brackets
  3. Compute rates and percentages from summed counts
  4. Compute derived metrics (density, etc.) using NSA geometry
"""

import numpy as np
import pandas as pd
import geopandas as gpd
from config import NSA_ID_COL, NSA_NAME_COL, CRS_PROJECTED
from fetch_census import (
    INCOME_BRACKETS, INCOME_BRACKET_COLS,
    RENT_BRACKETS, RENT_BRACKET_COLS,
    HOMEVAL_BRACKETS, HOMEVAL_BRACKET_COLS,
)


# ---------------------------------------------------------------------------
# Median interpolation
# ---------------------------------------------------------------------------

def interpolate_median(counts: pd.Series, brackets: list[tuple]) -> float:
    """
    Estimate the median from grouped frequency data using linear interpolation.

    Parameters
    ----------
    counts   : array-like of counts per bracket (must align with `brackets`)
    brackets : list of (lower, upper) bound tuples; last bracket upper may be inf

    Returns
    -------
    Estimated median value (float), or NaN if total is 0 or data is missing.

    Method
    ------
    Standard Ogive / interpolation within the median-containing bracket:
      median = L + [(n/2 - F) / f] × h
    where:
      L = lower bound of median bracket
      F = cumulative freq below the median bracket
      f = freq in the median bracket
      h = bracket width (or a reasonable proxy for open-ended top bracket)
    """
    counts = np.array(counts, dtype=float)
    if np.isnan(counts).all() or counts.sum() == 0:
        return float("nan")

    total = counts.sum()
    target = total / 2.0
    cumulative = 0.0

    for i, (lower, upper) in enumerate(brackets):
        freq = counts[i]
        if np.isnan(freq):
            freq = 0.0

        if cumulative + freq >= target:
            if freq == 0:
                return float("nan")

            if upper == float("inf") or i == len(brackets) - 1:
                # Top-coded bracket: return lower bound as best estimate
                return float(lower)

            width = upper - lower
            fraction = (target - cumulative) / freq
            return lower + fraction * width

        cumulative += freq

    return float("nan")


def _row_median(row: pd.Series, bracket_cols: list, brackets: list) -> float:
    """Apply interpolate_median to a single DataFrame row."""
    return interpolate_median(row[bracket_cols].values, brackets)


# ---------------------------------------------------------------------------
# Main aggregation function
# ---------------------------------------------------------------------------

def build_nsa_metrics(
    allocated: pd.DataFrame,
    nsas: gpd.GeoDataFrame,
    allocated_prior: pd.DataFrame | None = None,
    cpi_adj: float = 1.0,
) -> pd.DataFrame:
    """
    Derive all NSA-level metrics from allocated block group data.

    Parameters
    ----------
    allocated       : output of allocate.allocate() for the current period
    nsas            : NSA GeoDataFrame (for area / name lookup)
    allocated_prior : allocated data for the prior ACS period (for trends)
    cpi_adj         : CPI adjustment factor to inflate prior-period income/rent
                      to current-period dollars

    Returns
    -------
    DataFrame: one row per NSA, all derived metrics as columns.
    """
    df = allocated.copy()

    # ------------------------------------------------------------------
    # Attach NSA name and area (km²)
    # ------------------------------------------------------------------
    nsa_meta = nsas[[NSA_ID_COL, NSA_NAME_COL, "geometry"]].copy()
    nsa_meta["area_km2"] = nsa_meta.geometry.area / 1e6  # m² → km²
    df = df.merge(nsa_meta[[NSA_ID_COL, NSA_NAME_COL, "area_km2"]], on=NSA_ID_COL, how="left")

    # ------------------------------------------------------------------
    # TOP PANEL: population, medians, tenure
    # ------------------------------------------------------------------

    df["pop_total"] = df["pop_total"].round().astype(int)

    # Population density (per km²)
    df["pop_density_per_km2"] = (df["pop_total"] / df["area_km2"]).round(1)

    # Median household income — interpolated from brackets
    df["med_hh_income"] = df.apply(
        lambda r: _row_median(r, INCOME_BRACKET_COLS, INCOME_BRACKETS), axis=1
    ).round(0)

    # Median gross rent — interpolated from brackets
    df["med_gross_rent"] = df.apply(
        lambda r: _row_median(r, RENT_BRACKET_COLS, RENT_BRACKETS), axis=1
    ).round(0)

    # Median home value — interpolated from brackets
    df["med_home_value"] = df.apply(
        lambda r: _row_median(r, HOMEVAL_BRACKET_COLS, HOMEVAL_BRACKETS), axis=1
    ).round(0)

    # % renters
    df["pct_renters"] = _safe_pct(df["tenure_renter"], df["tenure_total"])

    # % owners
    df["pct_owners"] = _safe_pct(df["tenure_owner"], df["tenure_total"])

    # ------------------------------------------------------------------
    # CUSTOMER BASE: age, education, household, commute, mobility
    # ------------------------------------------------------------------

    # Age 18–34
    age_18_34_cols = [
        "male_18_19", "male_20", "male_21", "male_22_24", "male_25_29", "male_30_34",
        "female_18_19", "female_20", "female_21", "female_22_24", "female_25_29", "female_30_34",
    ]
    df["pop_18_34"] = df[[c for c in age_18_34_cols if c in df.columns]].sum(axis=1)

    # Age 35–64
    age_35_64_cols = [
        "male_35_39", "male_40_44", "male_45_49", "male_50_54", "male_55_59",
        "male_60_61", "male_62_64",
        "female_35_39", "female_40_44", "female_45_49", "female_50_54", "female_55_59",
        "female_60_61", "female_62_64",
    ]
    df["pop_35_64"] = df[[c for c in age_35_64_cols if c in df.columns]].sum(axis=1)

    # Age 65+
    age_65plus_cols = [
        "male_65_66", "male_67_69", "male_70_74", "male_75_79", "male_80_84", "male_85plus",
        "female_65_66", "female_67_69", "female_70_74", "female_75_79", "female_80_84", "female_85plus",
    ]
    df["pop_65plus"] = df[[c for c in age_65plus_cols if c in df.columns]].sum(axis=1)

    df["pct_18_34"] = _safe_pct(df["pop_18_34"], df["pop_total"])
    df["pct_35_64"] = _safe_pct(df["pop_35_64"], df["pop_total"])
    df["pct_65plus"] = _safe_pct(df["pop_65plus"], df["pop_total"])

    # Education: % bachelor's or higher (of pop 25+)
    df["edu_bachelors_plus"] = df[
        ["edu_bachelors", "edu_masters", "edu_professional", "edu_doctorate"]
    ].sum(axis=1)
    df["pct_bachelors_plus"] = _safe_pct(df["edu_bachelors_plus"], df["edu_total_25plus"])

    # Average household size (weighted average was computed in allocate step;
    # if available use it; otherwise compute from persons / households)
    if "wtd_avg_hh_size" in df.columns:
        df["avg_hh_size"] = df["wtd_avg_hh_size"].round(2)
    else:
        df["avg_hh_size"] = (df["pop_total"] / df["tenure_total"].replace(0, np.nan)).round(2)

    # Commuting
    df["pct_commute_drive"] = _safe_pct(df["commute_drive_alone"], df["commute_total"])
    df["pct_commute_carpool"] = _safe_pct(df["commute_carpool"], df["commute_total"])
    df["pct_commute_transit"] = _safe_pct(df["commute_transit"], df["commute_total"])
    df["pct_commute_walk"] = _safe_pct(df["commute_walk"], df["commute_total"])
    df["pct_wfh"] = _safe_pct(df["commute_wfh"], df["commute_total"])

    # Residential mobility — % who moved in the past year
    moved_cols = [c for c in df.columns if c.startswith("mobility_moved")]
    if moved_cols:
        df["pop_moved_past_year"] = df[moved_cols].sum(axis=1)
        df["pct_moved_past_year"] = _safe_pct(df["pop_moved_past_year"], df["mobility_total"])

    # ------------------------------------------------------------------
    # TOP PANEL: income tier bands
    # ------------------------------------------------------------------

    below50k_cols = [
        "hhinc_lt10k", "hhinc_10_15k", "hhinc_15_20k", "hhinc_20_25k",
        "hhinc_25_30k", "hhinc_30_35k", "hhinc_35_40k", "hhinc_40_45k", "hhinc_45_50k",
    ]
    mid_cols = ["hhinc_50_60k", "hhinc_60_75k", "hhinc_75_100k"]
    high_cols = ["hhinc_100_125k", "hhinc_125_150k", "hhinc_150_200k", "hhinc_200k_plus"]

    df["hh_below50k"] = df[[c for c in below50k_cols if c in df.columns]].sum(axis=1)
    df["hh_50_100k"]  = df[[c for c in mid_cols       if c in df.columns]].sum(axis=1)
    df["hh_100k_plus"]= df[[c for c in high_cols      if c in df.columns]].sum(axis=1)

    df["pct_hh_below50k"] = _safe_pct(df["hh_below50k"],  df["hhinc_total_hh"])
    df["pct_hh_50_100k"]  = _safe_pct(df["hh_50_100k"],   df["hhinc_total_hh"])
    df["pct_hh_100k_plus"]= _safe_pct(df["hh_100k_plus"], df["hhinc_total_hh"])

    # ------------------------------------------------------------------
    # HOUSING & MARKET
    # ------------------------------------------------------------------

    # Vacancy rate — total
    df["pct_vacant"] = _safe_pct(df["housing_vacant"], df["housing_total_units"])

    # Vacancy breakdown by reason (B25004)
    if "vacant_for_rent" in df.columns:
        # Transitional: for rent or for sale
        df["pct_vacant_for_rent"] = _safe_pct(df["vacant_for_rent"], df["housing_total_units"])
        df["pct_vacant_for_sale"] = _safe_pct(df["vacant_for_sale"], df["housing_total_units"])
        # Distressed: other vacant (abandoned, long-term empty)
        distressed_cols = ["vacant_other", "vacant_rented_not_occ", "vacant_sold_not_occ",
                           "vacant_seasonal", "vacant_migrant"]
        df["vacant_distressed"] = df[[c for c in distressed_cols if c in df.columns]].sum(axis=1)
        df["pct_vacant_distressed"] = _safe_pct(df["vacant_distressed"], df["housing_total_units"])

    # % housing units built pre-1950 (owner + renter combined)
    pre1950_cols = ["yrbuilt_owner_1940_49", "yrbuilt_owner_pre1940",
                    "yrbuilt_renter_1940_49", "yrbuilt_renter_pre1940"]
    df["units_pre1950"] = df[[c for c in pre1950_cols if c in df.columns]].sum(axis=1)
    df["pct_pre1950"] = _safe_pct(df["units_pre1950"], df["yrbuilt_total"])

    # Poverty rate
    if "poverty_below" in df.columns:
        df["pct_poverty"] = _safe_pct(df["poverty_below"], df["poverty_total_denom"])

    # ------------------------------------------------------------------
    # TRENDS (if prior period data provided)
    # ------------------------------------------------------------------

    if allocated_prior is not None:
        prior = allocated_prior.copy()

        # Population change
        prior_pop = prior[[NSA_ID_COL, "pop_total"]].rename(columns={"pop_total": "pop_total_prior"})
        df = df.merge(prior_pop, on=NSA_ID_COL, how="left")
        df["pop_change_abs"] = df["pop_total"] - df["pop_total_prior"]
        df["pop_change_pct"] = _safe_pct_change(df["pop_total"], df["pop_total_prior"])

        # Median income change (inflation-adjusted)
        prior["med_hh_income_prior"] = prior.apply(
            lambda r: _row_median(r, INCOME_BRACKET_COLS, INCOME_BRACKETS), axis=1
        )
        prior["med_hh_income_prior_adj"] = prior["med_hh_income_prior"] * cpi_adj
        df = df.merge(
            prior[[NSA_ID_COL, "med_hh_income_prior", "med_hh_income_prior_adj"]],
            on=NSA_ID_COL, how="left"
        )
        df["income_change_pct"] = _safe_pct_change(
            df["med_hh_income"], df["med_hh_income_prior_adj"]
        )

        # Median rent change
        prior["med_gross_rent_prior"] = prior.apply(
            lambda r: _row_median(r, RENT_BRACKET_COLS, RENT_BRACKETS), axis=1
        )
        df = df.merge(prior[[NSA_ID_COL, "med_gross_rent_prior"]], on=NSA_ID_COL, how="left")
        df["rent_change_pct"] = _safe_pct_change(df["med_gross_rent"], df["med_gross_rent_prior"])

        # Median home value change
        homeval_prior_avail = all(c in prior.columns for c in HOMEVAL_BRACKET_COLS)
        if homeval_prior_avail:
            prior["med_home_value_prior"] = prior.apply(
                lambda r: _row_median(r, HOMEVAL_BRACKET_COLS, HOMEVAL_BRACKETS), axis=1
            )
            df = df.merge(prior[[NSA_ID_COL, "med_home_value_prior"]], on=NSA_ID_COL, how="left")
            df["home_value_change_pct"] = _safe_pct_change(df["med_home_value"], df["med_home_value_prior"])

        # Vacancy rate change
        if "housing_total_units" in prior.columns and "housing_vacant" in prior.columns:
            prior["pct_vacant_prior"] = (
                prior["housing_vacant"] / prior["housing_total_units"].replace(0, np.nan) * 100
            ).round(1)
            df = df.merge(prior[[NSA_ID_COL, "pct_vacant_prior"]], on=NSA_ID_COL, how="left")
            df["vacancy_change_pp"] = (df["pct_vacant"] - df["pct_vacant_prior"]).round(1)

        # Education change
        edu_cols = ["edu_bachelors", "edu_masters", "edu_professional", "edu_doctorate"]
        if all(c in prior.columns for c in edu_cols) and "edu_total_25plus" in prior.columns:
            prior_edu = prior[[NSA_ID_COL]].copy()
            prior_edu["edu_bachelors_plus_prior"] = prior[edu_cols].sum(axis=1)
            prior_edu["pct_bachelors_plus_prior"] = _safe_pct(
                prior_edu["edu_bachelors_plus_prior"], prior["edu_total_25plus"]
            )
            df = df.merge(prior_edu[[NSA_ID_COL, "pct_bachelors_plus_prior"]], on=NSA_ID_COL, how="left")
            df["edu_change_pp"] = (df["pct_bachelors_plus"] - df["pct_bachelors_plus_prior"]).round(1)

        # ------------------------------------------------------------------
        # VS CITY-WIDE AVERAGES (population-weighted)
        # ------------------------------------------------------------------
        trend_cols = [
            "pop_change_pct", "income_change_pct", "rent_change_pct",
            "home_value_change_pct", "vacancy_change_pp", "edu_change_pp",
        ]
        city_avgs = {}
        for col in trend_cols:
            if col in df.columns:
                city_avgs[col] = _city_wtd_avg(df, col, "pop_total")
                df[f"{col}_vs_city"] = (df[col] - city_avgs[col]).round(1)

        # ------------------------------------------------------------------
        # MARKET SIGNALS
        # ------------------------------------------------------------------
        df["market_signals"] = df.apply(
            lambda r: _market_signals(r, city_avgs, df), axis=1
        )

    return df


# ---------------------------------------------------------------------------
# City-wide weighted average helper
# ---------------------------------------------------------------------------

def _city_wtd_avg(df: pd.DataFrame, col: str, weight_col: str) -> float:
    """Population-weighted mean of `col` across all NSAs."""
    valid = df[df[col].notna() & df[weight_col].notna() & (df[weight_col] > 0)]
    if valid.empty:
        return float("nan")
    return (valid[col] * valid[weight_col]).sum() / valid[weight_col].sum()


# ---------------------------------------------------------------------------
# Market signal generator
# ---------------------------------------------------------------------------

def _market_signals(row: pd.Series, city_avgs: dict, df: pd.DataFrame) -> str:
    """
    Return a semicolon-separated string of interpretive market signals for one NSA.
    Each signal fires when a specific combination of metrics crosses a threshold.
    """
    signals = []

    def get(col, default=float("nan")):
        v = row.get(col, default)
        return default if pd.isna(v) else v

    city_rent_chg    = city_avgs.get("rent_change_pct", float("nan"))
    city_inc_chg     = city_avgs.get("income_change_pct", float("nan"))
    city_pop_chg     = city_avgs.get("pop_change_pct", float("nan"))
    city_edu_chg     = city_avgs.get("edu_change_pp", float("nan"))
    city_vac_chg     = city_avgs.get("vacancy_change_pp", float("nan"))
    city_hv_chg      = city_avgs.get("home_value_change_pct", float("nan"))
    city_moved       = _city_wtd_avg(df, "pct_moved_past_year", "pop_total") if "pct_moved_past_year" in df.columns else float("nan")
    city_pct_vacant  = _city_wtd_avg(df, "pct_vacant", "pop_total") if "pct_vacant" in df.columns else float("nan")
    city_pct_18_34   = _city_wtd_avg(df, "pct_18_34", "pop_total") if "pct_18_34" in df.columns else float("nan")

    rent_chg   = get("rent_change_pct")
    inc_chg    = get("income_change_pct")
    pop_chg    = get("pop_change_pct")
    pct_vacant = get("pct_vacant")
    pct_18_34  = get("pct_18_34")
    moved      = get("pct_moved_past_year")
    edu_chg    = get("edu_change_pp")
    vac_chg    = get("vacancy_change_pp")
    hv_chg     = get("home_value_change_pct")

    # Renter demand rising: rents up faster than city + vacancy below city avg
    if not (np.isnan(rent_chg) or np.isnan(city_rent_chg) or np.isnan(pct_vacant) or np.isnan(city_pct_vacant)):
        if rent_chg > city_rent_chg and pct_vacant < city_pct_vacant:
            signals.append("Renter demand rising")

    # Incomes rising faster than city
    if not (np.isnan(inc_chg) or np.isnan(city_inc_chg)):
        if inc_chg > city_inc_chg + 10:
            signals.append("Household incomes rising faster than city")

    # Young adult population growing: above-avg share + positive pop growth
    if not (np.isnan(pct_18_34) or np.isnan(city_pct_18_34) or np.isnan(pop_chg)):
        if pct_18_34 > city_pct_18_34 and pop_chg > 0:
            signals.append("Young adult population growing")

    # High residential turnover: moved rate well above city avg
    if not (np.isnan(moved) or np.isnan(city_moved)):
        if moved > city_moved * 1.4:
            signals.append("High residential turnover — retail capture opportunity")

    # Affordability stress: rents rising faster than incomes
    if not (np.isnan(rent_chg) or np.isnan(inc_chg)):
        if rent_chg > (inc_chg + 15):
            signals.append("Affordability stress — rents outpacing incomes")

    # Gentrification indicators: income + rent + education all rising above city
    if not any(np.isnan(v) for v in [inc_chg, city_inc_chg, rent_chg, city_rent_chg, edu_chg, city_edu_chg]):
        if inc_chg > city_inc_chg + 5 and rent_chg > city_rent_chg and edu_chg > city_edu_chg + 2:
            signals.append("Gentrification indicators present")

    # Vacancy distress: high and worsening vacancy
    if not (np.isnan(pct_vacant) or np.isnan(vac_chg)):
        if pct_vacant > 20 and vac_chg > 2:
            signals.append("Vacancy distress — disinvestment risk")

    # Home value appreciation: outpacing city
    if not (np.isnan(hv_chg) or np.isnan(city_hv_chg)):
        if hv_chg > city_hv_chg + 15:
            signals.append("Home value appreciation outpacing city")

    # Population decline — absolute threshold (city avg skewed by redevelopment outliers)
    if not np.isnan(pop_chg):
        if pop_chg < -15:
            signals.append("Significant population loss")

    return "; ".join(signals) if signals else "No strong signals"


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _safe_pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Compute percentage with zero-division protection. Returns 0–100 float."""
    return (numerator / denominator.replace(0, np.nan) * 100).round(1)


def _safe_pct_change(current: pd.Series, prior: pd.Series) -> pd.Series:
    """Compute % change from prior to current. Returns 0–100+ float."""
    return ((current - prior) / prior.replace(0, np.nan) * 100).round(1)
