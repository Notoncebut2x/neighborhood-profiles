# Baltimore Neighborhood Profiles — Plain English Guide

## What is this?

This project creates a demographic and housing snapshot for each of Baltimore's **279 Neighborhood Statistical Areas (NSAs)** — the official neighborhood boundaries used by Baltimore City government for planning and analysis.

The problem it solves: the U.S. Census Bureau collects data in its own geographic units (called block groups) that don't line up with Baltimore's neighborhood boundaries. This project mathematically redistributes that Census data into NSA boundaries so you can answer questions like *"what is the median household income in Hampden?"* or *"how much has rent changed in Canton?"*

---

## Where does the data come from?

**Neighborhood boundaries**
The NSA polygons come from Baltimore City's open data portal. There are 279 NSAs covering all of Baltimore City.

**Population counts**
Block-level population counts come from the **2020 Decennial Census** — the actual head count the Census Bureau conducts every 10 years. These are the most accurate population figures available and are used specifically to figure out how to split data across neighborhood boundaries.

**Demographic and housing data**
Everything else comes from the **American Community Survey (ACS) 5-year estimates**, which is a rolling survey the Census Bureau runs continuously. The "5-year" refers to the fact that the results are pooled across five years to get large enough sample sizes at the neighborhood level.

- **Current period:** 2019–2023 ACS (the most recent release, published December 2024)
- **Prior period:** 2014–2018 ACS (used to calculate 5-year trends)

The ACS covers things the Decennial Census doesn't: income, rent, home values, education, commuting, poverty, and more.

---

## How was the data redistibuted into neighborhoods?

This is the technical heart of the project. Here's the plain-English version:

### The problem
A Census block group might cover parts of two different neighborhoods. If you just split it down the middle, you'd be assuming equal numbers of people on both sides — which is rarely true. A park, a school, or a factory could sit on one side with zero residents.

### The solution: population-weighted interpolation
Instead of splitting by area, we split by where people actually live.

1. **Start with Census blocks** — the smallest Census geography, about the size of a city block. Each block has an official population count from the 2020 Census.

2. **Map blocks to neighborhoods** — for each Census block, figure out which NSA it falls in.

3. **Calculate a weight for each block group / neighborhood pair** — if 30% of a block group's population lives in NSA A and 70% lives in NSA B, then NSA A gets 30% of that block group's data and NSA B gets 70%.

4. **Apply the weights** — every count (number of renters, commuters, degree holders, etc.) gets multiplied by the appropriate weight and summed into each NSA.

### Why medians are handled differently
You can't calculate an average median. If one block group has a median income of $30,000 and another has $90,000, the combined median is not $60,000 — it depends on the actual distribution of households across all income levels.

To solve this, the Census provides income, rent, and home value data in *brackets* (e.g., how many households earn $30,000–$35,000). We allocate those bracket counts to each NSA using the population weights, then re-calculate the median from scratch using the combined bracket distribution. This is the same method the Census Bureau itself uses.

---

## What's in the output?

Each NSA gets a row in the master table (`output/nsa_profiles_all.csv`) with the following columns:

### Snapshot metrics
| Column | What it means |
|--------|--------------|
| `pop_total` | Total population (2019–2023) |
| `med_hh_income` | Median household income — half of households earn more, half earn less |
| `med_gross_rent` | Median monthly rent including utilities |
| `med_home_value` | Median value of owner-occupied homes |
| `pct_renters` | Share of occupied housing units that are rented |
| `pct_vacant` | Share of all housing units that are vacant |
| `pct_poverty` | Share of residents below the federal poverty line |

### Age & education
| Column | What it means |
|--------|--------------|
| `pct_18_34` | Share of population aged 18–34 |
| `pct_35_64` | Share of population aged 35–64 |
| `pct_65plus` | Share of population aged 65+ |
| `pct_bachelors_plus` | Share of adults 25+ with at least a bachelor's degree |
| `avg_hh_size` | Average number of people per household |

### Housing stock
| Column | What it means |
|--------|--------------|
| `pct_pre1950` | Share of housing units built before 1950 — a rough proxy for older rowhouse stock |
| `pct_moved_past_year` | Share of residents who moved to their current address in the past year — a measure of neighborhood turnover |

### Commuting
| Column | What it means |
|--------|--------------|
| `pct_commute_drive` | Share of workers who drive alone to work |
| `pct_commute_transit` | Share of workers who take public transit |
| `pct_commute_walk` | Share of workers who walk to work |
| `pct_wfh` | Share of workers who work from home |

### Trends (2014–2018 vs 2019–2023)
| Column | What it means |
|--------|--------------|
| `pop_change_pct` | Percent population change over the five-year window |
| `pop_change_abs` | Absolute population change (people gained or lost) |
| `income_change_pct` | Percent change in median income, adjusted for inflation |
| `rent_change_pct` | Percent change in median rent |

Inflation adjustment: prior-period incomes are multiplied by **1.138** to convert 2018 dollars to 2023 dollars (based on the BLS Consumer Price Index).

---

## What the outputs look like

**`output/nsa_profiles_all.csv`**
One row per NSA, all metrics in a single table. Good for sorting, filtering, and analysis in Excel or Python.

**`output/nsa_profiles/<id>_<name>.csv`**
A formatted two-column profile (Metric / Value) for each individual NSA. Good for reports or sharing a single neighborhood.

**`output/nsa_profiles.geojson`**
All NSA polygons with every metric attached. Drop it into QGIS, Mapbox, or Kepler.gl to make maps.

**`output/nsa_profiles.shp`**
Same as the GeoJSON but in Shapefile format for GIS software.

---

## Important caveats

**ACS data is a survey, not a count.** Unlike the Decennial Census, ACS figures come with margins of error. For small neighborhoods (under ~1,000 people), estimates can be unreliable. Treat figures for very small NSAs as approximate.

**Interpolation introduces a small error.** Our population-weighted method is the industry standard for this kind of analysis, but it is still an approximation. The city-wide totals match the Census benchmark within 1.5%, which is well within acceptable tolerance.

**Trends compare different five-year windows, not single years.** A "5-year population change" of +10% means the 2019–2023 average is 10% higher than the 2014–2018 average — not that population grew 10% from one specific year to another.

**Redevelopment areas show extreme swings.** Neighborhoods undergoing major construction (Poppleton, Baltimore Peninsula, Perkins Homes) show dramatic percentage changes that reflect demolition and rebuilding rather than organic growth or decline.
