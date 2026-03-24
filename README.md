# Denver Liquor License Tracker

Part of the **Dreck Suite** — internal newsroom tools for BusinessDen.

## What it does

Tracks liquor license activity across the Denver metro area by pulling from two public data sources:

1. **Colorado CIM / Socrata API** (state-level) — Recently approved licenses and all active licenses statewide, filtered to Denver metro cities
2. **Denver ArcGIS feature service** (city-level) — Richer data including neighborhood, council district, zone district, hearing dates, and hearing status

The scraper runs daily at 5am MT via GitHub Actions, computes a diff against the previous day's data, and publishes results to GitHub Pages.

## Newsroom value

- **Pending licenses** = restaurants/bars planning to open (2–4 months lead time)
- **Recently approved** = opening imminent
- **Surrendered/expired** = closures
- **Delinquent** = potential trouble
- **Status changes** = movement in the pipeline
- **Hearing dates** = earliest possible signal of a new venue

## Data sources

| Source | Dataset ID | Records | Update frequency |
|--------|-----------|---------|-----------------|
| CO Socrata: Recently Approved | `htyp-tqzh` | ~950 | Monthly |
| CO Socrata: All Active | `ier5-5ms2` | ~20,000 | Monthly |
| Denver ArcGIS | `ODC_BUSN_LIQUORLICENSES_P/27` | ~10,000 | Weekly |

## Setup

1. Create repo `businessden/Liquor-tracker` on GitHub
2. Copy all files from this directory
3. Copy `auth.js` from another Dreck Suite repo (e.g., Restaurant-tracker)
4. Enable GitHub Pages (deploy from GitHub Actions)
5. Run the workflow manually to seed initial data

No API keys required — all data sources are public.

## Files

- `index.html` — Frontend (map + table + filters)
- `scraper.py` — Data collection script
- `auth.js` — Shared Dreck Suite authentication
- `.github/workflows/scrape.yml` — GitHub Actions workflow
- `liquor-data.json` — Generated data file (committed by scraper)
- `data/previous.json` — Previous snapshot for diff computation
