# Liquor License Tracker — Dreck Suite

Tracks liquor license activity across the Denver metro area using three public data sources.

## Data Sources

- **Denver ArcGIS** — City liquor licenses with hearing data, neighborhoods, council districts
- **CO Socrata (htyp-tqzh)** — Recently approved state licenses
- **CO Socrata (ier5-5ms2)** — All active state licenses

Cross-references against the BusinessDen Restaurant Tracker for Google rating/review context.

No API keys required — all sources are public and unauthenticated.

## Setup

1. Create repo `businessden/Liquor-tracker` on GitHub
2. Push all files
3. Copy the real `auth.js` from another Dreck Suite repo (e.g., Restaurant-tracker)
4. Enable GitHub Pages: Settings → Pages → Source: GitHub Actions
5. Run the workflow manually to seed initial data
6. Verify at `businessden.github.io/Liquor-tracker/`

## Architecture

- `scraper.py` — Data collection, classification, diff computation, chart data
- `index.html` — Full frontend: KPI cards, dual-axis chart, donut cluster map, filtered license list
- `.github/workflows/scrape.yml` — DST-aware daily cron (5am MT)

## Categories

Records are classified as:
- **New Application** — Recently issued, no prior license at that address
- **Pending** — Application in progress
- **Delinquent** — Behind on fees/compliance
- **Active** — Current valid license
- **Renewal** — Recently re-issued at an address with prior history
- **Closed** — Expired, surrendered, withdrawn, denied, or revoked
