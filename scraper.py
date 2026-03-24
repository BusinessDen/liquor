#!/usr/bin/env python3
"""
Denver Liquor License Tracker - Scraper
Pulls from:
1. Colorado CIM Socrata API (recently approved + all active statewide)
2. Denver ArcGIS feature service (city-level with hearing data)

Produces:
- liquor-data.json (main data file for the frontend)
- data/previous.json (snapshot for next diff)
"""

import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone

# ── Configuration ──────────────────────────────────────────────────────
SOCRATA_BASE = "https://data.colorado.gov/resource"
SOCRATA_APPROVED = "htyp-tqzh"  # Recently approved
SOCRATA_ACTIVE = "ier5-5ms2"    # All active licenses

ARCGIS_BASE = (
    "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/"
    "ODC_BUSN_LIQUORLICENSES_P/FeatureServer/27/query"
)

# Denver metro cities to include from statewide Socrata data
METRO_CITIES = [
    "Denver", "Aurora", "Lakewood", "Englewood", "Littleton",
    "Westminster", "Arvada", "Thornton", "Golden", "Broomfield",
    "Commerce City", "Northglenn", "Federal Heights", "Wheat Ridge",
    "Edgewater", "Sheridan", "Glendale", "Greenwood Village",
    "Cherry Hills Village", "Centennial", "Lone Tree", "Highlands Ranch",
    "Parker", "Castle Rock", "Brighton", "Erie", "Louisville", "Superior",
    "Lafayette", "Boulder"
]

# License types most relevant to BusinessDen readers
NEWSWORTHY_TYPES_STATE = [
    "Hotel & Restaurant",
    "Tavern",
    "Beer & Wine",
    "Brew Pub",
    "Entertainment Facility",
    "Retail Liquor Store",
    "Lodging Facility",
]

# ── API Helpers ────────────────────────────────────────────────────────

def fetch_json(url):
    """Fetch JSON from a URL with retry."""
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "BusinessDen-Tracker/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt == 2:
                raise
    return None


def fetch_socrata(dataset_id, where_clause=None, limit=5000):
    """Fetch from Socrata SODA API with pagination."""
    all_records = []
    offset = 0
    while True:
        params = {"$limit": str(limit), "$offset": str(offset)}
        if where_clause:
            params["$where"] = where_clause
        url = f"{SOCRATA_BASE}/{dataset_id}.json?{urllib.parse.urlencode(params)}"
        print(f"  Fetching {dataset_id} offset={offset}...")
        data = fetch_json(url)
        if not data:
            break
        all_records.extend(data)
        if len(data) < limit:
            break
        offset += limit
    return all_records


def fetch_arcgis(where="1=1", fields="*", max_records=5000):
    """Fetch from ArcGIS feature service with pagination."""
    all_features = []
    offset = 0
    batch = 1000
    while True:
        params = {
            "where": where,
            "outFields": fields,
            "outSR": "4326",
            "f": "json",
            "resultRecordCount": str(batch),
            "resultOffset": str(offset),
        }
        url = f"{ARCGIS_BASE}?{urllib.parse.urlencode(params)}"
        print(f"  Fetching ArcGIS offset={offset}...")
        data = fetch_json(url)
        features = data.get("features", [])
        all_features.extend(features)
        if len(features) < batch or len(all_features) >= max_records:
            break
        offset += batch
    return all_features


# ── Data Processing ────────────────────────────────────────────────────

def ts_to_iso(ms_timestamp):
    """Convert millisecond timestamp to ISO date string."""
    if not ms_timestamp:
        return None
    try:
        if isinstance(ms_timestamp, str):
            ms_timestamp = int(float(ms_timestamp))
        dt = datetime.fromtimestamp(ms_timestamp / 1000, tz=timezone.utc)
        # Sanity check: skip dates far in the future (bad data)
        if dt.year > 2100 or dt.year < 1990:
            return None
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError, TypeError):
        return None


def normalize_socrata_record(rec, source):
    """Normalize a Socrata record to common format."""
    loc = rec.get("location", {})
    lat = loc.get("latitude")
    lng = loc.get("longitude")

    return {
        "id": f"state-{rec.get('license_number', '')}",
        "source": source,
        "name": (rec.get("licensee_name") or "").strip(),
        "dba": (rec.get("doing_business_as") or "").strip(),
        "license_type": (rec.get("license_type") or "").strip(),
        "license_number": (rec.get("license_number") or "").strip(),
        "status": "Approved" if source == "state_approved" else "Active",
        "address": (rec.get("street_address") or "").strip(),
        "city": (rec.get("city") or "").strip(),
        "state": "CO",
        "zip": (rec.get("zip") or "").replace(".0", "").strip(),
        "lat": float(lat) if lat else None,
        "lng": float(lng) if lng else None,
        "issue_date": rec.get("issue_date", "")[:10] if rec.get("issue_date") else None,
        "expiration_date": rec.get("expiration", "")[:10] if rec.get("expiration") else None,
        "neighborhood": None,
        "council_district": None,
        "hearing_date": None,
        "hearing_time": None,
        "hearing_status": None,
        "rt_match": None,
    }


def normalize_arcgis_record(feat):
    """Normalize an ArcGIS feature to common format."""
    a = feat.get("attributes", {})
    geo = feat.get("geometry", {})

    lat = geo.get("y") if geo else None
    lng = geo.get("x") if geo else None

    # Filter out bad coordinates (must be in Colorado)
    if lat and (lat < 36 or lat > 42):
        lat = None
    if lng and (lng > -100 or lng < -110):
        lng = None

    return {
        "id": f"denver-{a.get('BFN', '')}",
        "source": "denver",
        "name": (a.get("BUS_PROF_NAME") or "").strip(),
        "dba": "",
        "license_type": (a.get("LICENSES") or "").replace("LIQUOR - ", "").strip(),
        "license_number": (a.get("BFN") or "").strip(),
        "status": (a.get("LIC_STATUS") or "").strip(),
        "address": (a.get("FULL_ADDRESS") or a.get("ADDRESS_LINE1") or "").strip(),
        "city": "Denver",
        "state": "CO",
        "zip": (a.get("ZIP") or "").strip(),
        "lat": lat,
        "lng": lng,
        "issue_date": ts_to_iso(a.get("ISSUE_DATE")),
        "expiration_date": ts_to_iso(a.get("END_DATE")),
        "neighborhood": (a.get("NEIGHBORHOOD") or "").strip() or None,
        "council_district": (a.get("COUNCIL_DIST") or "").strip() or None,
        "hearing_date": ts_to_iso(a.get("HEARING_DATE")),
        "hearing_time": (a.get("HEARING_TIME") or "").strip() or None,
        "hearing_status": (a.get("HEARING_STATUS") or "").strip() or None,
        "rt_match": None,
    }


def compute_diff(current_records, previous_records):
    """Compute what changed since last run."""
    prev_ids = {r["id"] for r in previous_records}
    curr_ids = {r["id"] for r in current_records}

    prev_by_id = {r["id"]: r for r in previous_records}
    curr_by_id = {r["id"]: r for r in current_records}

    new_ids = curr_ids - prev_ids
    removed_ids = prev_ids - curr_ids

    # Check for status changes
    status_changes = []
    for rid in curr_ids & prev_ids:
        old_status = prev_by_id[rid].get("status", "")
        new_status = curr_by_id[rid].get("status", "")
        if old_status != new_status:
            status_changes.append({
                "id": rid,
                "name": curr_by_id[rid]["name"],
                "old_status": old_status,
                "new_status": new_status,
                "address": curr_by_id[rid]["address"],
            })

    return {
        "new": [curr_by_id[rid] for rid in new_ids],
        "removed": [prev_by_id[rid] for rid in removed_ids],
        "status_changes": status_changes,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Denver Liquor License Tracker - Scraper")
    print(f"Run time: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    all_records = []

    # 1. Fetch Denver ArcGIS data (non-special-events only)
    print("\n[1/3] Fetching Denver ArcGIS data...")
    arcgis_where = "LICENSES NOT LIKE '%SPECIAL%'"
    arcgis_features = fetch_arcgis(where=arcgis_where)
    print(f"  Got {len(arcgis_features)} Denver features")

    for feat in arcgis_features:
        rec = normalize_arcgis_record(feat)
        if rec["name"]:
            all_records.append(rec)

    # 2. Fetch state recently approved (Denver metro)
    print("\n[2/3] Fetching state recently approved licenses...")
    city_clauses = " OR ".join([f"city='{c}'" for c in METRO_CITIES])
    where = f"({city_clauses})"
    approved = fetch_socrata(SOCRATA_APPROVED, where_clause=where)
    print(f"  Got {len(approved)} recently approved records")

    for rec in approved:
        normalized = normalize_socrata_record(rec, "state_approved")
        # Skip special events and non-newsworthy types from state data
        lt = normalized["license_type"]
        if any(nw in lt for nw in NEWSWORTHY_TYPES_STATE):
            all_records.append(normalized)

    # 3. Fetch state active licenses (Denver metro, for expiration tracking)
    print("\n[3/3] Fetching state active licenses (metro)...")
    active = fetch_socrata(SOCRATA_ACTIVE, where_clause=where)
    print(f"  Got {len(active)} active state records")

    # Only include state active records that aren't already in Denver ArcGIS
    denver_addresses = {r["address"].upper() for r in all_records if r["source"] == "denver"}
    state_added = 0
    for rec in active:
        normalized = normalize_socrata_record(rec, "state_active")
        lt = normalized["license_type"]
        if any(nw in lt for nw in NEWSWORTHY_TYPES_STATE):
            if normalized["city"] != "Denver" or normalized["address"].upper() not in denver_addresses:
                all_records.append(normalized)
                state_added += 1

    print(f"  Added {state_added} non-duplicate state records")

    # ── Cross-reference with restaurant tracker ──
    print("\n[4/4] Cross-referencing with restaurant tracker...")
    try:
        rt_url = "https://businessden.github.io/Restaurant-tracker/restaurant-data.json"
        rt_data = fetch_json(rt_url)
        rt_restaurants = rt_data.get("restaurants", [])
        print(f"  Loaded {len(rt_restaurants)} restaurant tracker entries")

        # Build lookup by normalized address prefix and by proximity
        def normalize_addr(addr):
            """Normalize address for fuzzy matching."""
            if not addr:
                return ""
            # Take just the street number + street name, uppercase, strip unit/suite
            import re
            a = addr.upper().strip()
            a = re.split(r',\s*', a)[0]  # Drop city/state/zip
            a = re.sub(r'\s+(STE|SUITE|UNIT|APT|#)\s*\S*', '', a)
            a = re.sub(r'\s+', ' ', a).strip()
            return a

        rt_by_addr = {}
        rt_by_coords = []
        for rt in rt_restaurants:
            na = normalize_addr(rt.get("address", ""))
            if na:
                rt_by_addr[na] = rt
            if rt.get("lat") and rt.get("lng"):
                rt_by_coords.append(rt)

        matched = 0
        for rec in all_records:
            # Try address match first
            na = normalize_addr(rec.get("address", ""))
            rt_match = rt_by_addr.get(na)

            # If no address match but we have coords, try proximity (~50m)
            if not rt_match and rec.get("lat") and rec.get("lng"):
                for rt in rt_by_coords:
                    dlat = abs(rec["lat"] - rt["lat"])
                    dlng = abs(rec["lng"] - rt["lng"])
                    if dlat < 0.0005 and dlng < 0.0006:  # ~50m
                        rt_match = rt
                        break

            if rt_match:
                rec["rt_match"] = {
                    "name": rt_match.get("name", ""),
                    "rating": rt_match.get("rating"),
                    "reviews": rt_match.get("user_ratings_total"),
                    "type": rt_match.get("primary_type_display", ""),
                    "status": rt_match.get("business_status", ""),
                    "first_seen": rt_match.get("first_seen"),
                }
                matched += 1

        print(f"  Matched {matched} records to restaurant tracker")

    except Exception as e:
        print(f"  Warning: Restaurant tracker cross-reference failed: {e}")

    # ── Compute diff ──
    print("\n[Diff] Computing changes...")
    os.makedirs("data", exist_ok=True)
    previous = []
    if os.path.exists("data/previous.json"):
        with open("data/previous.json", "r") as f:
            previous = json.load(f)

    diff = compute_diff(all_records, previous)
    print(f"  New: {len(diff['new'])} | Removed: {len(diff['removed'])} | Status changes: {len(diff['status_changes'])}")

    # ── Compute summary stats ──
    status_counts = {}
    type_counts = {}
    neighborhood_counts = {}
    city_counts = {}

    for r in all_records:
        s = r.get("status") or "Unknown"
        status_counts[s] = status_counts.get(s, 0) + 1

        t = r.get("license_type") or "Unknown"
        type_counts[t] = type_counts.get(t, 0) + 1

        n = r.get("neighborhood")
        if n:
            neighborhood_counts[n] = neighborhood_counts.get(n, 0) + 1

        c = r.get("city") or "Unknown"
        city_counts[c] = city_counts.get(c, 0) + 1

    # ── Save outputs ──
    output = {
        "metadata": {
            "generated": datetime.now(timezone.utc).isoformat(),
            "total_records": len(all_records),
            "sources": {
                "denver_arcgis": len([r for r in all_records if r["source"] == "denver"]),
                "state_approved": len([r for r in all_records if r["source"] == "state_approved"]),
                "state_active": len([r for r in all_records if r["source"] == "state_active"]),
            },
        },
        "summary": {
            "by_status": dict(sorted(status_counts.items(), key=lambda x: -x[1])),
            "by_type": dict(sorted(type_counts.items(), key=lambda x: -x[1])),
            "by_neighborhood": dict(sorted(neighborhood_counts.items(), key=lambda x: -x[1])[:30]),
            "by_city": dict(sorted(city_counts.items(), key=lambda x: -x[1])),
        },
        "diff": {
            "new_count": len(diff["new"]),
            "removed_count": len(diff["removed"]),
            "status_changes_count": len(diff["status_changes"]),
            "new": diff["new"][:50],
            "removed": diff["removed"][:50],
            "status_changes": diff["status_changes"][:50],
            "timestamp": diff["timestamp"],
        },
        "records": all_records,
    }

    with open("liquor-data.json", "w") as f:
        json.dump(output, f, separators=(",", ":"))
    print(f"\nWrote liquor-data.json ({len(all_records)} records)")

    # Save current as previous for next diff
    with open("data/previous.json", "w") as f:
        json.dump(all_records, f, separators=(",", ":"))
    print("Saved data/previous.json for next diff")

    print(f"\n{'='*60}")
    print("Summary:")
    print(f"  Total records: {len(all_records)}")
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"    {status}: {count}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
