#!/usr/bin/env python3
"""
Denver Metro Liquor License Tracker — Scraper
Pulls from 3 public data sources, cross-references against BusinessDen restaurant tracker,
computes diffs, classifies new vs renewal, and generates chart data.
"""

import json
import os
import re
import math
import urllib.request
import urllib.parse
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

METRO_CITIES = [
    "Denver", "Aurora", "Lakewood", "Arvada", "Westminster", "Thornton",
    "Centennial", "Boulder", "Littleton", "Broomfield", "Englewood",
    "Wheat Ridge", "Golden", "Commerce City", "Northglenn", "Federal Heights",
    "Sheridan", "Edgewater", "Glendale", "Cherry Hills Village",
    "Greenwood Village", "Lone Tree", "Parker", "Castle Rock", "Brighton",
    "Louisville", "Lafayette", "Superior", "Erie"
]

NEWSWORTHY_TYPES = [
    "HOTEL AND RESTAURANT", "TAVERN", "BREW PUB", "DISTILLERY PUB",
    "VINTNER'S RESTAURANT", "BEER AND WINE", "CLUB", "RESORT COMPLEX",
    "ARTS", "OPTIONAL PREMISES", "RETAIL LIQUOR STORE", "LIQUOR-LICENSED DRUGSTORE",
    "FERMENTED MALT BEVERAGE", "Hotel and Restaurant", "Tavern", "Brew Pub",
    "Retail Liquor Store", "Beer and Wine", "Club", "Arts",
    "Distillery Pub", "Vintner's Restaurant", "Optional Premises"
]

ARCGIS_URL = (
    "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/"
    "ODC_BUSN_LIQUORLICENSES_P/FeatureServer/27/query"
)

SOCRATA_BASE = "https://data.colorado.gov/resource"
APPROVED_DATASET = "htyp-tqzh"
ACTIVE_DATASET = "ier5-5ms2"

RT_URL = "https://businessden.github.io/Restaurant-tracker/restaurant-data.json"

OUTPUT_FILE = "liquor-data.json"
PREVIOUS_FILE = "data/previous.json"
CHART_FILE = "data/chart-history.json"

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def fetch_json(url, retries=3, timeout=30):
    """Fetch JSON from URL with retries."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "BusinessDen-Tracker/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  Attempt {attempt+1}/{retries} failed for {url[:80]}...: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def fetch_socrata(dataset_id, where_clause, limit=5000):
    """Paginated Socrata SODA API fetch."""
    records = []
    offset = 0
    while True:
        params = urllib.parse.urlencode({
            "$where": where_clause,
            "$limit": limit,
            "$offset": offset,
            "$order": ":id"
        })
        url = f"{SOCRATA_BASE}/{dataset_id}.json?{params}"
        data = fetch_json(url)
        if not data:
            break
        records.extend(data)
        if len(data) < limit:
            break
        offset += limit
        print(f"  Fetched {len(records)} records so far...")
    return records


def fetch_arcgis(where="1=1", fields="*", max_records=None):
    """Paginated ArcGIS feature service fetch."""
    records = []
    offset = 0
    batch_size = 1000
    while True:
        params = urllib.parse.urlencode({
            "where": where,
            "outFields": fields,
            "outSR": "4326",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": batch_size
        })
        url = f"{ARCGIS_URL}?{params}"
        data = fetch_json(url)
        if not data or "features" not in data:
            break
        features = data["features"]
        records.extend(features)
        if len(features) < batch_size:
            break
        offset += batch_size
        if max_records and len(records) >= max_records:
            break
        print(f"  Fetched {len(records)} ArcGIS records...")
    return records

# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def ts_to_iso(ms_timestamp):
    """Convert millisecond timestamp to ISO date string. Filters junk dates."""
    if ms_timestamp is None:
        return None
    try:
        if isinstance(ms_timestamp, str):
            # Try parsing ISO string directly
            if "T" in ms_timestamp or "-" in ms_timestamp:
                dt = datetime.fromisoformat(ms_timestamp.replace("Z", "+00:00"))
                if 1990 <= dt.year <= 2100:
                    return dt.strftime("%Y-%m-%d")
                return None
            ms_timestamp = int(ms_timestamp)
        ts = ms_timestamp / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        if 1990 <= dt.year <= 2100:
            return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError, OSError):
        pass
    return None


def normalize_addr(addr):
    """Normalize address for cross-referencing."""
    if not addr:
        return ""
    addr = addr.upper().strip()
    # Take first segment before comma
    addr = addr.split(",")[0].strip()
    # Remove unit/suite designations
    addr = re.sub(r"\s+(UNIT|STE|SUITE|APT|#)\s*\S*", "", addr)
    # Remove extra whitespace
    addr = re.sub(r"\s+", " ", addr)
    return addr


def haversine(lat1, lng1, lat2, lng2):
    """Distance in meters between two lat/lng points."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def normalize_arcgis_record(feat):
    """Normalize ArcGIS feature to common format."""
    attrs = feat.get("attributes", {})
    geom = feat.get("geometry", {})

    lat = geom.get("y")
    lng = geom.get("x")

    # Bounds check — must be in Colorado
    if lat and lng:
        if not (36 <= lat <= 42 and -110 <= lng <= -100):
            return None

    bfn = attrs.get("BFN", "")
    issue_date = ts_to_iso(attrs.get("ISSUE_DATE"))
    end_date = ts_to_iso(attrs.get("END_DATE"))
    hearing_date = ts_to_iso(attrs.get("HEARING_DATE"))

    return {
        "id": f"denver-{bfn}",
        "source": "denver",
        "name": (attrs.get("BUS_PROF_NAME") or "").strip(),
        "dba": "",
        "license_type": (attrs.get("LICENSES") or "").strip(),
        "license_number": str(bfn),
        "status": (attrs.get("LIC_STATUS") or "").strip(),
        "address": (attrs.get("FULL_ADDRESS") or "").strip(),
        "city": (attrs.get("CITY") or "Denver").strip(),
        "state": "CO",
        "zip": (attrs.get("ZIP") or "").strip(),
        "lat": lat,
        "lng": lng,
        "issue_date": issue_date,
        "expiration_date": end_date,
        "neighborhood": (attrs.get("NEIGHBORHOOD") or "").strip(),
        "council_district": str(attrs.get("COUNCIL_DIST") or ""),
        "hearing_date": hearing_date,
        "hearing_time": (attrs.get("HEARING_TIME") or "").strip(),
        "hearing_status": (attrs.get("HEARING_STATUS") or "").strip(),
    }


def normalize_socrata_record(rec, source):
    """Normalize Socrata record to common format."""
    lat = None
    lng = None
    location = rec.get("location") or rec.get("location_1")
    if location:
        if isinstance(location, dict):
            lat = location.get("latitude")
            lng = location.get("longitude")
            if lat:
                lat = float(lat)
            if lng:
                lng = float(lng)
        elif isinstance(location, str):
            # Try "POINT (lng lat)" format
            m = re.search(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", location)
            if m:
                lng, lat = float(m.group(1)), float(m.group(2))

    issue_date = ts_to_iso(rec.get("issue_date"))
    exp_date = ts_to_iso(rec.get("expiration") or rec.get("expiration_date"))

    name = (rec.get("licensee_name") or "").strip()
    dba = (rec.get("doing_business_as") or "").strip()
    lic_type = (rec.get("license_type") or "").strip()
    lic_num = (rec.get("license_number") or "").strip()
    address = (rec.get("street_address") or "").strip()
    city = (rec.get("city") or "").strip()
    state = (rec.get("state") or "CO").strip()
    zipcode = (rec.get("zip") or rec.get("zip_code") or "").strip()

    status = "Approved" if source == "state_approved" else "Active"

    record_id = f"{source}-{lic_num or name}"

    return {
        "id": record_id,
        "source": source,
        "name": name,
        "dba": dba,
        "license_type": lic_type,
        "license_number": lic_num,
        "status": status,
        "address": address,
        "city": city,
        "state": state,
        "zip": zipcode,
        "lat": lat,
        "lng": lng,
        "issue_date": issue_date,
        "expiration_date": exp_date,
        "neighborhood": "",
        "council_district": "",
        "hearing_date": None,
        "hearing_time": "",
        "hearing_status": "",
    }

# ---------------------------------------------------------------------------
# Classification: New vs Renewal
# ---------------------------------------------------------------------------

def classify_records(records, previous_records):
    """
    Classify each record as 'new_application', 'renewal', 'delinquent',
    'pending', 'closed', or 'active'. Adds 'category' field.
    """
    # Build address lookup from previous data
    prev_addresses = set()
    if previous_records:
        for rec in previous_records:
            addr = normalize_addr(rec.get("address", ""))
            if addr:
                prev_addresses.add(addr)

    now = datetime.now(timezone.utc)
    one_year_ago = now - timedelta(days=365)

    for rec in records:
        status = (rec.get("status") or "").upper()

        # Closed statuses
        if "CLOSED" in status or "EXPIRED" in status:
            rec["category"] = "closed"
            continue

        # Delinquent
        if "DELINQUENT" in status:
            rec["category"] = "delinquent"
            continue

        # Pending
        if "PENDING" in status:
            rec["category"] = "pending"
            continue

        # Denied / Revoked
        if "DENIED" in status or "REVOKED" in status:
            rec["category"] = "closed"
            continue

        # For active/approved licenses, determine if new or renewal
        issue_date_str = rec.get("issue_date")
        addr = normalize_addr(rec.get("address", ""))
        is_recent = False

        if issue_date_str:
            try:
                issue_dt = datetime.fromisoformat(issue_date_str)
                if issue_dt.tzinfo is None:
                    issue_dt = issue_dt.replace(tzinfo=timezone.utc)
                is_recent = issue_dt >= one_year_ago
            except (ValueError, TypeError):
                pass

        # If recently issued AND address wasn't in previous data → new application
        # If recently issued AND address was in previous data → renewal
        # If not recently issued → active (existing)
        if is_recent:
            if addr and addr in prev_addresses:
                rec["category"] = "renewal"
            else:
                rec["category"] = "new_application"
        else:
            # Check if it was a state-approved record (always highlight these)
            if rec.get("source") == "state_approved":
                rec["category"] = "new_application"
            else:
                rec["category"] = "active"

    return records

# ---------------------------------------------------------------------------
# Chart data computation
# ---------------------------------------------------------------------------

def compute_chart_data(records):
    """
    Compute monthly new licenses and delinquencies for current year and prior year.
    Uses issue_date for new licenses, expiration_date as proxy for delinquency timing.
    """
    now = datetime.now(timezone.utc)
    current_year = now.year
    prior_year = current_year - 1
    current_month = now.month

    # Monthly counts
    new_by_month = defaultdict(int)       # {(year, month): count}
    delinq_by_month = defaultdict(int)    # {(year, month): count}

    for rec in records:
        status = (rec.get("status") or "").upper()
        issue_str = rec.get("issue_date")
        exp_str = rec.get("expiration_date")

        # Count new licenses by issue_date
        if issue_str and ("CLOSED" not in status):
            try:
                dt = datetime.fromisoformat(issue_str)
                if dt.year in (current_year, prior_year):
                    new_by_month[(dt.year, dt.month)] += 1
            except (ValueError, TypeError):
                pass

        # Count delinquencies by expiration_date as proxy
        if "DELINQUENT" in status and exp_str:
            try:
                dt = datetime.fromisoformat(exp_str)
                if dt.year in (current_year, prior_year):
                    delinq_by_month[(dt.year, dt.month)] += 1
            except (ValueError, TypeError):
                pass

    # Build monthly arrays for Jan-Dec
    months = list(range(1, 13))
    chart = {
        "current_year": current_year,
        "prior_year": prior_year,
        "current_month": current_month,
        "months": ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        "new_licenses_current": [new_by_month.get((current_year, m), 0) for m in months],
        "new_licenses_prior": [new_by_month.get((prior_year, m), 0) for m in months],
        "delinquencies_current": [delinq_by_month.get((current_year, m), 0) for m in months],
        "delinquencies_prior": [delinq_by_month.get((prior_year, m), 0) for m in months],
    }

    # Compute cumulative arrays
    def cumulative(arr, max_month=12):
        result = []
        total = 0
        for i, v in enumerate(arr):
            if i < max_month:
                total += v
                result.append(total)
            else:
                result.append(None)
        return result

    chart["cumulative_new_current"] = cumulative(chart["new_licenses_current"], current_month)
    chart["cumulative_new_prior"] = cumulative(chart["new_licenses_prior"])
    chart["cumulative_delinq_current"] = cumulative(chart["delinquencies_current"], current_month)
    chart["cumulative_delinq_prior"] = cumulative(chart["delinquencies_prior"])

    return chart

# ---------------------------------------------------------------------------
# Restaurant Tracker cross-reference
# ---------------------------------------------------------------------------

def cross_reference_rt(records):
    """Cross-reference records against the BusinessDen restaurant tracker."""
    print("Fetching restaurant tracker data...")
    try:
        rt_data = fetch_json(RT_URL)
        if not rt_data or "records" not in rt_data:
            print("  Warning: Could not fetch restaurant tracker data")
            return records
    except Exception as e:
        print(f"  Warning: Restaurant tracker fetch failed: {e}")
        return records

    rt_records = rt_data["records"]
    print(f"  Loaded {len(rt_records)} restaurant tracker records")

    # Build address index
    rt_by_addr = {}
    rt_by_coords = []
    for rt in rt_records:
        addr = normalize_addr(rt.get("address", ""))
        if addr:
            rt_by_addr[addr] = rt
        lat = rt.get("lat")
        lng = rt.get("lng")
        if lat and lng:
            rt_by_coords.append((float(lat), float(lng), rt))

    matched = 0
    for rec in records:
        # Try address match first
        addr = normalize_addr(rec.get("address", ""))
        rt = rt_by_addr.get(addr)

        # Fall back to proximity match
        if not rt and rec.get("lat") and rec.get("lng"):
            min_dist = float("inf")
            closest = None
            for rlat, rlng, rrt in rt_by_coords:
                d = haversine(rec["lat"], rec["lng"], rlat, rlng)
                if d < min_dist:
                    min_dist = d
                    closest = rrt
            if min_dist <= 50:
                rt = closest

        if rt:
            rec["rt_match"] = {
                "name": rt.get("name", ""),
                "rating": rt.get("rating"),
                "reviews": rt.get("user_ratings_total"),
                "type": rt.get("primary_type_display", ""),
                "status": rt.get("business_status", ""),
                "first_seen": rt.get("first_seen", ""),
            }
            matched += 1
        else:
            rec["rt_match"] = None

    print(f"  Matched {matched} of {len(records)} records to restaurant tracker")
    return records

# ---------------------------------------------------------------------------
# Diff computation
# ---------------------------------------------------------------------------

def compute_diff(current_records, previous_records):
    """Compute new, removed, and status-changed records."""
    if not previous_records:
        return {
            "new_count": 0, "removed_count": 0, "status_changes_count": 0,
            "new": [], "removed": [], "status_changes": [],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    prev_by_id = {r["id"]: r for r in previous_records}
    curr_by_id = {r["id"]: r for r in current_records}

    new_records = []
    for rid, rec in curr_by_id.items():
        if rid not in prev_by_id:
            new_records.append({
                "id": rid, "name": rec.get("name", ""),
                "address": rec.get("address", ""), "status": rec.get("status", ""),
                "license_type": rec.get("license_type", ""),
                "category": rec.get("category", "")
            })

    removed = []
    for rid, rec in prev_by_id.items():
        if rid not in curr_by_id:
            removed.append({
                "id": rid, "name": rec.get("name", ""),
                "address": rec.get("address", ""), "status": rec.get("status", "")
            })

    status_changes = []
    for rid in curr_by_id:
        if rid in prev_by_id:
            if curr_by_id[rid].get("status") != prev_by_id[rid].get("status"):
                status_changes.append({
                    "id": rid, "name": curr_by_id[rid].get("name", ""),
                    "address": curr_by_id[rid].get("address", ""),
                    "old_status": prev_by_id[rid].get("status", ""),
                    "new_status": curr_by_id[rid].get("status", "")
                })

    return {
        "new_count": len(new_records),
        "removed_count": len(removed),
        "status_changes_count": len(status_changes),
        "new": new_records,
        "removed": removed,
        "status_changes": status_changes,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    now = datetime.now(timezone.utc)
    print(f"=== Liquor License Tracker Scraper ===")
    print(f"Started: {now.isoformat()}")

    all_records = []
    source_counts = {}

    # --- 1. Denver ArcGIS ---
    print("\n1. Fetching Denver ArcGIS licenses (non-special-events)...")
    arcgis_features = fetch_arcgis(
        where="LICENSES NOT LIKE '%SPECIAL%'",
        fields="*"
    )
    print(f"  Raw ArcGIS records: {len(arcgis_features)}")

    denver_records = []
    denver_addresses = set()
    for feat in arcgis_features:
        rec = normalize_arcgis_record(feat)
        if rec:
            denver_records.append(rec)
            addr = normalize_addr(rec["address"])
            if addr:
                denver_addresses.add(addr)

    all_records.extend(denver_records)
    source_counts["denver_arcgis"] = len(denver_records)
    print(f"  Normalized: {len(denver_records)} Denver records")

    # --- 2. State recently approved ---
    print("\n2. Fetching state recently approved licenses...")
    city_filter = " OR ".join([f"city='{c}'" for c in METRO_CITIES])
    type_filter = " OR ".join([f"license_type='{t}'" for t in NEWSWORTHY_TYPES])
    where = f"({city_filter}) AND ({type_filter})"

    approved_raw = fetch_socrata(APPROVED_DATASET, where)
    print(f"  Raw approved records: {len(approved_raw)}")

    approved_records = [normalize_socrata_record(r, "state_approved") for r in approved_raw]
    all_records.extend(approved_records)
    source_counts["state_approved"] = len(approved_records)
    print(f"  Normalized: {len(approved_records)} approved records")

    # --- 3. State active licenses (dedup against Denver) ---
    print("\n3. Fetching state active licenses...")
    active_raw = fetch_socrata(ACTIVE_DATASET, where)
    print(f"  Raw active records: {len(active_raw)}")

    active_records = []
    for raw in active_raw:
        rec = normalize_socrata_record(raw, "state_active")
        addr = normalize_addr(rec["address"])
        if addr not in denver_addresses:
            active_records.append(rec)

    all_records.extend(active_records)
    source_counts["state_active"] = len(active_records)
    print(f"  After dedup: {len(active_records)} state active records")

    print(f"\nTotal records: {len(all_records)}")

    # --- 4. Cross-reference with restaurant tracker ---
    print("\n4. Cross-referencing with restaurant tracker...")
    all_records = cross_reference_rt(all_records)

    # --- 5. Load previous data and classify ---
    print("\n5. Classifying records (new vs renewal)...")
    previous_records = []
    if os.path.exists(PREVIOUS_FILE):
        try:
            with open(PREVIOUS_FILE, "r") as f:
                prev_data = json.load(f)
                previous_records = prev_data.get("records", [])
            print(f"  Loaded {len(previous_records)} previous records")
        except Exception as e:
            print(f"  Warning: Could not load previous data: {e}")

    all_records = classify_records(all_records, previous_records)

    # Count categories
    cat_counts = defaultdict(int)
    for rec in all_records:
        cat_counts[rec.get("category", "unknown")] += 1
    print(f"  Categories: {dict(cat_counts)}")

    # --- 6. Compute diff ---
    print("\n6. Computing diff...")
    diff = compute_diff(all_records, previous_records)
    print(f"  New: {diff['new_count']}, Removed: {diff['removed_count']}, "
          f"Status changes: {diff['status_changes_count']}")

    # --- 7. Compute summary stats ---
    print("\n7. Computing summary stats...")
    summary = {
        "by_status": defaultdict(int),
        "by_type": defaultdict(int),
        "by_neighborhood": defaultdict(int),
        "by_city": defaultdict(int),
        "by_category": defaultdict(int),
    }
    for rec in all_records:
        summary["by_status"][rec.get("status", "Unknown")] += 1
        summary["by_type"][rec.get("license_type", "Unknown")] += 1
        if rec.get("neighborhood"):
            summary["by_neighborhood"][rec["neighborhood"]] += 1
        summary["by_city"][rec.get("city", "Unknown")] += 1
        summary["by_category"][rec.get("category", "unknown")] += 1

    # Convert defaultdicts to regular dicts for JSON
    summary = {k: dict(v) for k, v in summary.items()}

    # --- 8. Compute chart data ---
    print("\n8. Computing chart data...")
    chart_data = compute_chart_data(all_records)
    print(f"  Current year: {chart_data['current_year']}, "
          f"Prior year: {chart_data['prior_year']}")

    # --- 9. Write output ---
    print("\n9. Writing output files...")
    output = {
        "metadata": {
            "generated": now.isoformat(),
            "total_records": len(all_records),
            "sources": source_counts,
        },
        "summary": summary,
        "chart": chart_data,
        "diff": diff,
        "records": all_records,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    print(f"  Wrote {OUTPUT_FILE} ({os.path.getsize(OUTPUT_FILE) / 1024 / 1024:.1f} MB)")

    # Save current as previous
    os.makedirs("data", exist_ok=True)
    with open(PREVIOUS_FILE, "w") as f:
        json.dump({"records": all_records}, f, separators=(",", ":"))
    print(f"  Saved previous snapshot to {PREVIOUS_FILE}")

    print(f"\n=== Done ===")


if __name__ == "__main__":
    main()
