#!/usr/bin/env python3
"""
Denver Metro Liquor License Tracker — Scraper
Pulls from 3 public data sources, cross-references against BusinessDen restaurant tracker,
tracks first_seen dates for genuinely new record detection, and accumulates daily chart data.
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
CHART_HISTORY_FILE = "data/chart-history.json"

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def fetch_json(url, retries=3, timeout=30):
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
# Normalization
# ---------------------------------------------------------------------------

def ts_to_iso(ms_timestamp):
    if ms_timestamp is None:
        return None
    try:
        if isinstance(ms_timestamp, str):
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
    if not addr:
        return ""
    addr = addr.upper().strip()
    addr = addr.split(",")[0].strip()
    addr = re.sub(r"\s+(UNIT|STE|SUITE|APT|#)\s*\S*", "", addr)
    addr = re.sub(r"\s+", " ", addr)
    return addr


def haversine(lat1, lng1, lat2, lng2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def normalize_arcgis_record(feat):
    attrs = feat.get("attributes", {})
    geom = feat.get("geometry", {})
    lat = geom.get("y")
    lng = geom.get("x")
    if lat and lng:
        if not (36 <= lat <= 42 and -110 <= lng <= -100):
            return None
    bfn = attrs.get("BFN", "")
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
        "issue_date": ts_to_iso(attrs.get("ISSUE_DATE")),
        "expiration_date": ts_to_iso(attrs.get("END_DATE")),
        "neighborhood": (attrs.get("NEIGHBORHOOD") or "").strip(),
        "council_district": str(attrs.get("COUNCIL_DIST") or ""),
        "hearing_date": ts_to_iso(attrs.get("HEARING_DATE")),
        "hearing_time": (attrs.get("HEARING_TIME") or "").strip(),
        "hearing_status": (attrs.get("HEARING_STATUS") or "").strip(),
    }


def normalize_socrata_record(rec, source):
    lat = None
    lng = None
    location = rec.get("location") or rec.get("location_1")
    if location:
        if isinstance(location, dict):
            lat = location.get("latitude")
            lng = location.get("longitude")
            if lat: lat = float(lat)
            if lng: lng = float(lng)
        elif isinstance(location, str):
            m = re.search(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", location)
            if m:
                lng, lat = float(m.group(1)), float(m.group(2))

    name = (rec.get("licensee_name") or "").strip()
    dba = (rec.get("doing_business_as") or "").strip()
    lic_num = (rec.get("license_number") or "").strip()
    status = "Approved" if source == "state_approved" else "Active"

    return {
        "id": f"{source}-{lic_num or name}",
        "source": source,
        "name": name,
        "dba": dba,
        "license_type": (rec.get("license_type") or "").strip(),
        "license_number": lic_num,
        "status": status,
        "address": (rec.get("street_address") or "").strip(),
        "city": (rec.get("city") or "").strip(),
        "state": (rec.get("state") or "CO").strip(),
        "zip": (rec.get("zip") or rec.get("zip_code") or "").strip(),
        "lat": lat,
        "lng": lng,
        "issue_date": ts_to_iso(rec.get("issue_date")),
        "expiration_date": ts_to_iso(rec.get("expiration") or rec.get("expiration_date")),
        "neighborhood": "",
        "council_district": "",
        "hearing_date": None,
        "hearing_time": "",
        "hearing_status": "",
    }

# ---------------------------------------------------------------------------
# Classification + first_seen tracking
# ---------------------------------------------------------------------------

def classify_and_track(records, previous_records):
    """
    Simple classification by current status.
    Assigns first_seen dates from previous data or today for new records.
    Suppresses delinquencies where an active license exists at the same address.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Build lookup of first_seen from previous data
    prev_first_seen = {}
    if previous_records:
        for rec in previous_records:
            prev_first_seen[rec["id"]] = rec.get("first_seen", today)

    is_first_run = len(prev_first_seen) == 0

    # On first run, use "baseline" marker instead of today's date
    # so the frontend can distinguish baseline records from genuinely new ones
    default_first_seen = "baseline" if is_first_run else today

    # Classify by status and assign first_seen
    for rec in records:
        status = (rec.get("status") or "").upper()

        if "CLOSED" in status or "EXPIRED" in status or "DENIED" in status or "REVOKED" in status:
            rec["category"] = "closed"
        elif "DELINQUENT" in status:
            rec["category"] = "delinquent"
        elif "PENDING" in status:
            rec["category"] = "pending"
        else:
            rec["category"] = "active"

        # Assign first_seen: inherited from previous data, or baseline/today for new
        if rec["id"] in prev_first_seen:
            rec["first_seen"] = prev_first_seen[rec["id"]]
        else:
            rec["first_seen"] = default_first_seen

    # Suppress resolved delinquencies
    active_addrs = set()
    for rec in records:
        status = (rec.get("status") or "").upper()
        if "ACTIVE" in status and "DELINQUENT" not in status:
            addr = normalize_addr(rec.get("address", ""))
            if addr:
                active_addrs.add(addr)

    resolved = 0
    for rec in records:
        if rec.get("category") == "delinquent":
            addr = normalize_addr(rec.get("address", ""))
            if addr and addr in active_addrs:
                rec["category"] = "resolved_delinquent"
                resolved += 1

    if resolved:
        print(f"  Suppressed {resolved} delinquencies with active licenses at same address")

    # Count genuinely new records (ID not in previous data, not first run)
    new_today = 0
    if not is_first_run:
        for rec in records:
            if rec["id"] not in prev_first_seen and rec["category"] not in ("closed", "resolved_delinquent"):
                new_today += 1

    print(f"  First run: {is_first_run}")
    print(f"  New records today: {new_today}")

    return records, new_today, is_first_run

# ---------------------------------------------------------------------------
# Chart history
# ---------------------------------------------------------------------------

def update_chart_history(new_today, delinquent_count, is_first_run):
    """
    Append today's genuinely-new-record count to chart-history.json.
    On first run, records 0 (baseline — no false positives).
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    history = {"daily": {}}
    if os.path.exists(CHART_HISTORY_FILE):
        try:
            with open(CHART_HISTORY_FILE, "r") as f:
                history = json.load(f)
        except Exception:
            pass
    if "daily" not in history:
        history["daily"] = {}

    # On first run, record 0 to establish baseline
    count = 0 if is_first_run else new_today

    history["daily"][today] = {
        "new": count,
        "delinquent": delinquent_count,
    }

    os.makedirs("data", exist_ok=True)
    with open(CHART_HISTORY_FILE, "w") as f:
        json.dump(history, f, separators=(",", ":"))

    print(f"  Chart history: {today} → {count} new, {delinquent_count} delinquent")
    return history

# ---------------------------------------------------------------------------
# Restaurant Tracker cross-reference
# ---------------------------------------------------------------------------

def cross_reference_rt(records):
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
        addr = normalize_addr(rec.get("address", ""))
        rt = rt_by_addr.get(addr)
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
# Diff
# ---------------------------------------------------------------------------

def compute_diff(current_records, previous_records):
    if not previous_records:
        return {
            "new_count": 0, "removed_count": 0, "status_changes_count": 0,
            "new": [], "removed": [], "status_changes": [],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    prev_by_id = {r["id"]: r for r in previous_records}
    curr_by_id = {r["id"]: r for r in current_records}

    new_records = [
        {"id": rid, "name": rec.get("name", ""), "address": rec.get("address", ""),
         "status": rec.get("status", ""), "license_type": rec.get("license_type", ""),
         "category": rec.get("category", "")}
        for rid, rec in curr_by_id.items() if rid not in prev_by_id
    ]
    removed = [
        {"id": rid, "name": rec.get("name", ""), "address": rec.get("address", ""),
         "status": rec.get("status", "")}
        for rid, rec in prev_by_id.items() if rid not in curr_by_id
    ]
    status_changes = [
        {"id": rid, "name": curr_by_id[rid].get("name", ""),
         "address": curr_by_id[rid].get("address", ""),
         "old_status": prev_by_id[rid].get("status", ""),
         "new_status": curr_by_id[rid].get("status", "")}
        for rid in curr_by_id if rid in prev_by_id
        and curr_by_id[rid].get("status") != prev_by_id[rid].get("status")
    ]

    return {
        "new_count": len(new_records), "removed_count": len(removed),
        "status_changes_count": len(status_changes),
        "new": new_records, "removed": removed, "status_changes": status_changes,
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

    # 1. Denver ArcGIS (non-special-events)
    print("\n1. Fetching Denver ArcGIS licenses (non-special-events)...")
    arcgis_features = fetch_arcgis(where="LICENSES NOT LIKE '%SPECIAL%'", fields="*")
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

    # 2. State recently approved (metro cities only)
    print("\n2. Fetching state recently approved licenses...")
    city_filter = " OR ".join([f"city='{c}'" for c in METRO_CITIES])
    where_cities = f"({city_filter})"

    approved_raw = fetch_socrata(APPROVED_DATASET, where_cities)
    print(f"  Raw approved records: {len(approved_raw)}")
    approved_records = [normalize_socrata_record(r, "state_approved") for r in approved_raw]
    all_records.extend(approved_records)
    source_counts["state_approved"] = len(approved_records)
    print(f"  Normalized: {len(approved_records)} approved records")

    # 3. State active (metro cities, dedup against Denver)
    print("\n3. Fetching state active licenses...")
    active_raw = fetch_socrata(ACTIVE_DATASET, where_cities)
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

    # 4. Restaurant tracker cross-reference
    print("\n4. Cross-referencing with restaurant tracker...")
    all_records = cross_reference_rt(all_records)

    # 5. Load previous data, classify, and track first_seen
    print("\n5. Classifying and tracking first_seen...")
    previous_records = []
    if os.path.exists(PREVIOUS_FILE):
        try:
            with open(PREVIOUS_FILE, "r") as f:
                previous_records = json.load(f).get("records", [])
            print(f"  Loaded {len(previous_records)} previous records")
        except Exception as e:
            print(f"  Warning: Could not load previous data: {e}")

    all_records, new_today, is_first_run = classify_and_track(all_records, previous_records)

    cat_counts = defaultdict(int)
    for rec in all_records:
        cat_counts[rec.get("category", "unknown")] += 1
    print(f"  Categories: {dict(cat_counts)}")

    # 6. Diff
    print("\n6. Computing diff...")
    diff = compute_diff(all_records, previous_records)
    print(f"  New: {diff['new_count']}, Removed: {diff['removed_count']}, "
          f"Status changes: {diff['status_changes_count']}")

    # 7. Summary stats
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
    summary = {k: dict(v) for k, v in summary.items()}

    # 8. Update chart history
    print("\n8. Updating chart history...")
    delinq_count = cat_counts.get("delinquent", 0)
    chart_history = update_chart_history(new_today, delinq_count, is_first_run)

    # 9. Write output
    print("\n9. Writing output files...")
    output = {
        "metadata": {
            "generated": now.isoformat(),
            "total_records": len(all_records),
            "sources": source_counts,
            "is_first_run": is_first_run,
        },
        "summary": summary,
        "chart_history": chart_history,
        "diff": diff,
        "records": all_records,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    print(f"  Wrote {OUTPUT_FILE} ({os.path.getsize(OUTPUT_FILE) / 1024 / 1024:.1f} MB)")

    os.makedirs("data", exist_ok=True)
    with open(PREVIOUS_FILE, "w") as f:
        json.dump({"records": all_records}, f, separators=(",", ":"))
    print(f"  Saved previous snapshot to {PREVIOUS_FILE}")

    print(f"\n=== Done ===")


if __name__ == "__main__":
    main()
