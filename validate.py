import csv
import requests
import time


API_TOKEN             = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjYzMDcyODE5OCwiYWFpIjoxMSwidWlkIjoxMDA3Nzk0MDQsImlhZCI6IjIwMjYtMDMtMDlUMTY6MDg6NDYuMDAwWiIsInBlciI6Im1lOndyaXRlIiwiYWN0aWQiOjM0MTQzMDM0LCJyZ24iOiJ1c2UxIn0.deIWTxWDdm9Nu41xqAGHkb1zna2VAFwE75Qfrrc0P68"
ENGAGEMENTS_BOARD_ID  = 18403140685
DELIVERABLES_BOARD_ID = 18403219860
CSV_FILE              = "nexus_smartsheet_export.csv"

API_URL = "https://api.monday.com/v2"
HEADERS = {
    "Authorization": API_TOKEN,
    "Content-Type":  "application/json",
    "API-Version":   "2024-01"
}

# Expected normalised values after migration
ENGAGEMENT_STATUS_MAP = {
    "in progress": "Active",
    "active":      "Active",
    "complete":    "Complete",
    "done":        "Complete",
    "on hold":     "On Hold",
    "not started": "Not Started",
}
DELIVERABLE_STATUS_MAP = {
    "to do":         "To Do",
    "not started":   "To Do",
    "in progress":   "In Progress",
    "working on it": "In Progress",
    "in review":     "In Review",
    "done":          "Done",
}
PRIORITY_MAP = {
    "high":   "High",
    "medium": "Medium",
    "low":    "Low",
}

# ============================================================
# HELPERS
# ============================================================
def run_query(query, variables=None, retries=5):
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    for attempt in range(retries):
        response = requests.post(API_URL, headers=HEADERS, json=payload)
        if response.status_code == 429:
            retry_after = response.headers.get("retry-after")
            wait = int(retry_after) if retry_after else 60
            print(f"  Rate limited, waiting {wait}s (attempt {attempt + 1}/{retries})...")
            time.sleep(wait + 5)  # add 5s buffer on top of what monday specifies
            continue
        response.raise_for_status()
        data = response.json()
        if "errors" in data:
            raise Exception(f"GraphQL error: {data['errors']}")
        return data
    raise Exception("Max retries exceeded due to rate limiting.")

def format_date(date_str):
    """Convert M/D/YYYY or MM/DD/YYYY to YYYY-MM-DD."""
    if date_str is None or not date_str:
        return ""
    parts = str(date_str).strip().split("/")
    if len(parts) == 3:
        return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
    return date_str

# ============================================================
# STEP 1 — Load & parse the source CSV
# ============================================================
def load_csv(filepath):
    csv_engagements  = {}
    csv_deliverables = {}

    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            def _s(key):
                v = row.get(key)
                return (v or "").strip() if v is not None else ""

            eng_id = _s("engagement_id")
            del_id = _s("deliverable_id")

            if eng_id not in csv_engagements:
                csv_engagements[eng_id] = {
                    "engagement_id":   eng_id,
                    "engagement_name": _s("engagement_name"),
                    "client":          _s("client"),
                    "engagement_lead": _s("engagement_lead"),
                    "start_date":      format_date(_s("engagement_start")),
                    "end_date":        format_date(_s("engagement_end")),
                    "budget":          _s("budget"),
                    "raw_status":      _s("engagement_status"),
                    "expected_status": ENGAGEMENT_STATUS_MAP.get(
                                           _s("engagement_status").lower(), "Not Started"
                                       ),
                }

            csv_deliverables[del_id] = {
                "deliverable_id":    del_id,
                "deliverable_name":  _s("deliverable_name"),
                "engagement_id":     eng_id,
                "assignee":          _s("assignee"),
                "due_date":          format_date(_s("due_date")),
                "raw_priority":      _s("priority"),
                "expected_priority": PRIORITY_MAP.get(_s("priority").lower(), "Medium"),
                "raw_status":        _s("deliverable_status"),
                "expected_status":   DELIVERABLE_STATUS_MAP.get(
                                         _s("deliverable_status").lower(), "To Do"
                                     ),
                "hours_estimated":   _s("hours_estimated"),
            }

    print(f"CSV loaded — {len(csv_engagements)} engagements, {len(csv_deliverables)} deliverables")
    return csv_engagements, csv_deliverables

# ============================================================
# STEP 2 — Fetch migrated data from monday.com
# ============================================================
def fetch_monday_engagements():
    query = """
    query ($board_id: ID!) {
        boards(ids: [$board_id]) {
            items_page(limit: 50) {
                items {
                    id
                    name
                    column_values(ids: [
                        "text_mm19s2sx",
                        "text_mm1ag0m7",
                        "text_mm19sqy0",
                        "date_mm19c9dv",
                        "date_mm19zagn",
                        "numeric_mm195cgs",
                        "color_mm19k0y2"
                    ]) {
                        id
                        text
                    }
                }
            }
        }
    }
    """
    result = run_query(query, {"board_id": str(ENGAGEMENTS_BOARD_ID)})
    monday_engagements = {}
    for item in result["data"]["boards"][0]["items_page"]["items"]:
        cols   = {c["id"]: (c.get("text") or "") for c in item["column_values"]}
        eng_id = (cols.get("text_mm19s2sx") or "").strip()
        if eng_id:
            monday_engagements[eng_id] = {
                "monday_id":       item["id"],
                "engagement_name": item["name"],
                "client":          cols.get("text_mm1ag0m7", ""),
                "lead":            cols.get("text_mm19sqy0", ""),
                "start_date":      cols.get("date_mm19c9dv", ""),
                "end_date":        cols.get("date_mm19zagn", ""),
                "budget":          cols.get("numeric_mm195cgs", ""),
                "status":          cols.get("color_mm19k0y2", ""),
            }
    print(f"monday.com — {len(monday_engagements)} engagements fetched")
    return monday_engagements


def fetch_monday_deliverables():
    query = """
    query ($board_id: ID!) {
        boards(ids: [$board_id]) {
            items_page(limit: 50) {
                items {
                    id
                    name
                    column_values(ids: [
                        "text_mm193bmr",
                        "board_relation_mm19qww2",
                        "text_mm1985c8",
                        "date_mm19hgyc",
                        "color_mm19j92m",
                        "color_mm19wzqg",
                        "numeric_mm19qrc6"
                    ]) {
                        id
                        text
                        value
                        ... on BoardRelationValue {
                            linked_items {
                                id
                                name
                            }
                        }
                    }
                }
            }
        }
    }
    """
    result = run_query(query, {"board_id": str(DELIVERABLES_BOARD_ID)})
    monday_deliverables = {}
    for item in result["data"]["boards"][0]["items_page"]["items"]:
        cols     = {c["id"]: (c.get("text") or "") for c in item["column_values"]}
        raw_cols = {c["id"]: c for c in item["column_values"]}

        # board_relation returns null for text and value — check linked_items instead
        linked_items = raw_cols.get("board_relation_mm19qww2", {}).get("linked_items") or []
        has_link     = len(linked_items) > 0

        del_id = (cols.get("text_mm193bmr") or "").strip()
        if del_id:
            monday_deliverables[del_id] = {
                "monday_id":        item["id"],
                "deliverable_name": item["name"],
                "engagement_link":  linked_items[0]["name"] if has_link else "",
                "assignee":         cols.get("text_mm1985c8", ""),
                "due_date":         cols.get("date_mm19hgyc", ""),
                "priority":         cols.get("color_mm19j92m", ""),
                "status":           cols.get("color_mm19wzqg", ""),
                "hours":            cols.get("numeric_mm19qrc6", ""),
            }
    print(f"monday.com — {len(monday_deliverables)} deliverables fetched")
    return monday_deliverables

# ============================================================
# CHECKS
# ============================================================
def check_record_counts(csv_eng, csv_del, mon_eng, mon_del):
    print(f"\n{'='*50}")
    print("CHECK 1: RECORD COUNTS")
    print(f"{'='*50}")
    eng_match = "PASS" if len(csv_eng) == len(mon_eng) else "FAIL"
    del_match = "PASS" if len(csv_del) == len(mon_del) else "FAIL"
    print(f"  Engagements  — CSV: {len(csv_eng):>3}  |  monday.com: {len(mon_eng):>3}  |  {eng_match}")
    print(f"  Deliverables — CSV: {len(csv_del):>3}  |  monday.com: {len(mon_del):>3}  |  {del_match}")
    return len(csv_eng) == len(mon_eng), len(csv_del) == len(mon_del)


def check_missing_records(csv_eng, csv_del, mon_eng, mon_del):
    print(f"\n{'='*50}")
    print("CHECK 2: MISSING RECORDS")
    print(f"{'='*50}")
    missing_eng = [eid for eid in csv_eng if eid not in mon_eng]
    missing_del = [did for did in csv_del if did not in mon_del]

    if missing_eng:
        print(f"  FAIL — {len(missing_eng)} engagement(s) missing from monday.com:")
        for eid in missing_eng:
            print(f"    - {eid}: {csv_eng[eid]['engagement_name']}")
    else:
        print("  PASS — All engagements present in monday.com")

    if missing_del:
        print(f"  FAIL — {len(missing_del)} deliverable(s) missing from monday.com:")
        for did in missing_del:
            print(f"    - {did}: {csv_del[did]['deliverable_name']}")
    else:
        print("  PASS — All deliverables present in monday.com")

    return len(missing_eng) == 0, len(missing_del) == 0


def check_orphaned_deliverables(mon_del):
    print(f"\n{'='*50}")
    print("CHECK 3: ORPHANED DELIVERABLES")
    print("(Deliverables with no engagement link)")
    print(f"{'='*50}")
    orphans = [
        (did, d["deliverable_name"])
        for did, d in mon_del.items()
        if not (d.get("engagement_link") or "").strip()
    ]
    if orphans:
        print(f"  FAIL — {len(orphans)} orphaned deliverable(s) found:")
        for did, name in orphans:
            print(f"    - {did}: {name}")
    else:
        print("  PASS — All deliverables are linked to an engagement")
    return len(orphans) == 0


def check_status_normalisation(csv_eng, csv_del, mon_eng, mon_del):
    print(f"\n{'='*50}")
    print("CHECK 4: STATUS NORMALISATION")
    print(f"{'='*50}")

    eng_issues = []
    for eid, e in csv_eng.items():
        if eid not in mon_eng:
            continue
        if e["expected_status"] != mon_eng[eid]["status"]:
            eng_issues.append({
                "id": eid, "name": e["engagement_name"],
                "raw": e["raw_status"], "expected": e["expected_status"],
                "actual": mon_eng[eid]["status"],
            })

    del_status_issues   = []
    del_priority_issues = []
    for did, d in csv_del.items():
        if did not in mon_del:
            continue
        mon = mon_del[did]
        if d["expected_status"] != mon["status"]:
            del_status_issues.append({
                "id": did, "name": d["deliverable_name"],
                "raw": d["raw_status"], "expected": d["expected_status"],
                "actual": mon["status"],
            })
        if d["expected_priority"] != mon["priority"]:
            del_priority_issues.append({
                "id": did, "name": d["deliverable_name"],
                "raw": d["raw_priority"], "expected": d["expected_priority"],
                "actual": mon["priority"],
            })

    if eng_issues:
        print(f"  FAIL — {len(eng_issues)} engagement status mismatch(es):")
        for i in eng_issues:
            print(f"    - {i['id']} ({i['name']}): raw '{i['raw']}' → expected '{i['expected']}' | got '{i['actual']}'")
    else:
        print("  PASS — All engagement statuses correctly normalised")

    if del_status_issues:
        print(f"  FAIL — {len(del_status_issues)} deliverable status mismatch(es):")
        for i in del_status_issues:
            print(f"    - {i['id']} ({i['name']}): raw '{i['raw']}' → expected '{i['expected']}' | got '{i['actual']}'")
    else:
        print("  PASS — All deliverable statuses correctly normalised")

    if del_priority_issues:
        print(f"  FAIL — {len(del_priority_issues)} deliverable priority mismatch(es):")
        for i in del_priority_issues:
            print(f"    - {i['id']} ({i['name']}): raw '{i['raw']}' → expected '{i['expected']}' | got '{i['actual']}'")
    else:
        print("  PASS — All deliverable priorities correctly normalised")

    return len(eng_issues) == 0, len(del_status_issues) == 0, len(del_priority_issues) == 0


def check_data_quality(mon_eng, mon_del):
    print(f"\n{'='*50}")
    print("CHECK 5: DATA QUALITY — MISSING FIELDS")
    print(f"{'='*50}")

    eng_issues = []
    for eid, e in mon_eng.items():
        issues = []
        if not (e.get("client") or "").strip():          issues.append("missing client")
        if not (e.get("lead") or "").strip():            issues.append("missing engagement lead")
        if not (e.get("start_date") or "").strip():      issues.append("missing start date")
        if not (e.get("end_date") or "").strip():       issues.append("missing end date")
        b = (e.get("budget") or "").strip()
        if not b or b == "0":
            issues.append("missing/zero budget")
        if issues:
            eng_issues.append((eid, e["engagement_name"], issues))

    del_issues = []
    for did, d in mon_del.items():
        issues = []
        if not (d.get("assignee") or "").strip():        issues.append("missing assignee")
        if not (d.get("due_date") or "").strip():        issues.append("missing due date")
        h = (d.get("hours") or "").strip()
        if not h or h == "0":
            issues.append("missing/zero hours")
        if issues:
            del_issues.append((did, d["deliverable_name"], issues))

    if eng_issues:
        print(f"  WARNING — {len(eng_issues)} engagement(s) with data quality issues:")
        for eid, name, issues in eng_issues:
            print(f"    - {eid} ({name}): {', '.join(issues)}")
    else:
        print("  PASS — All engagements have complete field data")

    if del_issues:
        print(f"  WARNING — {len(del_issues)} deliverable(s) with data quality issues:")
        for did, name, issues in del_issues:
            print(f"    - {did} ({name}): {', '.join(issues)}")
    else:
        print("  PASS — All deliverables have complete field data")

    return len(eng_issues) == 0, len(del_issues) == 0

# ============================================================
# SUMMARY
# ============================================================
def print_summary(results):
    print(f"\n{'='*50}")
    print("VALIDATION SUMMARY")
    print(f"{'='*50}")

    checks = [
        ("Record Counts — Engagements",          results["eng_count"]),
        ("Record Counts — Deliverables",         results["del_count"]),
        ("Missing Records — Engagements",        results["eng_missing"]),
        ("Missing Records — Deliverables",       results["del_missing"]),
        ("Orphaned Deliverables",                results["orphans"]),
        ("Status Normalisation — Engagements",   results["eng_status"]),
        ("Status Normalisation — Deliverables",  results["del_status"]),
        ("Priority Normalisation",               results["del_priority"]),
        ("Data Quality — Engagements",           results["eng_quality"]),
        ("Data Quality — Deliverables",          results["del_quality"]),
    ]

    passed = sum(1 for _, r in checks if r)
    total  = len(checks)

    for name, result in checks:
        print(f"  [{'PASS' if result else 'FAIL'}]  {name}")

    print()
    print(f"  Result: {passed}/{total} checks passed")
    if passed == total:
        print("  Migration validated successfully.")
    else:
        print(f"  {total - passed} check(s) require attention — see details above.")

# ============================================================
# MAIN
# ============================================================
def main():
    print("Nexus Consulting Group — Migration Validation")
    print("=" * 50)

    # Load source data
    csv_eng, csv_del = load_csv(CSV_FILE)

    # Fetch monday.com data
    time.sleep(60)
    mon_eng = fetch_monday_engagements()
    time.sleep(60)
    mon_del = fetch_monday_deliverables()

    # Run checks
    eng_count,  del_count   = check_record_counts(csv_eng, csv_del, mon_eng, mon_del)
    eng_missing, del_missing = check_missing_records(csv_eng, csv_del, mon_eng, mon_del)
    orphans                  = check_orphaned_deliverables(mon_del)
    eng_status, del_status, del_priority = check_status_normalisation(csv_eng, csv_del, mon_eng, mon_del)
    eng_quality, del_quality = check_data_quality(mon_eng, mon_del)

    # Print summary
    print_summary({
        "eng_count":    eng_count,
        "del_count":    del_count,
        "eng_missing":  eng_missing,
        "del_missing":  del_missing,
        "orphans":      orphans,
        "eng_status":   eng_status,
        "del_status":   del_status,
        "del_priority": del_priority,
        "eng_quality":  eng_quality,
        "del_quality":  del_quality,
    })

if __name__ == "__main__":
    main()