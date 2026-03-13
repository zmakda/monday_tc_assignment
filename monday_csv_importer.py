import csv
import requests
import json
import time
from datetime import datetime

# ============================================================
# CONFIGURATION — fill in your API token before running
# ============================================================
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjYzMDcyODE5OCwiYWFpIjoxMSwidWlkIjoxMDA3Nzk0MDQsImlhZCI6IjIwMjYtMDMtMDlUMTY6MDg6NDYuMDAwWiIsInBlciI6Im1lOndyaXRlIiwiYWN0aWQiOjM0MTQzMDM0LCJyZ24iOiJ1c2UxIn0.deIWTxWDdm9Nu41xqAGHkb1zna2VAFwE75Qfrrc0P68"
ENGAGEMENTS_BOARD_ID = 18403140685
DELIVERABLES_BOARD_ID = 18403219860
CSV_FILE = "nexus_smartsheet_export.csv"

API_URL = "https://api.monday.com/v2"
HEADERS = {
    "Authorization": API_TOKEN,
    "Content-Type": "application/json",
    "API-Version": "2024-01"
}

# ============================================================
# COLUMN IDs — Engagements Board
# ============================================================
ENG_COL = {
    "engagement_id":  "text_mm19s2sx",
    "client":         "text_mm1ag0m7",
    "lead":           "text_mm19sqy0",
    "start_date":     "date_mm19c9dv",
    "end_date":       "date_mm19zagn",
    "budget":         "numeric_mm195cgs",
    "status":         "color_mm19k0y2",
}

# ============================================================
# COLUMN IDs — Deliverables Board
# ============================================================
DEL_COL = {
    "deliverable_id": "text_mm193bmr",
    "engagement_link":"board_relation_mm19qww2",
    "assignee":       "text_mm1985c8",
    "due_date":       "date_mm19hgyc",
    "priority":       "color_mm19j92m",
    "status":         "color_mm19wzqg",
    "hours":          "numeric_mm19qrc6",
}

# ============================================================
# STATUS MAPPINGS — normalise inconsistent source values
# ============================================================
ENGAGEMENT_STATUS_MAP = {
    "in progress": "Active",
    "active":      "Active",
    "complete":    "Complete",
    "done":        "Complete",
    "on hold":     "On Hold",
    "not started": "Not Started",
}

DELIVERABLE_STATUS_MAP = {
    "to do":        "To Do",
    "not started":  "To Do",
    "in progress":  "In Progress",
    "working on it":"In Progress",
    "in review":    "In Review",
    "done":         "Done",
}

PRIORITY_MAP = {
    "high":   "High",
    "medium": "Medium",
    "low":    "Low",
}

# ============================================================
# HELPERS
# ============================================================
def run_query(query: str, variables: dict = None):
    """Execute a GraphQL query against the monday.com API."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    response = requests.post(API_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    data = response.json()
    if "errors" in data:
        err_msg = json.dumps(data["errors"], indent=2)
        print(f"\n--- Full GraphQL response ---\n{json.dumps(data, indent=2)}\n---")
        raise Exception(f"GraphQL error:\n{err_msg}")
    return data

def format_date(date_str: str) -> str:
    """Convert MM/DD/YYYY to YYYY-MM-DD for the monday.com API."""
    if not date_str:
        return ""
    parts = date_str.strip().split("/")
    if len(parts) == 3:
        return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
    return date_str

def create_item(board_id: int, item_name: str, column_values: dict) -> str:
    """Create a single item on a board and return its ID."""
    query = """
        mutation ($board_id: ID!, $item_name: String!, $column_values: JSON!) {
            create_item(
                board_id: $board_id,
                item_name: $item_name,
                column_values: $column_values
            ) {
                id
            }
        }
    """
    variables = {
        "board_id": str(board_id),
        "item_name": item_name,
        "column_values": json.dumps(column_values)
    }
    result = run_query(query, variables)
    return result["data"]["create_item"]["id"]

# ============================================================
# DUPLICATE CHECKS — query existing items before creating
# ============================================================
def get_existing_engagement_ids() -> dict:
    """
    Query the Engagements board and return a mapping of
    engagement_id (e.g. ENG-001) -> monday item ID.
    """
    query = """
        query ($board_id: ID!) {
            boards(ids: [$board_id]) {
                items_page(limit: 500) {
                    items {
                        id
                        column_values(ids: ["text_mm19s2sx"]) {
                            text
                        }
                    }
                }
            }
        }
    """
    result = run_query(query, {"board_id": str(ENGAGEMENTS_BOARD_ID)})
    existing = {}
    items = result["data"]["boards"][0]["items_page"]["items"]
    for item in items:
        eng_id = item["column_values"][0]["text"]
        if eng_id:
            existing[eng_id] = item["id"]
    return existing


def get_existing_deliverable_ids() -> set:
    """
    Query the Deliverables board and return a set of
    deliverable_ids (e.g. DEL-001) that already exist.
    """
    query = """
        query ($board_id: ID!) {
            boards(ids: [$board_id]) {
                items_page(limit: 500) {
                    items {
                        id
                        column_values(ids: ["text_mm193bmr"]) {
                            text
                        }
                    }
                }
            }
        }
    """
    result = run_query(query, {"board_id": str(DELIVERABLES_BOARD_ID)})
    existing = set()
    items = result["data"]["boards"][0]["items_page"]["items"]
    for item in items:
        del_id = item["column_values"][0]["text"]
        if del_id:
            existing.add(del_id)
    return existing


# ============================================================
# STEP 1 — Parse & deduplicate CSV
# ============================================================
def load_csv(filepath: str):
    engagements = {}   # engagement_id -> dict
    deliverables = []  # list of dicts

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            eng_id = row["engagement_id"].strip()

            # Deduplicate engagements — first occurrence wins
            if eng_id not in engagements:
                engagements[eng_id] = {
                    "engagement_id":     eng_id,
                    "engagement_name":   row["engagement_name"].strip(),
                    "client":            row["client"].strip(),
                    "engagement_lead":   row["engagement_lead"].strip(),
                    "engagement_start":  row["engagement_start"].strip(),
                    "engagement_end":    row["engagement_end"].strip(),
                    "budget":            row["budget"].strip(),
                    "engagement_status": row["engagement_status"].strip(),
                }

            deliverables.append({
                "engagement_id":      eng_id,
                "deliverable_id":     row["deliverable_id"].strip(),
                "deliverable_name":   row["deliverable_name"].strip(),
                "assignee":           row["assignee"].strip(),
                "due_date":           row["due_date"].strip(),
                "priority":           row["priority"].strip(),
                "deliverable_status": row["deliverable_status"].strip(),
                "hours_estimated":    row["hours_estimated"].strip(),
            })

    return engagements, deliverables

# ============================================================
# STEP 2 — Migrate Engagements
# ============================================================
def migrate_engagements(engagements: dict) -> dict:
    """
    Create one monday.com item per engagement, skipping any that already exist.
    Returns a mapping of engagement_id -> monday item ID.
    """
    print(f"\n{'='*50}")
    print(f"Migrating {len(engagements)} engagements...")
    print(f"{'='*50}")

    # Check what's already on the board before creating anything
    print("  Checking for existing engagements...")
    existing = get_existing_engagement_ids()
    if existing:
        print(f"  Found {len(existing)} already migrated: {list(existing.keys())}")

    id_map = {**existing}  # seed map with any already-migrated items
    created = 0
    skipped = 0

    for eng in engagements.values():
        eng_id = eng["engagement_id"]

        # Skip if already exists
        if eng_id in existing:
            print(f"  skipping {eng_id} — already exists")
            skipped += 1
            continue

        raw_status = eng["engagement_status"].lower().strip()
        clean_status = ENGAGEMENT_STATUS_MAP.get(raw_status, "Not Started")

        column_values = {
            ENG_COL["engagement_id"]: eng_id,
            ENG_COL["client"]:        eng["client"],
            ENG_COL["lead"]:          eng["engagement_lead"],
            ENG_COL["start_date"]:    {"date": format_date(eng["engagement_start"])},
            ENG_COL["end_date"]:      {"date": format_date(eng["engagement_end"])},
            ENG_COL["budget"]:        eng["budget"],
            ENG_COL["status"]:        {"label": clean_status},
        }

        item_id = create_item(ENGAGEMENTS_BOARD_ID, eng["engagement_name"], column_values)
        id_map[eng_id] = item_id
        created += 1
        print(f"  created {eng_id} — {eng['engagement_name']} (item {item_id})")
        time.sleep(0.5)  # avoid 429 Too Many Requests

    print(f"\nEngagements — created: {created}, skipped (already exist): {skipped}")
    return id_map

# ============================================================
# STEP 3 — Migrate Deliverables
# ============================================================
def migrate_deliverables(deliverables: list, engagement_id_map: dict):
    """
    Create one monday.com item per deliverable, skipping any that already exist.
    """
    print(f"\n{'='*50}")
    print(f"Migrating {len(deliverables)} deliverables...")
    print(f"{'='*50}")

    # Check what's already on the board before creating anything
    print("  Checking for existing deliverables...")
    existing = get_existing_deliverable_ids()
    if existing:
        print(f"  Found {len(existing)} already migrated")

    success = 0
    skipped = 0
    failed = []

    for d in deliverables:
        del_id = d["deliverable_id"]

        # Skip if already exists
        if del_id in existing:
            print(f"  skipping {del_id} — already exists")
            skipped += 1
            continue

        raw_status   = d["deliverable_status"].lower().strip()
        raw_priority = d["priority"].lower().strip()
        clean_status   = DELIVERABLE_STATUS_MAP.get(raw_status, "To Do")
        clean_priority = PRIORITY_MAP.get(raw_priority, "Medium")

        monday_eng_id = engagement_id_map.get(d["engagement_id"])
        if not monday_eng_id:
            print(f"  ERROR {del_id} — no matching engagement {d['engagement_id']}")
            failed.append(del_id)
            continue

        column_values = {
            DEL_COL["deliverable_id"]:  del_id,
            DEL_COL["engagement_link"]: {"item_ids": [int(monday_eng_id)]},
            DEL_COL["assignee"]:        d["assignee"],
            DEL_COL["due_date"]:        {"date": format_date(d["due_date"])},
            DEL_COL["priority"]:        {"label": clean_priority},
            DEL_COL["status"]:          {"label": clean_status},
            DEL_COL["hours"]:           d["hours_estimated"],
        }

        item_id = create_item(DELIVERABLES_BOARD_ID, d["deliverable_name"], column_values)
        success += 1
        print(f"  created {del_id} — {d['deliverable_name']} (item {item_id})")
        time.sleep(0.5)  # avoid 429 Too Many Requests

    print(f"\nDeliverables — created: {success}, skipped (already exist): {skipped}")
    if failed:
        print(f"Failed (no matching engagement): {failed}")

# ============================================================
# MAIN
# ============================================================
def main():
    print("Nexus Consulting Group — Data Migration")
    print("monday.com Migration Script\n")


    # Load and parse CSV
    print(f"Loading data from {CSV_FILE}...")
    engagements, deliverables = load_csv(CSV_FILE)
    print(f"  Found {len(engagements)} unique engagements")
    print(f"  Found {len(deliverables)} deliverables")

    # Migrate engagements first (deliverables need their IDs)
    engagement_id_map = migrate_engagements(engagements)

    # Migrate deliverables with links back to engagements
    migrate_deliverables(deliverables, engagement_id_map)

    print(f"\n{'='*50}")
    print("Migration complete!")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
