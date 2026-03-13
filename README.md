## Repository Contents

| File | Description |
|---|---|
| `monday_csv_importer.py` | Migrates engagement and deliverable data from the Smartsheet CSV export into monday.com via the GraphQL API |
| `validate.py` | Validates the migration by comparing the source CSV against monday.com across 10 automated checks |
| `nexus_smartsheet_export.csv` | Source data export from Smartsheet — 27 deliverables across 6 engagements |
| `Nexus Consulting Migration Overview.pptx` | Slides for Nexus Consulting Stakeholder meeting |

## Setup

Both scripts require Python 3 and the `requests` library.
```bash
pip install requests
```

--- 
## Running the Migration
```bash
python migrate.py
```

The script will:
1. Parse the CSV and deduplicate 27 rows into 6 unique engagements
2. Check both boards for existing items before creating anything — safe to re-run
3. Create all 6 engagements on the Nexus Engagements board with normalised status values
4. Create all 27 deliverables on the Nexus Deliverables board, each linked to its parent engagement

**Note:** The script includes a 0.5s delay between API calls and automatic retry logic for rate limit errors. Do not remove the delay — monday.com uses a complexity budget system that will return 429 errors under sustained load.

---
## Running the Validation
```bash
python validate.py
```

The script runs 10 checks comparing the source CSV against monday.com:

| Check | What it verifies |
|---|---|
| Record Counts | 6 engagements and 27 deliverables present in monday.com |
| Missing Records | Every ID from the CSV exists in monday.com |
| Orphaned Deliverables | All 27 deliverables are linked to a parent engagement |
| Status Normalisation — Engagements | Legacy values correctly mapped to Active / Complete / On Hold / Not Started |
| Status Normalisation — Deliverables | Legacy values correctly mapped to To Do / In Progress / In Review / Done |
| Priority Normalisation | All priorities correctly mapped to High / Medium / Low |
| Data Quality — Engagements | All engagement fields populated (client, lead, dates, budget) |
| Data Quality — Deliverables | All deliverable fields populated (assignee, due date, hours) |

All 10 checks passing confirms a successful migration.

---
## Notes
- The validation script includes a 60 second pause between fetching engagements and deliverables to allow the monday.com complexity budget to recover between queries
