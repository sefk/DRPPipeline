# DRP Pipeline — Usage

This document describes how to use the pipeline. For installation, see [Setup](Setup.md).

There are three ways to run the pipeline: the **MCP orchestration server** (AI-assisted, recommended), the **SPA GUI** (interactive collector and pipeline controls), and the **command line** (per-module invocation).

---

## 1. MCP Orchestration (recommended)

The pipeline ships with an MCP server (`mcp_server/server.py`) that lets Claude drive the pipeline interactively — inspecting state, running modules with dry-run previews, fixing errors, and verifying results — without writing code or memorizing command-line flags.

### Setup

The server is registered automatically for Claude Code via `.mcp.json`. For Claude Desktop, add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "drp-pipeline": {
      "command": "/path/to/DRPPipeline/.venv/bin/python",
      "args": ["/path/to/DRPPipeline/mcp_server/server.py"]
    }
  }
}
```

### Available tools

| Tool | Category | Description |
|------|----------|-------------|
| `get_pipeline_stats` | Query | Total counts by status, error counts, db path |
| `list_projects` | Query | Filter by status and/or has_errors; paginated |
| `get_project` | Query | Full record for a single DRPID |
| `run_module` | Execution | Run any module; `dry_run=True` previews eligible projects |
| `update_project` | Write | Update metadata fields; returns a diff |
| `clear_errors` | Write | Clear errors so a project becomes eligible again |
| `set_project_status` | Write | Manually set status (e.g. roll back to `sourced`) |
| `delete_project` | Write | Remove a record (does not delete files) |
| `verify_module_run` | Verification | Check how many projects reached expected status |
| `check_project_files` | Verification | List files in a project's output folder |

All write tools and `run_module` default to `dry_run=True`. See [MCP.md](../MCP.md) for the full design and safety model.

### Typical session

```
1. get_pipeline_stats          → see overall state
2. list_projects(has_errors=True) → find stuck projects
3. get_project(drpid=5)        → inspect a specific project
4. clear_errors(drpid=5)       → make it eligible again
5. set_project_status(drpid=5, status="sourced") → roll back for re-collection
6. run_module("cms_collector", dry_run=True)  → preview eligible projects
7. run_module("cms_collector", dry_run=False) → execute
8. verify_module_run("cms_collector")         → confirm results
```

### Worked example: recovery operations (conversational)

This example shows the natural-language prompts a recovery engineer might type to Claude, and what Claude would do in response. No knowledge of tool names or parameters is required.

> **"How's the pipeline looking?"**

Claude calls `get_pipeline_stats` and replies: *"You have 11 projects total. 8 are collected and ready to upload, 1 has been uploaded, and 2 are stuck with errors."*

> **"What's wrong with the ones that have errors?"**

Claude calls `list_projects(has_errors=True)` then `get_project` on each, and replies: *"Both errors are in the CMS collector — DRPIDs 5 and 8. The collector couldn't find an API slug for their source URLs. This often means the CMS site restructured that path."*

> **"Clear the errors on both and roll them back so they get re-tried."**

Claude calls `clear_errors` and `set_project_status` (dry run first, showing what will change), confirms with you, then applies both changes. *"Done. DRPIDs 5 and 8 are back to 'sourced' with no errors and will be picked up on the next collector run."*

> **"Show me what the next upload batch would look like before we run it."**

Claude calls `run_module("upload", dry_run=True, num_rows=5)` and replies: *"The next upload batch would process 5 projects: Skilled Nursing Facility Change of Ownership, Federally Qualified Health Center Enrollments, and 3 others. Want me to run it?"*

> **"Yes, go ahead with those 5."**

Claude calls `run_module("upload", dry_run=False, num_rows=5)`, waits for completion, then calls `verify_module_run("upload")` and reports: *"Upload finished. 5 projects advanced to 'uploaded', no new errors."*

---

### Worked example: recovery operations (tool-level)

The following shows the underlying tool calls and exact outputs for the same scenario above.

**Step 1 — Check overall state**

```
get_pipeline_stats()
```
```
Database: drp_pipeline.db
Total projects: 11
With errors:    2
With warnings:  0

By status:
  collected: 8
  error: 2
  uploaded: 1
```

**Step 2 — Find stuck projects**

```
list_projects(has_errors=True)
```
```
Showing 2 of 2 matching projects (offset=0):

  DRPID=5  status='error' [ERRORS]  'https://data.cms.gov/medicare-shared-savings-program/...'
  DRPID=8  status='error' [ERRORS]  'https://data.cms.gov/provider-compliance/cost-report/...'
```

**Step 3 — Inspect one project**

```
get_project(drpid=5)
```
```
Project DRPID=5:
  DRPID: 5
  status: 'error'
  errors: 'Slug API returned nothing for path: /medicare-shared-savings-program/...'
  source_url: 'https://data.cms.gov/medicare-shared-savings-program/...'
  office: 'CMS'
```

**Step 4 — Preview clearing the error (dry run)**

```
clear_errors(drpid=5, dry_run=True)
```
```
[DRY RUN] clear_errors(DRPID=5):
  Current errors: 'Slug API returned nothing for path: /medicare-shared-savings-program/...'
  Status: 'error'

Run with dry_run=False to clear.
```

**Step 5 — Preview what upload would run (dry run)**

```
run_module("upload", dry_run=True, num_rows=5)
```
```
[DRY RUN] run_module('upload')
  prereq status: 'collected'
  output status: 'uploaded'
  Eligible projects (status='collected', no errors): 5
  num_rows limit: 5

  DRPID=1  'Skilled Nursing Facility Change of Ownership - Owner Information'
  DRPID=2  'Federally Qualified Health Center Enrollments'
  DRPID=3  'Federally Qualified Health Center All Owners'
  DRPID=4  'Medicare Geographic Variation - by Hospital Referral Region'
  DRPID=6  'Home Infusion Therapy Providers'

Run with dry_run=False to execute.
```

**Step 6 — Verify results after a collection run**

```
verify_module_run("cms_collector")
```
```
=== verify_module_run('cms_collector') ===
Expected output status: 'collected'
Projects at 'collected': 8
Projects with errors:   2

Sample errors (up to 5):

  DRPID=5:
    Slug API returned nothing for path: /medicare-shared-savings-program/...

  DRPID=8:
    Slug API returned nothing for path: /provider-compliance/cost-report/...
```

### Worked example: sourcing new projects (conversational)

This example shows how a recovery engineer would use natural language to pull new candidate URLs from the Google Sheet into the pipeline.

> **"How many new datasets could we pull from the CMS sheet?"**

Claude calls `preview_sourcing(num_rows=None)` and replies: *"The CMS sheet has 48 unclaimed rows matching the data.cms.gov prefix. I can see the full list — want me to show you a sample before running?"*

> **"Show me the first 5, then run sourcing to add the next batch."**

Claude calls `preview_sourcing(num_rows=5)` and shows 5 URLs from the sheet, then calls `run_module("sourcing", dry_run=True, num_rows=15)` to confirm the configured sheet and mode, then asks for confirmation.

> **"Looks good, go ahead."**

Claude calls `run_module("sourcing", dry_run=False, num_rows=15)`, waits for completion, then calls `verify_module_run("sourcing")` and `get_pipeline_stats()` and reports: *"Sourcing complete. 5 new projects added (DRPIDs 12–16), 10 skipped as duplicates already in the database. Pipeline now has 16 projects total, 5 ready to collect."*

---

### Worked example: sourcing new projects (tool-level)

The following shows the exact tool calls and outputs for the same scenario above.

**Step 1 — Preview what the sheet would yield**

```
preview_sourcing(num_rows=5)
```
```
preview_sourcing — sheet: 'CMS', mode: 'unclaimed'
URL prefix filter: https://data.cms.gov/
Sheet rows scanned: 50  |  Matching: 5  |  Skipped: 45

  https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/skilled-nursing-facility-change-of-ownership-owner-information  [CMS]
  https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/federally-qualified-health-center-enrollments  [CMS]
  https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/federally-qualified-health-center-all-owners  [CMS]
  https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-geographic-comparisons/medicare-geographic-variation-by-hospital-referral-region  [CMS]
  https://data.cms.gov/medicare-shared-savings-program/county-level-aggregate-expenditure-and-risk-score-data-on-assignable-beneficiaries  [CMS]
  ... (showing first 5; pass num_rows=None for all)

Note: URL availability and DB deduplication are not checked here.
Run run_module('sourcing', dry_run=False) to execute.
```

**Step 2 — Confirm sourcing configuration (dry run)**

```
run_module("sourcing", dry_run=True, num_rows=15)
```
```
[DRY RUN] run_module('sourcing')
  prereq status: None
  output status: 'sourced'
  Sourcing reads from a Google Sheet and creates DB records.
  Sheet:        1fpNctIesSYc2giu0aHduYLBxVYqlsMMMVhKIPVtY7P0
  Tab:          CMS
  URL column:   URL
  URL prefix:   https://data.cms.gov/
  Mode:         unclaimed
  Limit:        15 rows

  Use preview_sourcing() to see which sheet rows would be pulled
  without creating any DB records.

Run with dry_run=False to execute.
```

**Step 3 — Run sourcing**

```
run_module("sourcing", dry_run=False, num_rows=15)
```
```
=== run_module('sourcing') ===
Exit code: 0

── stdout ──
2026-03-17 - INFO - Storage initialized: drp_pipeline.db
2026-03-17 - INFO - Orchestrator running module='sourcing' num_rows=15
2026-03-17 - ERROR - Duplicate source URL already in storage, skipping (no row created): ...  [×10]
2026-03-17 - INFO - Sourcing: checking availability for 5 URLs (max_workers=1, timeout=15s)
2026-03-17 - INFO - Sourcing complete: 5 good (sourcing) (DRPIDs: 12-16),
                    10 dupe_in_storage (skipped, no row), 0 not_found, 0 errors
```

**Step 4 — Verify results**

```
verify_module_run("sourcing")
```
```
=== verify_module_run('sourcing') ===
Expected output status: 'sourced'
Projects at 'sourced': 5
Projects with errors:   2
```

**Step 5 — Check overall state**

```
get_pipeline_stats()
```
```
Database: drp_pipeline.db
Total projects: 16
With errors:    2
With warnings:  0

By status:
  collected: 8
  sourced: 5
  error: 2
  uploaded: 1
```

**Step 6 — Inspect the new projects**

```
list_projects(status="sourced")
```
```
Showing 5 of 5 matching projects (offset=0):

  DRPID=12  status='sourced'  'https://data.cms.gov/quality-of-care/deficit-reduction-act-...'
  DRPID=13  status='sourced'  'https://data.cms.gov/summary-statistics-on-use-and-payments/...'
  DRPID=14  status='sourced'  'https://data.cms.gov/cms-innovation-center-programs/end-stage-...'
  DRPID=15  status='sourced'  'https://data.cms.gov/cms-innovation-center-programs/end-stage-...'
  DRPID=16  status='sourced'  'https://data.cms.gov/summary-statistics-on-use-and-payments/...'
```

These projects are now ready to be processed by a collector (`cms_collector`, etc.).

---

## 2. Parameters

Configuration is resolved from (in order of priority, highest first):

1. **Command line arguments**
2. **Config file** (JSON, default `./config.json` if it exists)
3. **Default values**

If the config file does not exist, a warning is shown but the pipeline continues with defaults and command-line values.

### Parameter reference

| Parameter | CLI | Config | Default | Description |
|-----------|-----|--------|---------|-------------|
| `module` | required (positional) | — | — | Module to run: `noop`, `sourcing`, `socrata_collector`, `catalog_collector`, `interactive_collector`, `upload`, `publisher`, `cleanup_inprogress` |
| `config` / `-c` | yes | — | `./config.json` | Path to configuration file |
| `log_level` / `-l` | yes | `log_level` | `INFO` | Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `log_color` | yes (`--log-color`) | — | `false` | Color log severity in terminal (only when stdout is TTY) |
| `num_rows` / `-n` | yes | `num_rows` | `null` (unlimited) | Max projects or candidate URLs per batch |
| `start_row` | yes | `start_row` | `null` | 1-origin row to start at (skip earlier rows when listing from DB) |
| `start_drpid` | yes | `start_drpid` | `null` | Only process projects with DRPID ≥ this value |
| `db_path` | yes | `db_path` | `drp_pipeline.db` | Path to SQLite database file |
| `storage` | yes | — | `StorageSQLLite` | Storage implementation |
| `delete_all_db_entries` | yes | — | `false` | Delete all DB entries and reset auto-increment before running |
| `max_workers` / `-w` | yes | — | `1` | Max concurrent projects for modules that support it |
| `download_timeout_ms` | yes | — | `1800000` (30 min) | Download timeout in milliseconds |
| `no_use_url_download` | yes | — | `false` | Use Playwright save_as instead of URL + requests (no progress/resume) |
| `sourcing_url_column` | — | yes | `URL` | Column name for candidate URLs in sourcing sheet |
| `sourcing_url_prefix` | — | yes | `https://catalog.data.gov/` | Only source rows whose URL starts with this prefix; set to `""` for no filtering |
| `sourcing_fetch_timeout` | — | yes | `15` | Seconds per URL when checking availability in sourcing |
| `sourcing_mode` | yes (`--sourcing-mode`) | yes | `unclaimed` | Row filter: `unclaimed` (Claimed and Download Location empty), `completed` (Download Location filled), `all` (any row with a URL) |
| `base_output_dir` | — | yes | `C:\Documents\DataRescue\DRPData` | Base directory for collected files |
| `datalumos_username` | — | yes (required for upload) | — | DataLumos login email |
| `datalumos_password` | — | yes (required for upload) | — | DataLumos password |
| `upload_headless` | — | yes | `false` | Run browser in headless mode for upload |
| `upload_timeout` | — | yes | `60000` | Timeout in ms for upload operations |
| `socrata_app_token` | — | yes | — | Optional Socrata API token (avoids 403 on direct download) |
| `gwda_your_name` | — | yes (required for GWDA) | `""` | Name for GWDA nomination (nominates URLs to U.S. Gov Web & Data Archive) |
| `gwda_institution` | — | yes | `Data Rescue Project` | Institution for GWDA |
| `gwda_email` | — | yes | (from `datalumos_username`) | Email for GWDA |
| `google_sheet_id` | — | yes (required for sourcing) | — | Google Sheet ID from URL |
| `google_credentials` | — | yes | — | Path to service account JSON (for sheet updates) |
| `google_sheet_name` | — | yes | `CDC` | Worksheet/tab name |
| `google_username` | — | yes | `mkraley` | Value for "Claimed" column in inventory |

**Config-only:** Parameters without a CLI column can only be set in the config file (or use defaults).

### Config file format

Create `config.json` in the project root:

```json
{
  "log_level": "INFO",
  "num_rows": 10,
  "db_path": "drp_pipeline.db",
  "storage_implementation": "StorageSQLLite",
  "sourcing_url_column": "URL",
  "base_output_dir": "C:\\Documents\\DataRescue\\DRPData",
  "datalumos_username": "your@email",
  "datalumos_password": "your-password",
  "upload_headless": false,
  "upload_timeout": 60000,
  "google_sheet_id": "1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY",
  "google_credentials": "C:\\path\\to\\service-account.json",
  "google_sheet_name": "CDC",
  "google_username": "Your username",
  "gwda_your_name": "Your Name"
}
```

For Google Sheets setup, see [GOOGLE_SHEETS_SETUP.md](GOOGLE_SHEETS_SETUP.md).

---

## 3. SPA usage

### Running the SPA

1. **Backend:**  
   ```bash
   flask run
   ```
   Or via the orchestrator:  
   ```bash
   python main.py interactive_collector
   ```

2. **Frontend (development):**  
   ```bash
   cd interactive_collector/frontend && npm run dev
   ```
   Vite proxies `/api` and `/extension` to the Flask backend (the extension launcher must hit Flask, not the SPA).

   Dev-mode on `:5000` (Flask stays the public port with hot reload):

   - Start Vite (keeps running on `:5173`):  
     ```bash
     cd interactive_collector/frontend && npm run dev
     ```
   - Start Flask in debug mode (Flask on `:5000` proxies SPA requests to Vite):
     ```bash
     flask --app .\interactive_collector\app run --debug
     ```
   - Open `http://127.0.0.1:5000/`

   (If your Vite dev server is at a different origin/port, set `VITE_DEV_ORIGIN`.)

3. **Production:**  
   Build with `npm run build`, then Flask serves the built app at `/collector/`.

### Executing modules

To run a given module, e.g., sourcing, upload, publisher, just press the corresponding button on the main page. Output will be shown in the log window.

**Stale build on `:5000`:** Flask serves the **pre-built** SPA from `interactive_collector/frontend/dist`. The pipeline log is streamed as **NDJSON** and the UI parses each line. If you change the frontend TypeScript but do not rebuild, you will see raw frames like `{"line":...}` and `{"ping":true}` in the log pane, and lines can appear “missing” because nothing is decoding them. Fix: run `cd interactive_collector/frontend && npm run build`, or use **`http://127.0.0.1:5173/`** with `npm run dev` so Vite serves the current sources (Vite proxies `/api` to Flask).

**Upload, publisher, and other long runs:** With `--debug`, Flask’s stat **reloader** watches files and can restart the server while `/api/pipeline/run` is still streaming logs. The SPA then shows a network error (`net::ERR_CONNECTION_RESET`) and the log stops. Start the backend with reload disabled, for example:

```bash
flask --app .\interactive_collector\app run --debug --no-reload
```

(You can keep the interactive debugger; only automatic restarts are off.)

#### Parameters
`Start DRPID` - Begin scanning the database for the specified project and work from there. Blank means to start from the beginning.
`Max Rows` - Only execute this many projects, then exit. Blank means no limit.
`Log level` - Display log messages with this severity or higher.
`Max Workers` - Use multithreaded executors to speed things up.

### Pipeline chat (main page)

The SPA main page includes a **Pipeline Chat** panel that maps natural-language
requests to MCP 1 tools through the backend.

- Read-only tools execute immediately.
- Mutating tools return a proposal and require explicit **Confirm action**.
- Pending confirmations are session-bound and expire automatically.

Examples:

- `database status`
- `what's the next eligible project for collection`
- `call list_projects({"status":"sourced","limit":5})`
- `call run_module({"module":"cms_collector","dry_run":true})`


### Interactive collector

Most modules run as batch processes. The Interactive collector is the exception. 
Make sure you have installed the browser extension. See [Setup](Setup.md#browser-extension-optional)

Start the collector by pressing the `Interactive collector` button.
The UI will initally load the first project eligible for collection. Now press **Copy & Open**. The source URL for the project will open in another tab.
A **Save as PDF** button will be overlaid in the lower right corner. Press this convert the current web page's HTML to PDF and save it in the output folder created for this project. 
Now explore the links on the source page to find other relevant pages and/or datasets. As you encounter HTML pages of interest, press **Save as PDF**. If you click on a link which results in a download that would otherwise end up in your Downloads folder, the collector will intercept the downloaded file and move it to the output folder for the project. 

The collector also attempts to preload the Metadata fields where possible. Update these fields (typically from the source page) by manual entry or copy/paste.

Continue navigating until you have collected all the data that is appropriate for this project. Now press **Save**. The database will be updated to say that collection is complete `status = 'collected'` and to save the metadata values. Now press **Next** to load the next eligible project and repeat.

If the source page has no useful information, e.g., the datasets have been deleted or can't be found, press **No Links**. The project is updated in the database to record this status (`status = 'no links'`) and the project is not subject to further processing.

If you don't want to keep working on the current project, but want to come back later, e.g., the project will need more elaborate scripting, press **Skip**. A dialog will come up and ask for a reason. The database will be updated to `status = collector hold - {reason}`

If you want to work on a particular DRPID (not necessarily the next eligible), enter the id in the **Load DRPID** field and press **Load**.

The **Scoreboard** field keeps track of the files which have been collected for the given project.
The extension is considered to be active when `collecting`. The **Save as PDF** button and the capture of downloaded files is only active when in collecting mode. We enter this mode when **Copy & Open** is pressed, and exit when **Save**, **No Links**, or **Skip** is pressed. You may also exist collecting mode by clicking on the **Collecting** indicator.

---

## 4. Command line usage

This mode is similar to pressing the buttons on the SPA to run a particular module, but might be better suited to run batch processing.

### Basic invocation

```bash
python main.py <module> [options]
```

**Examples:**

```bash
python main.py sourcing
python main.py socrata_collector --num-rows 20 --max-workers 2
python main.py interactive_collector
python main.py upload --num-rows 5
python main.py publisher
python main.py cleanup_inprogress --log-color
```

**Claimed-by-name across all tabs:** Tallies non-empty cells in columns whose header in **row 1 or row 2** includes the whole word `claimed` (case-insensitive; not `unclaimed` / `disclaimed`). Also prints, per worksheet, how many rows have a non-empty **URL** column (`sourcing_url_column`, default `URL`) while every claimed header column is empty; lists tabs that have claimed columns but no exact URL header match. Then totals, unique claimants, worksheets with no claimed header, and per-name counts sorted descending. Downloads the spreadsheet once as XLSX using `google_sheet_id` and `google_credentials`:

```bash
python debug/tally_claimed_all_tabs.py
```

**Workflow order:** sourcing → socrata_collector / catalog_collector / cms_collector / interactive_collector → upload → publisher. Optional: cleanup_inprogress for stuck DataLumos projects.

### Modules

| Module | Purpose |
|--------|---------|
| **sourcing** | Fetches candidate URLs from the configured spreadsheet, checks duplicates, creates DB records. Requires `google_sheet_id`. Use `--sourcing-mode` to control which rows are selected (see below). |
| **socrata_collector** | Collects data and metadata from Socrata-hosted pages (e.g. data.cdc.gov). Processes `status="sourced"`. |
| **catalog_collector** | Collects download links from catalog.data.gov dataset pages. Processes `status="sourced"`. |
| **cms_collector** | Collects data from data.cms.gov API pages. Processes `status="sourced"`. |
| **interactive_collector** | Flask app for manual collection; SPA at `/collector/`. Under active development; not managed by the orchestration MCP. |
| **upload** | Uploads collected data to DataLumos. Requires `datalumos_username`, `datalumos_password`. Processes `status="collected"`. |
| **publisher** | Runs DataLumos publish; optionally updates Google Sheet. Processes `status="uploaded"`. |
| **cleanup_inprogress** | Deletes DataLumos projects in Deposit In Progress state (no DB changes). |
| **noop** | No-op; useful for testing. |

### Sourcing modes

`--sourcing-mode` (or `sourcing_mode` in config) controls which spreadsheet rows are selected:

| Mode | Rows selected |
|------|---------------|
| `unclaimed` (default) | `Claimed` empty **and** `Download Location` empty — unworked rows available for new collection |
| `completed` | `Download Location` non-empty — rows already manually archived |
| `all` | Any row with a non-empty URL, regardless of claim state |

`sourcing_url_prefix` is applied on top of the mode filter in all cases.

**Dev/test workflow:** use `completed` mode with a separate database to benchmark the automated pipeline against prior manual work:

```bash
python main.py sourcing --sourcing-mode completed --num-rows 10 --db-path benchmark.db
python main.py cms_collector --db-path benchmark.db
# compare benchmark.db results against the Download Location column in the sheet
```

### Database

You can create different databases to keep track of sets of projects, e.g. different sources. Just use `--db-path` or `db_path` in config. The default is `drp_pipeline.db`. An empty database will be created if none exists at the specified (or default) path.


### Batch and concurrency

- `--num-rows` / `num_rows` — Limits projects or URLs per run (omit for unlimited).
- `--max-workers` — Concurrent projects for modules that support it (default: 1).

---

## 5. Running tests

From the project root, run the full test suite:

```bash
python -m pytest -v
```

To run only the interactive collector tests:

```bash
python -m pytest interactive_collector/tests -v
```

### Coverage

To measure line coverage (requires `pytest-cov`):

```bash
python -m pytest --cov=. --cov-report=term-missing
```

This prints per-file coverage and highlights missing lines. Overall coverage is summarized at the end (e.g. ~77% for the full suite).
