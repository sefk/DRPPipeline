# DRP Pipeline — Usage

This document describes how to use the pipeline. For installation, see [Setup](Setup.md).

There are two ways to run the pipeline: the **SPA GUI** (interactive collector and pipeline controls) and the **command line** (per-module invocation).

---

## 1. Parameters

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
  "google_username": "mkraley",
  "gwda_your_name": "Your Name"
}
```

For Google Sheets setup, see [GOOGLE_SHEETS_SETUP.md](GOOGLE_SHEETS_SETUP.md).

---

## 2. SPA usage

The **interactive collector** and **pipeline controls** are available as a single-page app (SPA) or as a server-rendered legacy UI.

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
   Vite proxies `/api` to the Flask backend.

3. **Production:**  
   Build with `npm run build`, then Flask serves the built app at `/collector/`.

### Interactive collector (SPA or legacy)

The collector uses the pipeline database. It can run **standalone** (form-driven) or **pipeline-driven** (first eligible project auto-loaded).

- **Standalone:** `python -m interactive_collector` — uses `drp_pipeline.db` in the current directory. Open http://127.0.0.1:5000/ and click **Interactive collector**. If there are no eligible projects, the form is shown; you can set **Start DRPID** or load by DRPID.
- **Pipeline-driven:** `python main.py interactive_collector` — same DB and args as other modules. The orchestrator loads the first project with `status="sourced"` and no errors. Open http://127.0.0.1:5000/.

**In the collector:** Scoreboard (left) lists visited/saved URLs by referrer with status (OK, 404, DL). Use **Copy & Open** to copy the launcher URL and paste it in a browser with the extension; save pages as PDF from there. **Load DRPID** / **Load** loads a project by ID. **Next** fetches the next eligible project. **Save** updates metadata and writes visited URLs to `status_notes`. Export scoreboard as JSON or visited URLs as CSV. Link clicks load pages in the Linked pane without full reloads. Status uses the same 404 and logical-404 detection as the pipeline.

### Browser extension

For sites that block automated access, use the browser extension to save PDFs from a real Chrome session. See [Setup](Setup.md#browser-extension-optional).

---

## 3. Command line usage

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

**Workflow order:** sourcing → socrata_collector / catalog_collector / interactive_collector → upload → publisher. Optional: cleanup_inprogress for stuck DataLumos projects.

### Modules

| Module | Purpose |
|--------|---------|
| **noop** | No-op; useful for testing. |
| **sourcing** | Fetches candidate URLs from the configured spreadsheet, checks duplicates, creates DB records. Requires `google_sheet_id`. Use `--sourcing-mode` to control which rows are selected (see below). |
| **socrata_collector** | Collects data and metadata from Socrata-hosted pages (e.g. data.cdc.gov). Processes `status="sourced"`. |
| **catalog_collector** | Collects download links from catalog.data.gov dataset pages. Processes `status="sourced"`. |
| **interactive_collector** | Flask app for manual collection; SPA at `/collector/`. |
| **upload** | Uploads collected data to DataLumos. Requires `datalumos_username`, `datalumos_password`. Processes `status="collected"`. |
| **publisher** | Runs DataLumos publish; optionally updates Google Sheet. Processes `status="uploaded"`. |
| **cleanup_inprogress** | Deletes DataLumos projects in Deposit In Progress state (no DB changes). |

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

- **Path:** `drp_pipeline.db` (default); use `--db-path` or `db_path` in config.
- **Key fields:** DRPID, source_url, status, warnings, errors.
- **Status:** Past-tense values (e.g. `sourced`, `collected`, `uploaded`, `published`).
- **Eligibility:** A project is eligible when `status` matches the module prerequisite and `errors` is empty.

### Batch and concurrency

- `--num-rows` / `num_rows` — Limits projects or URLs per run (omit for unlimited).
- `--max-workers` — Concurrent projects for modules that support it (default: 1).

---

## 4. Running tests

From the project root, run the full test suite:

```bash
python -m pytest -v
```

To run only the interactive collector tests:

```bash
python -m pytest interactive_collector/tests -v
```
