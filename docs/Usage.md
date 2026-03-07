# DRP Pipeline — Usage

This document describes how to run the pipeline and its modules. For installation and configuration, see [Setup](Setup.md).

## Basic usage

Run a single module:

```bash
python main.py <module> [options]
```

Examples:

```bash
python main.py sourcing
python main.py collector
python main.py sourcing --num-rows 10 --log-level DEBUG
python main.py collector --db-path C:\path\to\database.db
python main.py cleanup_inprogress --log-color
```

Module order in a typical workflow: **sourcing** → **collector** → **upload** → **publisher**. Optional: **cleanup_inprogress** to remove DataLumos projects stuck in Deposit In Progress.

## Modules

### `noop`

Does nothing. Useful for testing or satisfying the module argument.

```bash
python main.py noop
```

### `sourcing`

Discovers candidate source URLs from the configured spreadsheet, checks for duplicates, and creates database records for new projects.

```bash
python main.py sourcing
python main.py sourcing --num-rows 50
```

**Process:**

1. Fetches URLs from the configured spreadsheet
2. Checks for duplicates (local DB and optionally DataLumos)
3. Verifies source URL availability
4. Creates database records with generated DRPIDs

Requires `google_sheet_id` in config (the sheet ID from the Google Sheet URL). Uses `google_sheet_name` for the tab (when credentials are set, that tab is used for CSV; otherwise the first sheet). Optionally set `sourcing_url_column`.

### `collector`

Processes projects with `status="sourced"` and no errors: collects data and metadata (e.g. Socrata), then updates status.

```bash
python main.py collector
python main.py collector --num-rows 20 --max-workers 2
```

**Process:**

1. Finds eligible projects (`status="sourced"`, no errors)
2. For each project, collects data and metadata
3. Updates project status on success; appends warnings/errors as needed

### `upload`

Processes projects with `status="collected"` and no errors. Uploads collected data and metadata to DataLumos via browser automation.

```bash
python main.py upload
python main.py upload --num-rows 5
```

Requires `datalumos_username` and `datalumos_password` in config. Uses `upload_headless` and `upload_timeout` for browser behavior.

### `publisher`

Processes projects with `status="uploaded"` and a valid `datalumos_id`. Runs the DataLumos publish workflow and sets `published_url` and `status="published"`. Optionally updates a Google Sheet (inventory) when `google_sheet_id` and `google_credentials` are set.

```bash
python main.py publisher
python main.py publisher --num-rows 5
```

### `cleanup_inprogress`

Finds all projects in the DataLumos workspace in **Deposit In Progress** state and deletes them. Does not read or write the pipeline database. Uses the same credentials as upload/publisher.

```bash
python main.py cleanup_inprogress
```

## Database

The pipeline uses SQLite (default: `drp_pipeline.db` in the current directory). Use `--db-path` or config `db_path` to change location.

**Key fields:**

- **DRPID** — Unique project identifier
- **source_url** — Source URL for the project
- **status** — Past-tense workflow state (e.g. `sourced`, `collected`, `uploaded`, `published`)
- **warnings** — Newline-separated warning messages
- **errors** — Newline-separated error messages (projects with errors are skipped for later modules)

**Eligibility:**

A project is eligible for a module if:

- `status` equals that module’s prerequisite (e.g. `status="sourced"` for collector)
- `errors` is empty or null (warnings do not disqualify)

## Batch size and concurrency

- **`--num-rows`** — Limits how many projects or candidate URLs are processed in one run (omit for unlimited).
- **`--max-workers`** — For modules that support it (e.g. collector), number of concurrent projects (default: 1).

See [Setup](Setup.md) for all options and config file format.
