# Collector Development Guide

How to add a new data source to the DRP Pipeline without writing code from scratch. This guide uses the **MCP 2 Collector Dev** server, which gives Claude Code a set of tools for inspecting sites, scaffolding collector classes, registering them, and running end-to-end tests.

---

## Prerequisites

- Python virtual environment set up: `python -m venv .venv && .venv/bin/pip install -r requirements.txt`
- Claude Code running in the project directory (`.mcp.json` is already configured)
- At least one project record in the database with `status = "sourced"` and a `source_url` pointing to the new data source

---

## Overview

A **collector** is a Python class with a single public method, `run(drpid)`, that:

1. Reads the project's `source_url` from Storage
2. Fetches data from that URL (HTML scraping, REST API, bulk download, etc.)
3. Saves files to a local output folder
4. Extracts metadata (title, agency, summary, keywords, dates, etc.)
5. Writes results back to Storage and sets `status = "collected"`

The full workflow has seven steps. Steps 1–2 and 4 use Claude's built-in file and web tools. Steps 3, 5, and 6 use the MCP tools provided by this server.

---

## Step-by-Step Workflow

### Step 1 — Inspect the source site

Use the MCP tools to examine the target URL before writing any code:

```
fetch_url_content(url="https://example.gov/datasets/my-dataset")
```

For a higher-level structural view (headings, links, JSON-LD metadata, API endpoints):

```
analyze_page_structure(url="https://example.gov/datasets/my-dataset")
```

Look for:
- Whether the page is static HTML or requires JavaScript rendering (Playwright may be needed)
- Download links and their file formats
- Structured metadata (JSON-LD `@type: Dataset` blocks, `<meta>` tags)
- Any REST API endpoints embedded in page scripts

### Step 2 — Read the interface spec and existing examples

Before writing the implementation, ask Claude to review the spec:

```
get_collector_interface()
```

This returns the full Storage schema, the implicit `run()` contract, and all available utility functions (`record_error`, `fetch_page_body`, `create_output_folder`, etc.).

To see working implementations as reference:

```
list_collector_examples()
```

This returns the full source of `SocrataCollector.py` and `CatalogDataCollector.py`. Use them to understand patterns for URL validation, Playwright usage, metadata extraction, and Storage updates.

### Step 3 — Scaffold the collector file

Generate the boilerplate class. Always preview with `dry_run=True` first:

```
scaffold_collector(
    class_name="ExampleGovCollector",
    module_name="example_gov_collector",
    description="Collector for example.gov open data portal",
    dry_run=True
)
```

When the output looks right, create the file:

```
scaffold_collector(
    class_name="ExampleGovCollector",
    module_name="example_gov_collector",
    description="Collector for example.gov open data portal",
    dry_run=False
)
```

This writes `collectors/ExampleGovCollector.py` with the standard structure:
- `__init__` with `headless` parameter
- `run(drpid)` — validates input, calls `_collect`, handles exceptions
- `_collect(url, drpid)` — stub with TODOs for fetching and metadata extraction
- `_update_storage_from_result(drpid, result)` — persists results and sets status

### Step 4 — Implement `_collect()`

Open `collectors/ExampleGovCollector.py` in Claude Code and fill in the `_collect()` method. The scaffolded TODOs guide what's needed:

- Fetch the page body (`fetch_page_body` handles AWS WAF automatically)
- Download files to `folder_path` (use `create_output_folder` to create it)
- Extract metadata fields: `title`, `agency`, `summary`, `keywords`, `time_start`, `time_end`
- Return a dict with Storage field names as keys

Refer to `SocrataCollector._collect()` for a full-featured example, or `CatalogDataCollector._collect()` for a lighter approach that only records links.

Key rules:
- Always call `record_error(drpid, msg)` and return early on failure; never raise
- Set `result["folder_path"]` to trigger `status = "collected"`
- Only return keys with non-None values; unused fields can be omitted

### Step 5 — Register the collector

Preview the change to `Orchestrator.py`:

```
register_collector(
    module_name="example_gov_collector",
    class_name="ExampleGovCollector",
    dry_run=True
)
```

Apply it:

```
register_collector(
    module_name="example_gov_collector",
    class_name="ExampleGovCollector",
    dry_run=False
)
```

This inserts the new entry into `MODULES` before the `upload` module. The Orchestrator uses dynamic class loading, so no import statement is needed.

### Step 6 — Test on a single project

Run the collector against one real project (requires a `sourced` DRPID with the right `source_url`):

```
test_collector_on_project(module_name="example_gov_collector", drpid=42)
```

The output shows:
- Exit code from the subprocess
- A diff of the Storage record before vs. after (status, folder_path, title, etc.)
- Files created in the output folder with sizes
- Any errors recorded in Storage
- Full subprocess stdout/stderr

**Note:** This makes real network requests and writes files to disk.

### Step 7 — Verify and scale up

Use MCP 1's verification tool to confirm the project advanced to `"collected"`:

```
verify_module_run(module="example_gov_collector", expected_count=1)
```

Then do a small batch dry-run before committing to full scale:

```
run_module(module="example_gov_collector", num_rows=5, dry_run=True)
run_module(module="example_gov_collector", num_rows=5, dry_run=False)
```

---

## Tool Reference

| Tool | Purpose | Key parameters |
|------|---------|----------------|
| `fetch_url_content` | Raw HTML/JSON body | `url`, `max_chars` (default 20 000) |
| `analyze_page_structure` | Title, headings, links, JSON-LD, API endpoints | `url` |
| `get_collector_interface` | Full spec: Storage schema, utils, status values | — |
| `list_collector_examples` | Source of all existing collector files | — |
| `scaffold_collector` | Generate boilerplate class file | `class_name`, `module_name`, `description`, `dry_run`, `overwrite` |
| `register_collector` | Add entry to `Orchestrator.MODULES` | `module_name`, `class_name`, `prereq`, `dry_run` |
| `test_collector_on_project` | End-to-end test on one real project | `module_name`, `drpid` |

All write operations (`scaffold_collector`, `register_collector`) default to `dry_run=True`.

---

## Safety Notes

- `scaffold_collector` will not overwrite an existing file unless `overwrite=True` is passed.
- `register_collector` will not add a duplicate module name.
- `test_collector_on_project` is intentionally scoped to one project to limit blast radius before batch use.
- After a successful test, always do a `run_module(..., dry_run=True)` before running at scale.

---

## Troubleshooting

**The page requires JavaScript to render.**
Static `fetch_url_content` will return the raw HTML without JS execution. Use Playwright in the collector via `_init_browser()` (see `SocrataCollector` as a reference) or `fetch_page_body`, which falls back to Playwright automatically for pages with AWS WAF challenges.

**`test_collector_on_project` shows no record changes.**
Check the subprocess stderr in the output. The most common causes are: missing `source_url`, the project's `status` is not `"sourced"`, or an unhandled exception in `_collect()` that was silently swallowed.

**`register_collector` can't find the insertion point.**
The tool looks for `    ,"upload": {` in `Orchestrator.py`. If that line has been reformatted, manual insertion is required — add the new module entry to `MODULES` immediately before the `"upload"` key.

**`scaffold_collector` produces a class that can't be imported.**
Run `.venv/bin/python -c "from collectors.ExampleGovCollector import ExampleGovCollector"` to check for import errors before testing end-to-end.
