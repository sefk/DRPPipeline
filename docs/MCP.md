# DRP Pipeline MCP Servers — Design Plan

## Table of Contents

- [Goals](#goals)
- [MCP 1 — Orchestration](#mcp-1--orchestration)
- [MCP 2 — Collector Development](#mcp-2--collector-development)
- [MCP 3 — Uploader Development](#mcp-3--uploader-development)
- [Open Questions / Future Work](#open-questions--future-work)

---

## Goals

Make it possible for users to move data without having to write code or interact with the pipeline through technically challenging command line interfaces.

1. **Orchestration workflows should be easy to use and understand.** Visibility and safety are key — users should always be able to see what the pipeline will do before it does it, and understand what happened after.

2. **Development tasks (Collector and Uploader) should not require users to read or write code.** Having thorough unit and system tests is important. Good error handling is important — errors should be surfaced clearly with enough context to act on them.

---

## MCP 1 — DRP Pipeline Orchestration

This will be the most commonly used MCP. The goal is to provide a programmatic
interface around the modules described in [the usage documentation](docs/Usage.md) 

There are two initial use cases we're building for.

1. **Collector Development**. Developing a new collector is iterative. While
   being developed, the *DRP Pipeline Orchestration MCP* can be used as part of
   an iterative refinement loop to drive the pipeline and produce a measures of
   each iteration's quality.

2. **Recovery Operations**. Enable a *Recovery Engineer* as part of their the
   day-to-day use of the DRP Pipeline. In addition to managing work through the
   various modules, this user would like visibility and debugging assistance.

### Architecture

- **Transport**: `stdio` — compatible with Claude Desktop and Claude Code
- **DB access**: Reads `config.json` for `db_path` (falls back to `drp_pipeline.db` in project root); uses `sqlite3` directly to avoid singleton initialization overhead
- **Module execution**: Runs `python main.py <module> [args]` via subprocess, same pattern as `interactive_collector/api_pipeline.py`

### Files

```
mcp_server/
    __init__.py
    server.py          # all tools; entry point: python mcp_server/server.py
.mcp.json              # Claude Code config
requirements.txt       # add: mcp>=1.0.0
```

### Configuration

**Claude Code** (`.mcp.json` in project root):
```json
{
  "mcpServers": {
    "drp-pipeline": {
      "command": "python",
      "args": ["mcp_server/server.py"]
    }
  }
}
```

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "drp-pipeline": {
      "command": "python",
      "args": ["/Users/sefk/src/datarescue/DRPPipeline/mcp_server/server.py"]
    }
  }
}
```

### Tools

#### Query tools (read-only)

| Tool | Description |
|------|-------------|
| `get_pipeline_stats` | Total project count, counts by status, projects with errors/warnings, db path |
| `list_projects` | List projects filtered by status and/or has_errors; paginated with limit/offset |
| `get_project` | Full record for a single DRPID |

#### Pipeline execution

| Tool | Description |
|------|-------------|
| `run_module` | Run a pipeline module. `dry_run=True` (default) shows eligible projects without executing. `dry_run=False` runs via subprocess and returns captured log output. Accepts `num_rows`, `max_workers`, `start_drpid`, `log_level`. |

#### Write tools (all default to `dry_run=True`)

| Tool | Description |
|------|-------------|
| `update_project` | Update metadata fields (title, agency, office, summary, keywords, time_start, time_end, data_types, extensions, download_date, collection_notes, file_size, status_notes). Returns a diff of old vs new values. |
| `clear_errors` | Clear the `errors` field on a project so it becomes eligible for re-processing. |
| `set_project_status` | Manually set a project's status (e.g. roll back to `sourced` to re-collect). |
| `delete_project` | Delete a project record. Does not delete files from disk. |

#### Verification tools

| Tool | Description |
|------|-------------|
| `verify_module_run` | After running a module, checks how many projects reached the expected output status, how many are stuck with errors, and surfaces a sample of error messages. Accepts `expected_count` to assert against. |
| `check_project_files` | Lists files in a project's `folder_path`, with names, sizes, and extensions. Confirms folder exists. |

### Safety design

- All write tools and `run_module` default to `dry_run=True`.
- Dry-run responses are clearly labeled and describe exactly what *would* change.
- `delete_project` and `set_project_status` show the full current record before any deletion/mutation.
- Protected fields (`DRPID`, `source_url`, `datalumos_id`, `status`, `errors`, `warnings`, `published_url`) cannot be updated via `update_project`; use dedicated tools (`clear_errors`, `set_project_status`) for status/error fields.

### Module registry (from `orchestration/Orchestrator.py`)

| Module | Prereq status | Output status | Notes |
|--------|--------------|---------------|-------|
| `noop` | — | — | |
| `sourcing` | — | `sourced` | |
| `socrata_collector` | `sourced` | `collected` | |
| `catalog_collector` | `sourced` | `collected` | |
| `cms_collector` | `sourced` | `collected` | |
| `upload` | `collected` | `uploaded` | |
| `publisher` | `uploaded` | `published` | Also processes `not_found` and `no_links` (sheet-only update); dry-run shows all three buckets. |
| `cleanup_inprogress` | — | — | DataLumos only, no DB changes; `verify_module_run` will return an error for this module. |

This list is not exhaustive — more collectors will be added over time. `interactive_collector` is **not** managed by MCP 1; it is under active development as a separate tool and runs as a Flask app, which is incompatible with the subprocess execution model used here.

---

## MCP 2 — Collector Development

Enables adding support for a new data source without writing code.  The orchestrator uses this MCP to inspect a source site, scaffold a new collector, register it, and verify it works — producing a tested, integrated collector as output.

For the first version the user of MCP 2 will be technical users. While they won't be writing code themselves, what MCP2 produces itself is a code module that will have to be checked into version control and run manually. But we will consider for subsequent versions fronting the collector development itself with some sort of UI (eg. a web interface) so that even non-technical users could have collector development done for them with AI help.

### What a collector is

A collector is a Python class with a single public method: `run(drpid: int) -> None`. It:

1. Reads the project record from `Storage.get(drpid)` to get `source_url`
2. Fetches data from that URL (HTML scraping, REST API, bulk download, etc.)
3. Creates a local output folder, saves files there
4. Extracts metadata (title, agency, summary, keywords, dates, etc.)
5. Writes results back via `Storage.update_record(drpid, {...})` and sets `status = "collector"`
6. Records errors with `record_error(drpid, ...)` on failure

The class is registered in `orchestration/Orchestrator.py` under `MODULES` with a `prereq` of `"sourcing"`. The two existing collectors (`SocrataCollector`, `CatalogDataCollector`) serve as reference implementations.

### Dataflow

Proposed workflow:

1. **Inspect** -- Fetch the source URL and analyzes its structure. Use AI to analyze the site to pull out the key information needed when populating the site later.
2. **Reference** -- Claude reads the interface spec and existing collector examples
3. **Scaffold** -- MCP tool generates a new collector file from the standard template
4. **Write** -- Claude fills in the implementation (standard code editing)
5. **Register** -- MCP tool adds the module to Orchestrator.MODULES (with dry-run)
6. **Test** -- MCP tool runs the collector on a single real project and reports results
7. **Verify** -- MCP 1's verify_module_run confirms the project advanced to "collector"

Steps 1–2 and 4 use Claude Code's normal file/web tools. Steps 3, 5, and 6 need
MCP tools.

For the first version we can assumed that the user has their own Claude Code license to do this work. For future versions consider other models depending on what license the user has.

### Training and Prompt Development

We can use [DSPy] to managing the loop for inspecting sites to download from and developing the "prompts" to develop collectors. DSPy's goal is to not produce prompts per se, but programmatic LLM drivers.

We have one collector examples to work from that we could use as input to DSPy to develop its interfaces so we don't have to rely on writing and maintaining prompts.

But most of the iteration should be done over the wealth of manual rescue work done already. The Data Rescue Project tracks its work in a large [Google Spreadsheet][track]. Each tab represents a dataset to copy. The ones that have already been completed can be our training data.

* Look for tabs that have "DONE" in the tab name, for example "[NRLB - Done - In Tracker][nrlb]". Each row represents a site we have already done a manual collection on. Looking more at this example, the "URL" column H is where data came from, and the "Download Location" column N is where it went to.
* Since these URL's are generally unauthenticated and available, Claude should be able to iterate on each one to examine what fields in the source mapped which fields in DataLumos.

[DSPy]: https://dspy.ai/
[track]: https://docs.google.com/spreadsheets/d/1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY/edit?gid=1855551901#gid=1855551901
[nrlb]: https://docs.google.com/spreadsheets/d/1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY/edit?gid=1333679334#gid=1333679334

### Files

```
mcp_collector_dev/
    __init__.py
    server.py
```

### Tools

#### Inspection tools

| Tool | Description |
|------|-------------|
| `fetch_url_content` | Fetch a URL and return the raw HTML or JSON body (truncated to a readable size). |
| `analyze_page_structure` | Fetch a URL and return a structured summary: page title, headings (h1–h3), all links with anchor text, JSON-LD or `<meta>` structured data, and any API endpoints found in `<script>` tags. |

#### Reference tools (read-only)

| Tool | Description |
|------|-------------|
| `get_collector_interface` | Returns the `ModuleProtocol` definition, the `Storage` schema (all DB columns and types), and the utility functions available in `utils/` (file_utils, url_utils, Errors). This is the spec a new collector must satisfy. |
| `list_collector_examples` | Returns the full source of all existing collector files in `collectors/`. Lets Claude use them as concrete reference. |

#### Scaffolding and integration tools (with dry-run)

| Tool | Description |
|------|-------------|
| `scaffold_collector` | Given a class name, module name, and optional description, writes a new collector file under `collectors/` using the standard boilerplate: imports, `__init__`, `run`, `_collect`, `_update_storage_from_result`. `dry_run=True` (default) shows the file content; `dry_run=False` creates it. Will not overwrite an existing file unless `overwrite=True`. |
| `register_collector` | Adds an entry to `MODULES` in `orchestration/Orchestrator.py`. Accepts `module_name`, `class_name`, `prereq` (default `"sourcing"`). `dry_run=True` (default) shows the exact diff; `dry_run=False` applies it. Errors if the module name already exists. |

#### Testing tools

| Tool | Description |
|------|-------------|
| `test_collector_on_project` | Run the new collector against a single DRPID (`--num-rows 1 --start-drpid <drpid>`). Returns: Storage record before and after, files created in the output folder, any errors recorded. Designed to verify the collector works end-to-end before batch use. **Note:** this is a proof-of-concept tool. Once MCP 1 is built, this step should be refactored to use MCP 1's `run_module` (with `num_rows=1` and `start_drpid`) so the two MCPs share a single execution path. |

### Safety design

- `scaffold_collector` and `register_collector` both default to `dry_run=True`.
- `scaffold_collector` will not overwrite an existing file unless `overwrite=True` is passed.
- `register_collector` will not add a duplicate module name.
- `test_collector_on_project` is low-blast-radius (one project), but does make real network requests and write files to disk.
- After `test_collector_on_project`, use MCP 1's `verify_module_run` and `run_module` (dry-run first) to scale up to a batch.

---

## MCP 3 — Uploader Development

*Future work.* Similar in structure to MCP 2, but for adding support for uploading to a new data repository besides DataLumos. Not designed yet.

---

## Open Questions / Future Work

- `run_module` with `dry_run=False` blocks until the subprocess finishes and returns the full output. For long-running modules (upload, publisher with browser automation), this could take many minutes. A future enhancement could add a background-run mode that returns a job ID and a separate `poll_run` tool to check status.
- `cleanup_inprogress` has no DB effect and no verifiable output status; it only affects DataLumos. `verify_module_run` will return an error for this module.
- If `config.json` is absent, the server falls back to `drp_pipeline.db` in the project root. If the DB does not exist, all tools return a clear error.

## Comparison Tool Prompt

Please write an end-to-end test.

Use a project that has already been fetched. A good candidate is row 13 from the spreadsheet, where "Title of Site" = "Value Modifier". for this one project

- use the sourcing module to get this one row
- use the cms_collector module to get its data
- use use the upload module to create a new datalumos project and upload its data

Then once that's done, have the test compare the datalumos project that we created today ("treatment") vs. the one that was done previously
("control"). Column G has the control.

I'd like to eventually summarize the results
- green - identical
- yellow - some differences, e.g. missing some data or some metadata is different. There should be a summary of the differences
- red - pipeline failure, data not present or mostly incomplete, metadata significantly differnt, etc.

Once we get this working I plan to run over many more samples. But for this first test, let's just do one.
