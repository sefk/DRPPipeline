# DRP Pipeline

A modular pipeline for collecting data from various sources (e.g., government websites) and uploading to repositories such as DataLumos.

- **[Setup](docs/Setup.md)** — Prerequisites and installation  
- **[Usage](docs/Usage.md)** — Parameters, SPA, command line, database  

## Overview

The DRP Pipeline is a Python-based data collection and processing system that:

- **Sources** candidate URLs from inventory spreadsheets (e.g. Data_Inventories)
- **Collects** datasets and metadata from web sources (e.g., government web sites)
- **Tracks** project status and progress in a SQLite database
- **Uploads** to repositories (e.g., DataLumos)
- **Publishes** projects, i.e., final committment in the repository
- **Updates** the original inventory spreadsheet

Projects move through a series of modules in order; each module updates status so the next can process eligible projects.
In some cases, we have multiple implementations of a particular module. This allows us to support different source and destination formats

## Terminology

-  source URL/file - a file which is the gateway to a set of information, typically an entry in a data repository index
-  project - all the work done on behalf of a single source url - named after a datalumos project
-  drpid - a numeric identifier for a project. Unique for a given database
-  asset - an individual file, e.g., dataset, PDF file. Projects usually have more than one file
-  metadata - information not contained in a file - often represents information extracted from a landing page
-  module - a stage in the pipeline that does a specific function, such as collection or uploading
-  batch - a set of projects run through a specific module

## Architecture

The code is broken up into a set of modules, each of which performs a step of the pipeline. There can be multiple modules for a given function, e.g. separate collectors for source websites that have different formatting.

The work of the pipeline is coordinated amongst the modules via a SQLite database. The list of potential source URLs are obtained by the **sourcing** module. The  e.g. metadata and files, is then **collected**. The metadata itself is stored in the database; files are kept on local disk in a folder pointed to by a field in the database. Project contents are then **uploaded** to the repository. When all is complete, the project is **published** and the original source spreadsheet is **updated**.

Not all modules need to be used. For example, data can be collected by other means, a spreadsheet that contains the structure can then be imported into a sqllite database and then uploaded and published.

## Project directory structure

```
DRPPipeline/
├── collectors/             # Data collection (e.g., SocrataCollector, CmsGovCollector)
├── cleanup_inprogress/     # Delete DataLumos projects in Deposit In Progress
├── debug/                  # Debug scripts
├── docs/                   # Setup, Usage, Google Sheets setup
├── interactive_collector/  # Flask app, SPA frontend, Chrome extension
│   └── extension/          # Chrome extension (Save as PDF, metadata preload, PDF-to-project)
├── duplicate_checking/     # Duplicate detection (e.g., DataLumos search)
├── mcp_server/             # MCP 1: pipeline orchestration tools
├── mcp_collector_dev/      # MCP 2: collector development tools
├── orchestration/          # Orchestrator and module protocol
├── publisher/              # DataLumos publish and optional Google Sheet update
├── sourcing/               # Source URL discovery and project creation
├── storage/                # Database storage (SQLite)
├── upload/                 # DataLumos upload (browser automation)
├── utils/                  # Args, Logger, file/URL utilities
├── main.py                 # Entry point
└── requirements.txt        # Dependencies
```

## Modules

| Module | Purpose |
|--------|--------|
| **sourcing** | Fetches candidate URLs from a spreadsheet, checks duplicates, creates DB records. |
| **socrata_collector** | Collects data and metadata from Socrata-hosted pages (e.g. data.cdc.gov). |
| **catalog_collector** | Collects download links from catalog.data.gov dataset pages. |
| **cms_collector** | Collects data from data.cms.gov API pages. |
| **interactive_collector** | Flask app for manual collection: browse URLs, save PDFs, update metadata. Under active development; not managed by the orchestration MCP. |
| **upload** | Uploads collected data to DataLumos via browser automation. |
| **publisher** | Runs DataLumos publish workflow; also updates source inventory|
| **cleanup_inprogress** | Standalone utility that deletes DataLumos workspace projects in “Deposit In Progress” state (no DB changes). |
| **noop** | No-op; useful for testing. |

Each module (except `noop` and `cleanup_inprogress`) advances project `status` so the next module can run on eligible projects. See [Usage](docs/Usage.md) for how to run them and how the database is used.

## MCP Servers

The pipeline exposes two [Model Context Protocol](https://modelcontextprotocol.io/) servers that allow Claude (and other MCP-compatible clients) to drive the pipeline without writing code. Both are registered in `.mcp.json`.

### MCP 1 — Pipeline Orchestration (`mcp_server/`)

The primary interface for running and monitoring the pipeline. Tools:

- **Query:** `get_pipeline_stats`, `list_projects`, `get_project`
- **Execution:** `run_module` (dry-run by default)
- **Write:** `update_project`, `clear_errors`, `set_project_status`, `delete_project`
- **Verification:** `verify_module_run`, `check_project_files`

All write tools default to `dry_run=True`. See [MCP.md](MCP.md) for the full design.

### MCP 2 — Collector Development (`mcp_collector_dev/`)

Tools for adding support for a new data source: inspecting a site, scaffolding a collector class, registering it with the Orchestrator, and running a test against a single project.

## Implementation details

- **Module protocol** — Modules implement `run(drpid: int)` and use the shared **Storage** singleton to read/update project data. Sourcing runs once with `drpid=-1`; others are invoked per eligible project.
- **Orchestrator** — Resolves the requested module by name, loads its class, and runs it (once for no-prereq modules, or over the list of eligible projects for prereq-based modules). Uses `Args` for config and `num_rows` for batch limits.
- **Storage** — SQLite-backed singleton; exposes `initialize`, `create_record`, `get`, `update_record`, `append_to_field`, `list_eligible_projects`, etc.

See [Usage](docs/Usage.md) for database fields and eligibility rules.

## Interactive Collector 

Most modules run as batch processes. The Interactive collector is the exception. 
The interactive collector allows the user to freely navigatge among source pages to choose the appropriate metadata, web pages, and datasets to save for later uploading. By taking advantage of a Chrome extension, the user can interact with the pipeline while browsing. One additional advantage is that by using a user controlled browser, we can handle most "are you a human" challenges as well as any required login credentials.

### Running the SPA

1. **Backend:** `flask run`. For **long pipeline runs from the main page** (e.g. upload, publisher) with `flask run --debug`, use **`--no-reload`** so the dev reloader does not restart mid-stream (`net::ERR_CONNECTION_RESET` in the browser). See [Usage § SPA](docs/Usage.md#3-spa-usage).
2. **Frontend (dev):** `cd interactive_collector/frontend && npm run dev` — Vite proxies `/api` to Flask.
3. **Production:** Build with `npm run build`, then Flask serves the built app at `/collector/`.

### SPA Implementation

- **Backend:** `interactive_collector/api.py` — Blueprint with `/api/projects/*`, `/api/projects/load`, `/api/scoreboard`, `/api/save`, `/api/download-file`, `/api/pipeline/*`, `/api/proxy`, `/api/extension/save-pdf`, `/api/metadata-from-page`, `/api/downloads-watcher/*`, plus `/api/chat/query` and `/api/chat/confirm` for pipeline chat.
- **Frontend:** `interactive_collector/frontend/` — Vite + React + Zustand. Link clicks are intercepted via postMessage; pages load via API and update the Linked pane without reload.
- **Pipeline chat orchestration:** top-level `pipeline_chat/` package handles planner, allowlisted execution, confirmation tokens, and audit logging. Mutating actions are proposal-first and require explicit confirmation before execution.

### Chrome extension

The **DRP Collector** Chrome extension (`interactive_collector/extension/`) allows the user to freely browse source files and interactively decide which metadata, pages, and files to include in the collection for later uploading. See [Interactive collector](docs/Setup.md#Interactive collector)

- **Manifest:** Manifest V3; permissions include `storage`, `debugger` (for browser print-to-PDF), `tabs`, `contextMenus`; host access for localhost and `*.data.gov` (and all URLs for the content script).
- **Content script** (`content.js`): Injected into all pages. On the launcher page (`/extension/launcher?drpid=…&url=…`), stores `drpid`, `collectorBase`, and `sourcePageUrl`, then redirects to the target URL. On other pages, if the watcher is active, shows a **Save as PDF** button; runs metadata preload (e.g. for data.cms.gov) and sends it to `/api/metadata-from-page`; intercepts clicks on PDF links and sends those URLs to the background so the PDF can be fetched and posted to the project.
- **Background** (`background.js`): Service worker. Handles messages: watcher status/stop, save PDF (blob or print-to-PDF via debugger API), metadata-from-page POST, and fetch-PDF-to-project (for direct PDF URLs). Adds context menu items: **Save this PDF to DRP project** (current tab) and **Save linked PDF to DRP project** (right-click on a link). Fetches PDFs from URLs and POSTs them to `/api/extension/save-pdf`.
- **Page script** (`page.js`): Injected into the page context for PDF generation: expands “Show more”, accordions, etc., then uses html2pdf/jsPDF or triggers Chrome’s print-to-PDF. Fires custom events so the content script can coordinate.

The extension calls the Flask backend at the collector origin (e.g. `http://localhost:5000`) for save-pdf, metadata-from-page, and downloads-watcher status/stop. See [Setup](docs/Setup.md#browser-extension-optional) for loading the extension and [Usage](docs/Usage.md) for the Copy & Open workflow.

## Development

- **Tests:** `python -m pytest` or `python -m unittest discover -p "test_*.py"`
- **New module:** Implement a class with `run(drpid: int)`, register it in `orchestration/Orchestrator.py` under `MODULES`, and add the module to the `module` argument in `Args`. The orchestrator discovers the class by name. See `.cursorrules` and existing modules for style (type hints, docstrings, one class per file, tests).
- **Code style:** PEP 8, type hints, unit tests; defaults in `Args._defaults`.

## Troubleshooting

- **ImportError (module class not found):** Ensure the class name matches the `MODULES` entry and the module is in the project tree (not only in tests).
- **Database:** Ensure `db_path` is writable; projects with non-empty `errors` are not eligible for later modules.
- **Playwright:** Run `playwright install`; use `upload_headless: false` in config for visible browser debugging.

For full configuration and command-line options, see [Usage](docs/Usage.md).

## Credits

Almost all of the code was written by Cursor

The original implementation for much of the uploader was done by @chiara using selenium. 

## License

[Add license information here]

## Contributing

[Add contributing guidelines here]
