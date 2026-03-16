# DRP Pipeline

A modular pipeline for collecting data from various sources (e.g., government websites) and uploading to repositories such as DataLumos.

- **[Setup](docs/Setup.md)** — Prerequisites and installation  
- **[Usage](docs/Usage.md)** — Parameters, SPA, command line, database  

## Overview

The DRP Pipeline is a Python-based data collection and processing system that:

- **Sources** candidate URLs from inventory spreadsheets (e.g. Data_Inventories)
- **Collects** data and metadata from web sources (e.g., Socrata)
- **Tracks** project status and progress in a SQLite database
- **Uploads** to repositories (e.g., DataLumos)
- **Publishes** projects, i.e., final committment in the repository
- **Updates** the original inventory spreadsheet

Projects move through a series of modules in order; each module updates status so the next can process eligible projects.

## Terminology

-  project - all the work done on behalf of a single source url - named after a datalumos project
-  asset - an individual file. projects usually have more than one file
-  metadata - information not contained in a file - often represents information extracted from a landing page
-  a stage in the pipeline that does a specific function, such as collection or uploading
-  batch - a set of projects run through a specific module

## Architecture

The code is broken up into a set of modules, each of which performs a step of the pipeline. There can be multiple modules for a given function, e.g. separate collectors for source websites that have different formatting.

The work of the pipeline is coordinated via a SQLite database. Metadata about each project is initialized by the **sourcing** module. More detailed info, e.g. metadata and files, is then **collected**. The metadata itself is stored in the database; files are kept on local disk and pointed to by a field in the database. Project contents are then **uploaded** to the repository. When all is complete, the project is **published** and the original source spreadsheet is **updated**.

Not all modules need to be used. For example, data can be collected by other means, a spreadsheet that contains the structure can then be imported into a sqllite database and then uploaded and published.

## Project directory structure

```
DRPPipeline/
├── collectors/          # Data collection (e.g., SocrataCollector)
├── cleanup_inprogress/  # Delete DataLumos projects in Deposit In Progress
├── debug/               # Debug scripts
├── docs/                # Setup, Usage, Google Sheets setup
├── duplicate_checking/  # Duplicate detection (e.g., DataLumos search)
├── orchestration/       # Orchestrator and module protocol
├── publisher/           # DataLumos publish and optional Google Sheet update
├── sourcing/            # Source URL discovery and project creation
├── storage/             # Database storage (SQLite)
├── upload/              # DataLumos upload (browser automation)
├── utils/               # Args, Logger, file/URL utilities
├── main.py              # Entry point
└── requirements.txt     # Dependencies
```

## Modules

| Module | Purpose |
|--------|--------|
| **noop** | No-op; useful for testing. |
| **sourcing** | Fetches candidate URLs from a spreadsheet, checks duplicates, creates DB records. |
| **socrata_collector** | Collects data and metadata from Socrata-hosted pages (e.g. data.cdc.gov). |
| **catalog_collector** | Collects download links from catalog.data.gov dataset pages. |
| **interactive_collector** | Flask app for manual collection: browse URLs, save PDFs, update metadata. |
| **upload** | Uploads collected data to DataLumos via browser automation. |
| **publisher** | Runs DataLumos publish workflow; also updates source inventory|
| **cleanup_inprogress** | Standalone utility that deletes DataLumos workspace projects in “Deposit In Progress” state (no DB changes). |

Each module (except `noop` and `cleanup_inprogress`) advances project `status` so the next module can run on eligible projects. See [Usage](docs/Usage.md) for how to run them and how the database is used.

## Implementation details

- **Module protocol** — Modules implement `run(drpid: int)` and use the shared **Storage** singleton to read/update project data. Sourcing runs once with `drpid=-1`; others are invoked per eligible project.
- **Orchestrator** — Resolves the requested module by name, loads its class, and runs it (once for no-prereq modules, or over the list of eligible projects for prereq-based modules). Uses `Args` for config and `num_rows` for batch limits.
- **Storage** — SQLite-backed singleton; exposes `initialize`, `create_record`, `get`, `update_record`, `append_to_field`, `list_eligible_projects`, etc.

See [Usage](docs/Usage.md) for database fields and eligibility rules.

## Interactive Collector (SPA)

The Interactive Collector is available in two modes:

- **Legacy:** Server-rendered at `/` (Flask templates, full-page reloads).
- **SPA:** React app at `/collector/` with JSON API, no full-page reloads.

### Running the SPA

1. **Backend:** `flask run` (or via orchestrator).
2. **Frontend (dev):** `cd interactive_collector/frontend && npm run dev` — Vite proxies `/api` to Flask.
3. **Production:** Build with `npm run build`, then Flask serves the built app at `/collector/`.

### SPA Architecture

- **Backend:** `interactive_collector/api.py` — Blueprint with `/api/projects/*`, `/api/projects/load`, `/api/scoreboard`, `/api/save`, `/api/download-file`, `/api/pipeline/*`, `/api/proxy`, `/extension/save-pdf`.
- **Frontend:** `interactive_collector/frontend/` — Vite + React + Zustand. Link clicks are intercepted via postMessage; pages load via API and update the Linked pane without reload.

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
