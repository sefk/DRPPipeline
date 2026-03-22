# DRP Pipeline — Setup

This document covers prerequisites, installation, and configuration. For day-to-day usage, see [Usage](Usage.md).

## Prerequisites

- Python 3.13 or later
- pip (Python package manager)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd DRPPipeline
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   ```

3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Install Playwright browsers (required for web scraping and DataLumos automation):
   ```bash
   playwright install
   ```

## Configuration

After installing, run the setup wizard to create your configuration files and verify everything is working:

```
python main.py setup
```

The wizard walks through four phases:

1. **config.json** — If the file is missing, prompts for all required settings and writes the file. If it exists, reports it found.
2. **google-credentials.json** — If the file is missing, prints step-by-step instructions for provisioning a Google service account key and copying it into place. If it exists, reports it found.
3. **Validation** — Checks each file independently (JSON structure, required keys, paths) and then together (Google Sheets API access, tab name resolution).

Re-run `python main.py setup` at any time to validate your configuration — for example after editing `config.json` or rotating credentials.

### Example output (first run, files already present)

```
DRP Pipeline Setup
Checks and creates config.json and google-credentials.json.

────────────────────────────────────────────────────────────
Step 1: config.json
────────────────────────────────────────────────────────────
  ✓  config.json found: /path/to/DRPPipeline/config.json

────────────────────────────────────────────────────────────
Step 2: google-credentials.json
────────────────────────────────────────────────────────────
  ✓  Credentials file found: /path/to/DRPPipeline/google-credentials.json

────────────────────────────────────────────────────────────
Validation: config.json
────────────────────────────────────────────────────────────
  ✓  config.json exists
  ✓  config.json is valid JSON
  ✓  Required keys present
  ✓  base_output_dir exists  — /Volumes/ext1/drppipeline
  ✓  google_credentials path set  — /path/to/google-credentials.json

────────────────────────────────────────────────────────────
Validation: google-credentials.json
────────────────────────────────────────────────────────────
  ✓  Credentials file exists
  ✓  Credentials file is valid JSON
  ✓  Required credential keys present
  ✓  type is service_account
  ✓  client_email present  — drppipeline@your-project.iam.gserviceaccount.com

────────────────────────────────────────────────────────────
Validation: Sheet access
────────────────────────────────────────────────────────────
  ✓  Credentials path in config matches file
  ✓  google_sheet_id set  — 1fpNctIesSYc2giu0aHduYLBxVYqlsMMMVhKIPVtY7P0
  ✓  Google API libraries installed
  ✓  Google Sheet accessible  — Found 40 tab(s): README, TASKS YOU CAN DO, NPS, ...
  ✓  Sheet tab found  — Tab 'CMS' exists in the spreadsheet

────────────────────────────────────────────────────────────
Summary
────────────────────────────────────────────────────────────
  ✓  All checks passed. Pipeline is ready to run.
```

### Example output (config.json missing — interactive creation)

When `config.json` is absent the wizard prompts for each required value:

```
────────────────────────────────────────────────────────────
Step 1: config.json
────────────────────────────────────────────────────────────
  ~  config.json not found. Let's create one.

  Press Enter to accept defaults shown in [brackets].

  Database path (db_path) [drp_pipeline.db]:
  Download directory (base_output_dir): /Volumes/ext1/drppipeline
  DataLumos username (email): you@example.com
  DataLumos password:
  Google Sheet ID (from the URL): 1fpNctIesSYc2giu0aHduYLBxVYqlsMMMVhKIPVtY7P0
  Google Sheet tab name (google_sheet_name) [CDC]: CMS
  Path to google-credentials.json [google-credentials.json]:
  Your username for 'Claimed' column (google_username): yourname
  Your full name for GWDA nomination (gwda_your_name): Your Name
  URL prefix filter for sourcing (leave blank for none): https://data.cms.gov/
  Max rows per batch (num_rows, blank for unlimited): 10
  Log level [INFO]:

  Will write config.json with these values:
    db_path: 'drp_pipeline.db'
    base_output_dir: '/Volumes/ext1/drppipeline'
    datalumos_username: 'you@example.com'
    datalumos_password: '***'
    ...

  Write config.json? [Y/n]: y
  ✓  config.json written to /path/to/DRPPipeline/config.json
```

### Example output (credentials missing — provisioning walkthrough)

When the credentials file is absent the wizard prints numbered steps and then offers to copy the downloaded file:

```
────────────────────────────────────────────────────────────
Step 2: google-credentials.json
────────────────────────────────────────────────────────────
  ~  Credentials file not found: google-credentials.json

  Google service account credentials are required for:
    • Sourcing (resolving sheet tab name)
    • Publisher (updating the spreadsheet)

  Follow these steps to create a service account key:

  1. Go to https://console.cloud.google.com/
     Create a project or select an existing one.

  2. Enable the Google Sheets API:
     APIs & Services → Library → search 'Google Sheets API' → Enable

  3. Create a Service Account:
     APIs & Services → Credentials → Create Credentials → Service Account
     Give it a name (e.g. 'drp-pipeline') → Create and Continue → Done

  4. Download the JSON key:
     Click your service account → Keys tab → Add Key → Create new key → JSON
     Save the downloaded file.

  5. Share your Google Sheet with the service account:
     Open the sheet → Share → add the service account email (client_email)
     from the JSON file → give Editor permissions → Send

  Have you downloaded the JSON key file? [y/N]: y
  Path to downloaded JSON file [google-credentials.json]: ~/Downloads/drp-pipeline-abc123.json
  ✓  Copied to /path/to/DRPPipeline/google-credentials.json
```

### What goes in config.json

| Key | Required | Description |
|-----|----------|-------------|
| `db_path` | Yes | Path to the SQLite database (default: `drp_pipeline.db`) |
| `base_output_dir` | Yes | Directory where downloaded files are stored |
| `datalumos_username` | Yes | Email address for DataLumos login |
| `datalumos_password` | Yes | Password for DataLumos login |
| `google_sheet_id` | Yes | Sheet ID from the Google Sheets URL |
| `google_credentials` | Yes | Path to the service account JSON file |
| `google_sheet_name` | Yes | Worksheet/tab name (e.g. `CMS`, `CDC`) |
| `google_username` | Yes | Value written to the "Claimed" column when sourcing |
| `gwda_your_name` | Yes | Full name for GWDA nomination (upload step) |
| `sourcing_url_prefix` | No | Only source rows whose URL starts with this prefix |
| `num_rows` | No | Max projects per batch (omit for unlimited) |
| `upload_headless` | No | Run upload browser headlessly (default: `true`) |
| `log_level` | No | `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`) |

For Google Sheets provisioning details, see [GOOGLE_SHEETS_SETUP.md](GOOGLE_SHEETS_SETUP.md).

## Pipeline chat (SPA + MCP 1)

The SPA main page includes a pipeline chat panel that can translate natural-language
requests into MCP 1 tool calls (e.g. `get_pipeline_stats`, `list_projects`, `run_module`).

### Configure OpenAI (recommended: `config.json`)

This repo’s SPA pipeline chat planner reads OpenAI credentials from `config.json`
to avoid relying on environment variables.

Add these keys:

| Key | Required | Description |
|-----|----------|-------------|
| `pipeline_chat_openai_api_key` | Yes | OpenAI API key used for tool planning |
| `pipeline_chat_openai_model` | No | Model name (default: `gpt-4o-mini`) |

Optional tuning:

- `PIPELINE_CHAT_TOOL_TIMEOUT_SECONDS` (default: `120`) — tool execution timeout guardrail

### Env var fallback (optional)

For convenience, the planner also supports:

- `PIPELINE_CHAT_OPENAI_API_KEY` (preferred)
- `OPENAI_API_KEY` (fallback)
- `PIPELINE_CHAT_OPENAI_MODEL` (default: `gpt-4o-mini`)

### Cursor MCP config location

If you use Cursor as the MCP client, place MCP config at:

- Project-level: `.cursor/mcp.json`
- Global: `~/.cursor/mcp.json`

This repo includes a project-level `.cursor/mcp.json` that launches:

- `drp-pipeline`
- `drp-collector-dev`

via `mcp_python_wrapper.py`, which prefers `.venv` python when present and
falls back to system Python.

## Browser extension (optional - used by the interactive_collector)

The browser extension lets you browse source pages in a real browser and save pages as PDF to the interactive collector when AWS WAF blocks automated access.

### Extension installation

1. Open Chrome and go to `chrome://extensions`.
2. Enable **Developer mode** (toggle in the top right).
3. Click **Load unpacked**.
4. Select the `interactive_collector/extension` folder in this project.
5. The extension is now loaded.
6. If the extension code is updated, make sure the version number in manifest.json is bumped. Then navigate to `chrome://extensions` and click the circular arrow to reload the code.
