"""
MCP 2 — Collector Development Server

Tools for inspecting source sites, scaffolding new collectors,
registering them with the Orchestrator, and running tests — without
writing code manually.

Usage:
    python mcp_collector_dev/server.py
"""
from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
from pathlib import Path
from textwrap import dedent
from typing import Any

import requests
from bs4 import BeautifulSoup
from mcp.server.fastmcp import FastMCP

PROJECT_ROOT = Path(__file__).parent.parent
COLLECTORS_DIR = PROJECT_ROOT / "collectors"
ORCHESTRATOR_FILE = PROJECT_ROOT / "orchestration" / "Orchestrator.py"

mcp = FastMCP("drp-collector-dev")


# ── helpers ───────────────────────────────────────────────────────────────────

def _read_config() -> dict:
    config_path = PROJECT_ROOT / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {}


def _get_db_path() -> str:
    config = _read_config()
    return config.get("db_path", str(PROJECT_ROOT / "drp_pipeline.db"))


BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ── inspection tools ──────────────────────────────────────────────────────────

@mcp.tool()
def fetch_url_content(url: str, max_chars: int = 20000) -> str:
    """Fetch a URL and return the raw HTML or JSON body, truncated to max_chars."""
    try:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=30)
        body = resp.text
        truncated = len(body) > max_chars
        body = body[:max_chars]
        result = (
            f"Status: {resp.status_code}\n"
            f"Content-Type: {resp.headers.get('content-type', 'unknown')}\n\n"
            f"{body}"
        )
        if truncated:
            result += f"\n\n[TRUNCATED — showing first {max_chars} chars]"
        return result
    except Exception as e:
        return f"Error fetching {url}: {e}"


@mcp.tool()
def analyze_page_structure(url: str) -> str:
    """
    Fetch a URL and return a structured summary: page title, headings (h1-h3),
    all links with anchor text, JSON-LD / <meta> structured data, and any API
    endpoints found in <script> tags.
    """
    try:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")

        title = soup.title.string.strip() if soup.title else "(no title)"

        headings = [
            f"  [{t.name.upper()}] {t.get_text(strip=True)}"
            for t in soup.find_all(["h1", "h2", "h3"])
            if t.get_text(strip=True)
        ]

        links = [
            f"  {a.get_text(strip=True)!r} → {a['href']}"
            for a in soup.find_all("a", href=True)
        ]

        meta_items = [
            f"  {meta.get('name') or meta.get('property')}: {meta.get('content', '')}"
            for meta in soup.find_all("meta")
            if (meta.get("name") or meta.get("property")) and meta.get("content")
        ]

        json_ld_items: list[str] = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                json_ld_items.append(json.dumps(data, indent=2))
            except Exception:
                pass

        api_re = re.compile(r'https?://[^\s\'"<>]+/api/[^\s\'"<>]+')
        api_endpoints: list[str] = []
        for script in soup.find_all("script"):
            for match in api_re.findall(script.string or ""):
                if match not in api_endpoints:
                    api_endpoints.append(match)

        parts = [
            f"=== PAGE STRUCTURE: {url} ===",
            f"HTTP Status: {resp.status_code}",
            f"Title: {title}",
        ]
        if headings:
            parts.append(f"\nHeadings ({len(headings)}):")
            parts.extend(headings[:50])
        if links:
            parts.append(f"\nLinks ({len(links)}):")
            parts.extend(links[:100])
        if meta_items:
            parts.append(f"\nMeta tags ({len(meta_items)}):")
            parts.extend(meta_items[:30])
        if json_ld_items:
            parts.append(f"\nJSON-LD structured data ({len(json_ld_items)} block(s)):")
            for item in json_ld_items[:3]:
                parts.append(item[:2000])
        if api_endpoints:
            parts.append(f"\nAPI endpoints in scripts ({len(api_endpoints)}):")
            parts.extend(f"  {ep}" for ep in api_endpoints[:20])

        return "\n".join(parts)
    except Exception as e:
        return f"Error analyzing {url}: {e}"


# ── reference tools ───────────────────────────────────────────────────────────

@mcp.tool()
def get_collector_interface() -> str:
    """
    Returns the collector interface spec: the implicit ModuleProtocol, the Storage
    schema (all DB columns and types), and the utility functions available for use
    in new collectors (file_utils, url_utils, Errors, Logger).
    """
    return dedent("""
    ═══════════════════════════════════════════════════════════════════
    COLLECTOR INTERFACE SPECIFICATION
    ═══════════════════════════════════════════════════════════════════

    ── REQUIRED INTERFACE ───────────────────────────────────────────
    Every collector is a Python class with at minimum:

        class MyCollector:
            def __init__(self, headless: bool = True) -> None: ...
            def run(self, drpid: int) -> None: ...

    run() must:
      1. Call Storage.get(drpid) to get the project record (dict)
      2. Read source_url from the record
      3. Fetch/download data, create a local output folder
      4. Extract metadata (title, agency, summary, keywords, dates, etc.)
      5. Call Storage.update_record(drpid, {...}) with collected fields
      6. Set status="collected" when folder_path is written
      7. Call record_error(drpid, msg) on failure (sets status="error")

    ── STORAGE SCHEMA ───────────────────────────────────────────────
    Storage.get(drpid) returns a dict with these fields (None if unset):

      DRPID           INTEGER   Primary key (read-only)
      status          TEXT      Current pipeline status
      status_notes    TEXT      Human-readable notes
      warnings        TEXT      Newline-separated warnings
      errors          TEXT      Newline-separated errors (non-null = skip)
      datalumos_id    TEXT      DataLumos record ID (set by upload module)
      source_url      TEXT      Source URL (read-only after creation)
      folder_path     TEXT      Absolute path to local output folder
      title           TEXT      Dataset title
      agency          TEXT      Sponsoring agency
      office          TEXT      Sub-office/department
      summary         TEXT      Dataset description
      keywords        TEXT      Comma-separated tags
      time_start      TEXT      Temporal coverage start (ISO date or year)
      time_end        TEXT      Temporal coverage end
      data_types      TEXT      Data type(s) (e.g. "tabular", "geospatial")
      extensions      TEXT      File extensions found (e.g. ".csv,.json")
      download_date   TEXT      Date collected (ISO)
      collection_notes TEXT     Free-form notes about the collection
      file_size       TEXT      Human-readable total size (e.g. "12.3 MB")
      published_url   TEXT      DataLumos published URL (set by publisher)

    Storage methods used by collectors:
      Storage.get(drpid)                         → dict | None
      Storage.update_record(drpid, fields_dict)  → None
        (only pass keys you want to update; DRPID and source_url are protected)

    ── OUTPUT FOLDER ────────────────────────────────────────────────
    Use file_utils.create_output_folder(base_dir, drpid) to create the
    standard output folder (named DRP{drpid:06d}). The base_dir is
    Args.base_output_dir. Setting folder_path in Storage signals success.

    ── REGISTRATION ─────────────────────────────────────────────────
    After writing the class, register it in orchestration/Orchestrator.py:

        MODULES["my_collector"] = {
            "prereq": "sourced",
            "class_name": "MyCollector",
        }

    ── STATUS VALUES ────────────────────────────────────────────────
    Collectors consume projects with status="sourced" and produce
    status="collected" (by setting folder_path) or status="error".

    ── UTILITY FUNCTIONS ────────────────────────────────────────────
    from utils.Errors import record_error, record_warning, record_crash
      record_error(drpid, msg)    → sets status="error", appends to errors field
      record_warning(drpid, msg)  → appends to warnings field (non-fatal)
      record_crash(msg)           → fatal; raises RuntimeError

    from utils.Logger import Logger
      Logger.info(msg) / Logger.warning(msg) / Logger.error(msg)
      Logger.exception(msg)       → includes traceback

    from utils.file_utils import (
        create_output_folder,       → create DRP{drpid:06d} folder
        sanitize_filename,          → make a string safe for filenames
        format_file_size,           → e.g. "1.2 MB"
        folder_extensions_and_size, → (list[str], total_bytes)
    )

    from utils.url_utils import access_url, fetch_page_body, fetch_url_head, infer_file_type, is_valid_url
      is_valid_url(url)            → bool
      access_url(url, timeout=30)  → (success: bool, status_msg: str)
      fetch_page_body(url)         → (status_code, body, content_type, is_logical_404)
        handles AWS WAF automatically via Playwright fallback
      fetch_url_head(url)          → (status_code, content_type, error_msg)
      infer_file_type(url, content_type=None) → str  (e.g. "csv", "json")

    from utils.Args import Args
      Args.base_output_dir         → base folder for project outputs
      Args.download_timeout_ms     → ms timeout for large downloads
    """).strip()


@mcp.tool()
def list_collector_examples() -> str:
    """
    Returns the full source of all existing collector files in collectors/.
    Use these as concrete reference implementations.
    """
    collector_files = sorted(COLLECTORS_DIR.glob("*.py"))
    collector_files = [
        f for f in collector_files
        if not f.name.startswith("__") and "test" not in f.name.lower()
    ]
    if not collector_files:
        return "No collector files found in collectors/"

    parts = []
    for path in collector_files:
        parts.append(f"\n{'═' * 60}")
        parts.append(f"FILE: {path.name}")
        parts.append("═" * 60)
        parts.append(path.read_text(encoding="utf-8"))
    return "\n".join(parts)


# ── scaffolding and integration tools ─────────────────────────────────────────

_COLLECTOR_TEMPLATE = '''\
"""{class_name} — {description}

Collector for DRP Pipeline. Fetches data from a specific source type,
downloads files to local storage, and extracts metadata.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from storage import Storage
from utils.Args import Args
from utils.Errors import record_error, record_warning
from utils.Logger import Logger
from utils.file_utils import (
    create_output_folder,
    folder_extensions_and_size,
    format_file_size,
    sanitize_filename,
)
from utils.url_utils import access_url, fetch_page_body, is_valid_url


class {class_name}:
    """{description}"""

    def __init__(self, headless: bool = True) -> None:
        self._headless = headless

    def run(self, drpid: int) -> None:
        """ModuleProtocol entry point — called by Orchestrator for each eligible project."""
        record = Storage.get(drpid)
        if record is None:
            record_error(drpid, "Record not found in Storage", update_storage=False)
            return

        source_url = record.get("source_url")
        if not source_url or not is_valid_url(source_url):
            record_error(drpid, f"Invalid or missing source_url: {{source_url!r}}")
            return

        try:
            result = self._collect(source_url, drpid)
            self._update_storage_from_result(drpid, result)
        except Exception as exc:
            Logger.exception(f"Unexpected error in {class_name}: {{exc}}")
            record_error(drpid, f"Unexpected error: {{exc}}")

    def _collect(self, url: str, drpid: int) -> Dict[str, Any]:
        """
        Fetch the source URL and extract data and metadata.

        Returns a dict with Storage field names as keys. Set folder_path to
        signal successful collection (triggers status="collected").
        """
        result: Dict[str, Any] = {{}}

        # --- Validate URL is accessible ---
        ok, status_msg = access_url(url)
        if not ok:
            record_error(drpid, f"Cannot access URL: {{status_msg}}")
            return result

        # --- Create output folder ---
        base_dir = Path(Args.base_output_dir)
        folder_path = create_output_folder(base_dir, drpid)
        if folder_path is None:
            record_error(drpid, "Failed to create output folder")
            return result

        # --- Fetch page body ---
        # status_code, body, content_type, is_404 = fetch_page_body(url)
        # if is_404:
        #     record_error(drpid, f"Page not found (logical 404): {{url}}")
        #     return result

        # --- TODO: download files to folder_path ---

        # --- TODO: extract metadata ---
        # result["title"] = ...
        # result["agency"] = ...
        # result["summary"] = ...
        # result["keywords"] = ...
        # result["time_start"] = ...
        # result["time_end"] = ...

        # --- Record folder and file stats ---
        extensions, total_bytes = folder_extensions_and_size(folder_path)
        result["folder_path"] = str(folder_path)
        result["file_size"] = format_file_size(total_bytes)
        result["extensions"] = ",".join(extensions)
        return result

    def _update_storage_from_result(self, drpid: int, result: Dict[str, Any]) -> None:
        """Persist result to Storage and set status to collected or error."""
        update_fields = {{k: v for k, v in result.items() if v is not None}}
        if update_fields:
            Storage.update_record(drpid, update_fields)
        if result.get("folder_path"):
            Storage.update_record(drpid, {{"status": "collected"}})
        else:
            record_error(drpid, "Collection incomplete: no folder_path in result")
'''


@mcp.tool()
def scaffold_collector(
    class_name: str,
    module_name: str,
    description: str = "Collector for a new data source",
    dry_run: bool = True,
    overwrite: bool = False,
) -> str:
    """
    Generate a new collector file under collectors/ using the standard boilerplate.

    Args:
        class_name:   Python class name (e.g. "FooBarCollector")
        module_name:  Pipeline module name for registration (e.g. "foo_bar_collector")
        description:  One-line description of what this collector fetches
        dry_run:      True (default) shows the file content without writing it
        overwrite:    If True, overwrite an existing file (only when dry_run=False)
    """
    if not class_name or not class_name[0].isupper():
        return "Error: class_name must be a valid Python class name (e.g. 'FooBarCollector')"
    if not module_name or not re.match(r'^[a-z][a-z0-9_]*$', module_name):
        return "Error: module_name must be lowercase with underscores (e.g. 'foo_bar_collector')"

    file_path = COLLECTORS_DIR / f"{class_name}.py"
    content = _COLLECTOR_TEMPLATE.format(class_name=class_name, description=description)

    if dry_run:
        return (
            f"[DRY RUN] Would write: {file_path}\n"
            f"{'═' * 60}\n"
            f"{content}\n"
            f"{'═' * 60}\n"
            f"Run with dry_run=False to create the file."
        )

    if file_path.exists() and not overwrite:
        return (
            f"Error: {file_path} already exists. "
            f"Pass overwrite=True to overwrite it."
        )

    file_path.write_text(content, encoding="utf-8")
    return (
        f"Created: {file_path}\n"
        f"Next step: implement _collect() in that file, then call "
        f"register_collector(module_name={module_name!r}, class_name={class_name!r})."
    )


@mcp.tool()
def register_collector(
    module_name: str,
    class_name: str,
    prereq: str = "sourced",
    dry_run: bool = True,
) -> str:
    """
    Add a new entry to MODULES in orchestration/Orchestrator.py.

    Args:
        module_name:  Pipeline module name (e.g. "foo_bar_collector")
        class_name:   Python class name (e.g. "FooBarCollector")
        prereq:       Prerequisite status (default "sourced")
        dry_run:      True (default) shows the diff; False applies it
    """
    if not module_name or not re.match(r'^[a-z][a-z0-9_]*$', module_name):
        return "Error: module_name must be lowercase with underscores"

    orchestrator_text = ORCHESTRATOR_FILE.read_text(encoding="utf-8")

    if f'"{module_name}"' in orchestrator_text:
        return f"Error: module {module_name!r} already exists in MODULES. Choose a different name."

    new_entry = (
        f'    "{module_name}": {{\n'
        f'        "prereq": "{prereq}",\n'
        f'        "class_name": "{class_name}",\n'
        f'    }}\n'
    )

    # Insert before the ,"upload": line that follows the collector block
    insert_marker = '    ,"upload": {'
    if insert_marker not in orchestrator_text:
        return (
            f"Error: could not find insertion point ({insert_marker!r}) "
            f"in {ORCHESTRATOR_FILE}. Manual edit required."
        )

    new_text = orchestrator_text.replace(insert_marker, new_entry + insert_marker, 1)

    old_lines = set(orchestrator_text.splitlines())
    new_lines = [l for l in new_text.splitlines() if l not in old_lines]
    diff_display = "\n".join(f"  + {l}" for l in new_lines)

    if dry_run:
        return (
            f"[DRY RUN] Would add to MODULES in {ORCHESTRATOR_FILE.name}:\n\n"
            f"{diff_display}\n\n"
            f"Run with dry_run=False to apply."
        )

    ORCHESTRATOR_FILE.write_text(new_text, encoding="utf-8")
    return (
        f"Registered {module_name!r} → {class_name!r} in {ORCHESTRATOR_FILE.name}\n\n"
        f"Added:\n{diff_display}\n\n"
        f"Next step: test_collector_on_project(module_name={module_name!r}, drpid=<a sourced DRPID>)"
    )


# ── testing tools ─────────────────────────────────────────────────────────────

@mcp.tool()
def test_collector_on_project(module_name: str, drpid: int,
                              return_raw: bool = False) -> str:
    """
    Run a collector against a single project (--num-rows 1 --start-drpid <drpid>).

    Returns the Storage record before and after, files created in the output
    folder, and any errors recorded. Makes real network requests and writes
    files to disk.

    If return_raw=True, returns a JSON string with the structured post-run
    record (all Storage fields + files list) instead of the human-readable
    report. Used by the training system for scoring.
    """
    orchestrator_text = ORCHESTRATOR_FILE.read_text(encoding="utf-8")
    if f'"{module_name}"' not in orchestrator_text:
        return (
            f"Error: {module_name!r} is not registered in Orchestrator.MODULES. "
            f"Call register_collector() first."
        )

    db_path = _get_db_path()

    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM projects WHERE DRPID = ?", (drpid,)).fetchone()
        if row is None:
            con.close()
            return f"Error: DRPID {drpid} not found in database ({db_path})"
        record_before: dict[str, Any] = dict(row)
        con.close()
    except Exception as e:
        return f"Error reading database: {e}"

    cmd = [
        sys.executable, str(PROJECT_ROOT / "main.py"),
        module_name,
        "--num-rows", "1",
        "--start-drpid", str(drpid),
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(PROJECT_ROOT),
        )
        stdout = proc.stdout
        stderr = proc.stderr
        returncode = proc.returncode
    except subprocess.TimeoutExpired:
        return f"Error: collector timed out after 5 minutes (DRPID {drpid})"
    except Exception as e:
        return f"Error running subprocess: {e}"

    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM projects WHERE DRPID = ?", (drpid,)).fetchone()
        record_after: dict[str, Any] = dict(row) if row else {}
        con.close()
    except Exception as e:
        record_after = {"error": str(e)}

    # Diff the record
    changes: list[str] = []
    for key in record_after:
        before_val = record_before.get(key)
        after_val = record_after.get(key)
        if before_val != after_val:
            changes.append(f"  {key}: {before_val!r} → {after_val!r}")

    # List files in output folder
    folder_files: list[str] = []
    folder_path = record_after.get("folder_path") or record_before.get("folder_path")
    if folder_path and Path(folder_path).exists():
        for f in sorted(Path(folder_path).iterdir()):
            size = f.stat().st_size if f.is_file() else 0
            folder_files.append(f"  {f.name}  ({size:,} bytes)")

    if return_raw:
        # Structured output for the training scoring system
        raw_output = dict(record_after)
        raw_output["files"] = [
            {"name": Path(f.split("  ")[0].strip()).name}
            for f in folder_files
        ]
        if record_after.get("status") == "error" or record_after.get("errors"):
            raw_output["_crashed"] = True
        return json.dumps(raw_output)

    parts = [
        f"=== test_collector_on_project: {module_name!r} on DRPID {drpid} ===",
        f"Exit code: {returncode}",
        "",
        "── Record changes ──",
        *(changes if changes else ["  (no changes)"]),
        "",
        "── Files in output folder ──",
        *(folder_files if folder_files else ["  (no folder or no files)"]),
        "",
        "── Subprocess stdout ──",
        stdout[:5000] or "(none)",
    ]
    if stderr:
        parts += ["", "── Subprocess stderr ──", stderr[:2000]]
    if record_after.get("errors"):
        parts += ["", "── Errors recorded in Storage ──", f"  {record_after['errors']}"]

    return "\n".join(parts)


# ── training tools ────────────────────────────────────────────────────────────

def _get_training_db() -> Any:
    """Return the training DB path from project root."""
    return PROJECT_ROOT / "collector_training.db"


@mcp.tool()
def start_training_run(
    collector_name: str,
    collector_module_name: str,
    source_site: str,
    max_iterations: int = 20,
    max_cost_usd: float = 10.0,
    model_refine: str = "claude-sonnet-4-6",
    notes: str = "",
) -> str:
    """
    Initialize a new collector training run.

    Args:
        collector_name:        Python class name (e.g. "CmsGovCollector")
        collector_module_name: Pipeline module name (e.g. "cms_collector")
        source_site:           Source site hostname (e.g. "data.cms.gov")
        max_iterations:        Hard cap on iterations (default 20)
        max_cost_usd:          Budget ceiling in USD (default $10.00)
        model_refine:          Model for code refinement (default claude-sonnet-4-6)
        notes:                 Free-form notes about this run

    Returns run_id that can be passed to other training tools.
    """
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from collector_training.schema import init_db
        from collector_training.coordinator import TrainingConfig, TrainingCoordinator

        init_db()
        config = TrainingConfig(
            collector_name=collector_name,
            collector_module_name=collector_module_name,
            source_site=source_site,
            max_iterations=max_iterations,
            max_cost_usd=max_cost_usd,
            model_refine=model_refine,
            notes=notes,
        )
        coord = TrainingCoordinator(config)
        run_id = coord.create_run()
        return (
            f"Created training run {run_id} for {collector_name!r}.\n"
            f"Next steps:\n"
            f"  1. import_training_data(run_id={run_id}, ...)\n"
            f"  2. evaluate_collector(run_id={run_id}) — to score the baseline\n"
            f"  3. Run the full training loop via TrainingCoordinator.run({run_id})"
        )
    except Exception as e:
        return f"Error starting training run: {e}"


@mcp.tool()
def import_training_data(
    run_id: int,
    sheet_id: str,
    sheet_gid: str,
    url_column: str = "URL",
    status_column: str = "Status",
    done_value: str = "DONE",
    download_location_column: str = "Download Location",
    max_rows: int = 100,
    scrape_datalumos: bool = False,
) -> str:
    """
    Import training examples from a Google Sheets CSV export.

    Fetches rows with status == done_value and imports source URLs.
    If scrape_datalumos=True, also scrapes DataLumos for ground truth
    (requires Playwright + DataLumos login).

    For faster setup, set scrape_datalumos=False and provide ground truth
    via collector_training.importer.import_from_json_file() instead.

    Args:
        run_id:                  Training run ID (from start_training_run)
        sheet_id:                Google Sheets document ID
        sheet_gid:               Worksheet GID (visible in URL ?gid=XXXX)
        url_column:              Column header containing source URLs
        status_column:           Column header containing row status
        done_value:              Status value that marks completed rows
        download_location_column: Column with DataLumos workspace URL/ID
        max_rows:                Max rows to import (default 100)
        scrape_datalumos:        Whether to scrape DataLumos for ground truth
    """
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from collector_training.importer import (
            import_from_spreadsheet,
            assign_train_validation_split,
        )
        count = import_from_spreadsheet(
            run_id=run_id,
            sheet_id=sheet_id,
            sheet_gid=sheet_gid,
            url_column=url_column,
            status_column=status_column,
            done_value=done_value,
            download_location_column=download_location_column,
            max_rows=max_rows,
            scrape_datalumos=scrape_datalumos,
        )
        n_train, n_val = assign_train_validation_split(run_id)
        return (
            f"Imported {count} training examples for run {run_id}.\n"
            f"Split: {n_train} training / {n_val} validation\n"
            f"Note: ground truth fields will be empty unless scrape_datalumos=True\n"
            f"      or you call import_from_json_file() with pre-scraped data."
        )
    except Exception as e:
        return f"Error importing training data: {e}"


@mcp.tool()
def evaluate_collector(
    run_id: int,
    iteration_num: int = 0,
) -> str:
    """
    Run the current collector version against all training examples and score them.

    Uses the collector code saved for iteration_num (0 = initial/current file).
    Results are written to the training database.

    Returns aggregate score and per-field breakdown.
    """
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        import sqlite3 as _sqlite3
        from collector_training.schema import get_connection, init_eval_db
        from collector_training.importer import list_examples
        from collector_training.trainer import CollectorEvaluator, per_field_averages
        from collector_training.coordinator import TRAINING_ROOT

        # Get run config
        db = _get_training_db()
        con = get_connection(db)
        try:
            run_row = con.execute(
                "SELECT * FROM training_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if not run_row:
                return f"Error: run_id={run_id} not found."
            run = dict(run_row)
            import json as _json
            config = _json.loads(run.get("config_json") or "{}")
        finally:
            con.close()

        collector_name = run["collector_name"]
        module_name = config.get("collector_module_name", "")
        collector_file = PROJECT_ROOT / "collectors" / f"{collector_name}.py"

        # Load code for this iteration — search date-based dirs then fall back to collector
        run_date = run["started_at"][:10]
        version_filename = f"{collector_name}_run{run_id}_v{iteration_num}.py"
        version_file = TRAINING_ROOT / run_date / version_filename
        if version_file.exists():
            code = version_file.read_text(encoding="utf-8")
        else:
            code = collector_file.read_text(encoding="utf-8")

        examples = list_examples(run_id, include_validation=False, db_path=db)
        if not examples:
            return f"No training examples found for run_id={run_id}. Import data first."

        evaluator = CollectorEvaluator(
            collector_module_name=module_name,
            collector_file=collector_file,
        )
        results = evaluator.evaluate_all(examples, code, num_workers=1)
        aggregate = sum(r["score"] for r in results) / len(results) if results else 0.0
        field_avgs = per_field_averages([r["per_field"] for r in results])

        lines = [
            f"Evaluation — run {run_id}, iteration {iteration_num}",
            f"  Examples:        {len(results)}",
            f"  Aggregate score: {aggregate:.3f}",
            "",
            "  Per-field scores:",
        ]
        for field, score in sorted(field_avgs.items(), key=lambda x: x[1]):
            bar = "█" * int(score * 20) + "░" * (20 - int(score * 20))
            lines.append(f"    {field:<22}  {score:.3f}  {bar}")

        worst = sorted(results, key=lambda r: r["score"])[:3]
        if worst:
            lines += ["", "  Worst cases:"]
            for r in worst:
                lines.append(f"    {r['score']:.3f}  {r['source_url']}")
                if r.get("error_message"):
                    lines.append(f"           Error: {r['error_message']}")

        return "\n".join(lines)
    except Exception as e:
        return f"Error evaluating collector: {e}"


@mcp.tool()
def get_training_status(run_id: int) -> str:
    """
    Show the current state of a training run: iterations, scores, cost, best result.
    """
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from collector_training.schema import get_connection

        db = _get_training_db()
        con = get_connection(db)
        try:
            run_row = con.execute(
                "SELECT * FROM training_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if not run_row:
                return f"Error: run_id={run_id} not found."
            run = dict(run_row)

            iterations = con.execute(
                "SELECT iteration_num, aggregate_score, model_used, started_at, finished_at "
                "FROM iterations WHERE run_id=? ORDER BY iteration_num",
                (run_id,),
            ).fetchall()

            cost_row = con.execute(
                "SELECT COALESCE(SUM(cost_usd),0), COUNT(*) FROM token_usage WHERE run_id=?",
                (run_id,),
            ).fetchone()

            example_counts = con.execute(
                "SELECT is_validation, COUNT(*) FROM training_examples "
                "WHERE run_id=? GROUP BY is_validation",
                (run_id,),
            ).fetchall()
        finally:
            con.close()

        ec = {row[0]: row[1] for row in example_counts}
        n_train = ec.get(0, 0)
        n_val = ec.get(1, 0)

        lines = [
            f"Training Run {run_id}: {run['collector_name']} ({run['source_site']})",
            f"  Status:     {run.get('status', '?').upper()}",
            f"  Started:    {(run.get('started_at') or '')[:19].replace('T',' ')}",
        ]
        if run.get("finished_at"):
            lines.append(f"  Finished:   {run['finished_at'][:19].replace('T',' ')}")
        lines += [
            f"  Examples:   {n_train} training / {n_val} validation",
            f"  Iterations: {len(iterations)} completed",
            f"  Cost:       ${cost_row[0]:.4f} ({cost_row[1]} LLM calls)",
            "",
            "  Score trajectory:",
        ]

        for it in iterations:
            score_str = f"{it['aggregate_score']:.3f}" if it["aggregate_score"] is not None else "(pending)"
            marker = " ← best" if it["iteration_num"] == run.get("best_iteration") else ""
            lines.append(f"    Iteration {it['iteration_num']:>2}: {score_str}  [{it['model_used'] or '?'}]{marker}")

        if run.get("best_score") is not None:
            lines += [
                "",
                f"  Best score: {run['best_score']:.3f} (iteration {run.get('best_iteration')})",
            ]

        return "\n".join(lines)
    except Exception as e:
        return f"Error getting training status: {e}"


@mcp.tool()
def get_iteration_details(run_id: int, iteration_num: int) -> str:
    """
    Show per-project scores, diffs, and field breakdown for a specific iteration.
    """
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        import json as _json
        from collector_training.schema import get_connection

        db = _get_training_db()
        con = get_connection(db)
        try:
            it_row = con.execute(
                "SELECT * FROM iterations WHERE run_id=? AND iteration_num=?",
                (run_id, iteration_num),
            ).fetchone()
            if not it_row:
                return f"Iteration {iteration_num} not found for run {run_id}."
            it = dict(it_row)

            scores = con.execute(
                "SELECT ps.*, te.source_url FROM project_scores ps "
                "JOIN training_examples te ON ps.example_id = te.example_id "
                "WHERE ps.iteration_id=? ORDER BY ps.score ASC",
                (it["iteration_id"],),
            ).fetchall()
        finally:
            con.close()

        lines = [
            f"Iteration {iteration_num} — run {run_id}",
            f"  Aggregate score: {it.get('aggregate_score', '?')}",
            f"  Model:           {it.get('model_used', '?')}",
            f"  Strategy:        {it.get('refinement_strategy', '(none)')}",
            "",
        ]

        if it.get("per_field_scores_json"):
            field_scores = _json.loads(it["per_field_scores_json"])
            lines.append("  Per-field averages:")
            for f, s in sorted(field_scores.items(), key=lambda x: x[1]):
                bar = "█" * int(s * 20) + "░" * (20 - int(s * 20))
                lines.append(f"    {f:<22}  {s:.3f}  {bar}")
            lines.append("")

        lines.append(f"  Per-project scores ({len(scores)} projects):")
        for row in scores:
            score = row["score"]
            url = row["source_url"] or "?"
            err = row["error_message"] or ""
            lines.append(f"    {score:.3f}  {url}")
            if err:
                lines.append(f"           Error: {err[:100]}")
            if row["diff_json"]:
                diff = _json.loads(row["diff_json"])
                for field, d in diff.items():
                    exp = d.get("expected")
                    act = d.get("actual")
                    if exp != act and exp is not None:
                        lines.append(f"           {field}: expected={str(exp)[:50]!r} actual={str(act)[:50]!r}")

        return "\n".join(lines)
    except Exception as e:
        return f"Error getting iteration details: {e}"


@mcp.tool()
def stop_training_run(run_id: int) -> str:
    """
    Mark a training run as stopped. In-flight iterations will complete, then stop.

    Note: this sets the DB status to 'stopped'. The coordinator checks this flag
    each iteration. A currently-running coordinator will stop at the next iteration
    boundary.
    """
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from collector_training.schema import get_connection
        import datetime as _dt

        db = _get_training_db()
        con = get_connection(db)
        try:
            row = con.execute(
                "SELECT status FROM training_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if not row:
                return f"Error: run_id={run_id} not found."
            now = _dt.datetime.now(_dt.timezone.utc).isoformat()
            con.execute(
                "UPDATE training_runs SET status='stopped', finished_at=? WHERE run_id=?",
                (now, run_id),
            )
            con.commit()
        finally:
            con.close()
        return f"Run {run_id} marked as stopped. Active iterations will complete normally."
    except Exception as e:
        return f"Error stopping training run: {e}"


if __name__ == "__main__":
    mcp.run()
