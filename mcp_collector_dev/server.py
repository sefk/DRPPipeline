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
def test_collector_on_project(module_name: str, drpid: int) -> str:
    """
    Run a collector against a single project (--num-rows 1 --start-drpid <drpid>).

    Returns the Storage record before and after, files created in the output
    folder, and any errors recorded. Makes real network requests and writes
    files to disk.
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


if __name__ == "__main__":
    mcp.run()
