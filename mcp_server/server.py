"""
MCP 1 — DRP Pipeline Orchestration Server

Provides programmatic access to the DRP Pipeline: querying project state,
running modules, updating records, and verifying results.

Usage:
    python mcp_server/server.py
"""
from __future__ import annotations

import csv
import io
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional
from urllib.request import Request, urlopen

from mcp.server.fastmcp import FastMCP

PROJECT_ROOT = Path(__file__).parent.parent

mcp = FastMCP("drp-pipeline")


# ── Module registry ────────────────────────────────────────────────────────────
# Mirrors orchestration/Orchestrator.py MODULES.
# prereq: status a project must have to be eligible (None = module runs once).
# output: status a project gets on success (None = no DB change).

_MODULES: dict[str, dict[str, Optional[str]]] = {
    "noop":               {"prereq": None,       "output": None},
    "setup":              {"prereq": None,       "output": None},
    "sourcing":           {"prereq": None,       "output": "sourced"},
    "socrata_collector":  {"prereq": "sourced",  "output": "collected"},
    "catalog_collector":  {"prereq": "sourced",  "output": "collected"},
    "cms_collector":      {"prereq": "sourced",  "output": "collected"},
    "upload":             {"prereq": "collected","output": "uploaded"},
    "publisher":          {"prereq": "uploaded", "output": "published"},
    "cleanup_inprogress": {"prereq": None,       "output": None},
}

# Fields that can be updated via update_project (not protected)
_UPDATABLE_FIELDS = {
    "title", "agency", "office", "summary", "keywords",
    "time_start", "time_end", "data_types", "extensions",
    "download_date", "collection_notes", "file_size", "status_notes",
}

# Fields that are protected from update_project (require dedicated tools)
_PROTECTED_FIELDS = {
    "DRPID", "source_url", "datalumos_id", "status",
    "errors", "warnings", "published_url", "folder_path",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read_config() -> dict:
    config_path = PROJECT_ROOT / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {}


def _get_sourcing_config() -> dict:
    """Return sourcing-related config values with their defaults."""
    config = _read_config()
    return {
        "google_sheet_id":   config.get("google_sheet_id", ""),
        "google_sheet_name": config.get("google_sheet_name", "CDC"),
        "google_credentials": config.get("google_credentials", ""),
        "sourcing_url_column": config.get("sourcing_url_column", "URL"),
        "sourcing_url_prefix": config.get("sourcing_url_prefix", "https://catalog.data.gov/"),
        "sourcing_mode":     config.get("sourcing_mode", "unclaimed"),
    }


def _get_db_path() -> str:
    config = _read_config()
    return config.get("db_path", str(PROJECT_ROOT / "drp_pipeline.db"))


def _connect() -> sqlite3.Connection:
    db_path = _get_db_path()
    if not Path(db_path).exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}. "
            "Run sourcing first, or check db_path in config.json."
        )
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def _format_project(row: dict[str, Any]) -> str:
    lines = []
    for k, v in row.items():
        if v is not None and v != "":
            lines.append(f"  {k}: {v!r}")
    return "\n".join(lines)


# ── Query tools ───────────────────────────────────────────────────────────────

@mcp.tool()
def get_pipeline_stats() -> str:
    """
    Return an overview of the pipeline: total project count, counts by status,
    projects with errors or warnings, and the database path.
    """
    try:
        con = _connect()
    except FileNotFoundError as e:
        return f"Error: {e}"

    try:
        total = con.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
        by_status = con.execute(
            "SELECT status, COUNT(*) as n FROM projects GROUP BY status ORDER BY n DESC"
        ).fetchall()
        with_errors = con.execute(
            "SELECT COUNT(*) FROM projects WHERE errors IS NOT NULL AND errors != ''"
        ).fetchone()[0]
        with_warnings = con.execute(
            "SELECT COUNT(*) FROM projects WHERE warnings IS NOT NULL AND warnings != ''"
        ).fetchone()[0]
    finally:
        con.close()

    lines = [
        f"Database: {_get_db_path()}",
        f"Total projects: {total}",
        f"With errors:    {with_errors}",
        f"With warnings:  {with_warnings}",
        "",
        "By status:",
    ]
    for row in by_status:
        status = row["status"] or "(null)"
        lines.append(f"  {status}: {row['n']}")

    return "\n".join(lines)


@mcp.tool()
def list_projects(
    status: Optional[str] = None,
    has_errors: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """
    List projects, optionally filtered by status and/or whether they have errors.
    Results are paginated (default limit=50, offset=0).

    Args:
        status:     Filter by status value (e.g. "sourced", "collected", "uploaded").
        has_errors: True = only projects with errors; False = only projects without errors.
        limit:      Max rows to return.
        offset:     Rows to skip (for pagination).
    """
    try:
        con = _connect()
    except FileNotFoundError as e:
        return f"Error: {e}"

    try:
        clauses = []
        params: list[Any] = []

        if status is not None:
            clauses.append("status = ?")
            params.append(status)

        if has_errors is True:
            clauses.append("errors IS NOT NULL AND errors != ''")
        elif has_errors is False:
            clauses.append("(errors IS NULL OR errors = '')")

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"SELECT * FROM projects {where} ORDER BY DRPID ASC LIMIT ? OFFSET ?"
        params += [limit, offset]

        rows = con.execute(query, params).fetchall()
        count_query = f"SELECT COUNT(*) FROM projects {where}"
        total = con.execute(count_query, params[:-2]).fetchone()[0]
    finally:
        con.close()

    if not rows:
        return f"No projects found (total matching: {total})."

    lines = [f"Showing {len(rows)} of {total} matching projects (offset={offset}):"]
    lines.append("")
    for row in rows:
        d = _row_to_dict(row)
        errors_flag = " [ERRORS]" if d.get("errors") else ""
        warnings_flag = " [WARNINGS]" if d.get("warnings") else ""
        lines.append(
            f"  DRPID={d['DRPID']}  status={d.get('status')!r}{errors_flag}{warnings_flag}"
            f"  {d.get('title') or d.get('source_url', '')!r}"
        )
    return "\n".join(lines)


@mcp.tool()
def get_project(drpid: int) -> str:
    """
    Return the full record for a single project by DRPID.

    Args:
        drpid: The project ID to look up.
    """
    try:
        con = _connect()
    except FileNotFoundError as e:
        return f"Error: {e}"

    try:
        row = con.execute("SELECT * FROM projects WHERE DRPID = ?", (drpid,)).fetchone()
    finally:
        con.close()

    if row is None:
        return f"Error: DRPID {drpid} not found."

    return f"Project DRPID={drpid}:\n{_format_project(_row_to_dict(row))}"


# ── Pipeline execution ────────────────────────────────────────────────────────

@mcp.tool()
def run_module(
    module: str,
    dry_run: bool = True,
    num_rows: Optional[int] = None,
    max_workers: Optional[int] = None,
    start_drpid: Optional[int] = None,
    log_level: str = "INFO",
    sourcing_mode: Optional[str] = None,
) -> str:
    """
    Run a pipeline module.

    dry_run=True (default): show which projects are eligible without running.
    dry_run=False: execute via subprocess and return captured log output.

    Supported modules: noop, sourcing, socrata_collector, catalog_collector,
    cms_collector, upload, publisher, cleanup_inprogress.

    Args:
        module:        Module name to run.
        dry_run:       If True, show eligible projects without executing.
        num_rows:      Max projects to process (None = all eligible).
        max_workers:   Parallel workers (None = use config default).
        start_drpid:   Only process projects with DRPID >= this value.
        log_level:     Logging verbosity (DEBUG, INFO, WARNING, ERROR).
        sourcing_mode: For sourcing only — row filter: "unclaimed" (default),
                       "completed" (Download Location filled), or "all".
                       Overrides the sourcing_mode in config.json for this run.
    """
    if module not in _MODULES:
        valid = ", ".join(sorted(_MODULES.keys()))
        return f"Error: unknown module {module!r}. Valid modules: {valid}"

    info = _MODULES[module]
    prereq = info["prereq"]
    output = info["output"]

    if dry_run:
        lines = [f"[DRY RUN] run_module({module!r})"]
        lines.append(f"  prereq status: {prereq!r}")
        lines.append(f"  output status: {output!r}")

        if prereq is None:
            if module == "sourcing":
                sc = _get_sourcing_config()
                effective_mode = sourcing_mode or sc["sourcing_mode"]
                lines.append("  Sourcing reads from a Google Sheet and creates DB records.")
                lines.append(f"  Sheet:        {sc['google_sheet_id'] or '(not configured)'}")
                lines.append(f"  Tab:          {sc['google_sheet_name']}")
                lines.append(f"  URL column:   {sc['sourcing_url_column']}")
                lines.append(f"  URL prefix:   {sc['sourcing_url_prefix'] or '(none)'}")
                lines.append(f"  Mode:         {effective_mode}")
                if num_rows is not None:
                    lines.append(f"  Limit:        {num_rows} rows")
                lines.append("")
                lines.append("  Use preview_sourcing() to see which sheet rows would be pulled")
                lines.append("  without creating any DB records.")
            elif module == "cleanup_inprogress":
                lines.append("  This module runs once (no per-project loop).")
                lines.append("  Note: cleanup_inprogress only affects DataLumos, no DB changes.")
            else:
                lines.append("  This module runs once (no per-project loop).")
        else:
            try:
                con = _connect()
                try:
                    q = (
                        "SELECT DRPID, source_url, title FROM projects "
                        "WHERE status = ? AND (errors IS NULL OR errors = '')"
                    )
                    params: list[Any] = [prereq]
                    if start_drpid is not None:
                        q += " AND DRPID >= ?"
                        params.append(start_drpid)
                    q += " ORDER BY DRPID ASC"
                    if num_rows is not None:
                        q += " LIMIT ?"
                        params.append(num_rows)
                    rows = con.execute(q, params).fetchall()
                finally:
                    con.close()

                lines.append(f"  Eligible projects (status={prereq!r}, no errors): {len(rows)}")
                if start_drpid is not None:
                    lines.append(f"  start_drpid filter: DRPID >= {start_drpid}")
                if num_rows is not None:
                    lines.append(f"  num_rows limit: {num_rows}")
                lines.append("")
                for row in rows[:20]:
                    d = _row_to_dict(row)
                    label = d.get("title") or d.get("source_url", "")
                    lines.append(f"  DRPID={d['DRPID']}  {label!r}")
                if len(rows) > 20:
                    lines.append(f"  ... and {len(rows) - 20} more")

            except FileNotFoundError as e:
                lines.append(f"  Error: {e}")

        lines.append("")
        lines.append("Run with dry_run=False to execute.")
        return "\n".join(lines)

    # --- Execute ---
    cmd = [sys.executable, str(PROJECT_ROOT / "main.py"), module, "--log-level", log_level]
    if num_rows is not None:
        cmd += ["--num-rows", str(num_rows)]
    if max_workers is not None:
        cmd += ["--max-workers", str(max_workers)]
    if start_drpid is not None:
        cmd += ["--start-drpid", str(start_drpid)]
    if sourcing_mode is not None:
        cmd += ["--sourcing-mode", sourcing_mode]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minutes
            cwd=str(PROJECT_ROOT),
        )
    except subprocess.TimeoutExpired:
        return f"Error: run_module({module!r}) timed out after 30 minutes."
    except Exception as e:
        return f"Error launching subprocess: {e}"

    parts = [
        f"=== run_module({module!r}) ===",
        f"Exit code: {proc.returncode}",
        "",
        "── stdout ──",
        proc.stdout[:8000] or "(none)",
    ]
    if proc.stderr:
        parts += ["", "── stderr ──", proc.stderr[:3000]]
    return "\n".join(parts)


# ── Sourcing helpers ─────────────────────────────────────────────────────────

def _fetch_sheet_csv(sheet_id: str, gid: str) -> str:
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    )
    req = Request(export_url, headers={"User-Agent": "DRPPipeline/1.0"})
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8-sig")


def _row_passes_sourcing_filter(row: dict, mode: str, url_prefix: str) -> bool:
    claimed = (row.get("Claimed (add your name)") or "").strip()
    download_location = (row.get("Download Location") or "").strip()
    url = (row.get("URL") or "").strip()
    if mode == "unclaimed":
        passes = claimed == "" and download_location == ""
    elif mode == "completed":
        passes = download_location != ""
    elif mode == "all":
        passes = True
    else:
        passes = claimed == "" and download_location == ""
    return passes and (not url_prefix or url.startswith(url_prefix))


@mcp.tool()
def preview_sourcing(
    num_rows: Optional[int] = 20,
    sourcing_mode: Optional[str] = None,
) -> str:
    """
    Fetch candidate URLs from the configured Google Sheet and show what sourcing
    would process — without creating any database records.

    Applies the same row filter as the real sourcing run (mode + URL prefix).
    Does NOT check URL availability or deduplicate against the DB.

    Args:
        num_rows:      Max rows to preview (default 20, None = all matching).
        sourcing_mode: Row filter override: "unclaimed" (default), "completed",
                       or "all". If omitted, uses sourcing_mode from config.json.
    """
    sc = _get_sourcing_config()
    sheet_id = sc["google_sheet_id"]
    sheet_name = sc["google_sheet_name"]
    creds_path_str = sc["google_credentials"]
    url_column = sc["sourcing_url_column"]
    url_prefix = sc["sourcing_url_prefix"]
    effective_mode = sourcing_mode or sc["sourcing_mode"]

    if not sheet_id:
        return "Error: google_sheet_id is not set in config.json."
    if not creds_path_str:
        return "Error: google_credentials is not set in config.json."

    creds_path = Path(creds_path_str)
    if not creds_path.exists():
        return f"Error: google_credentials file not found: {creds_path}"

    # Resolve sheet name to gid via Sheets API
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from utils.sheet_url_utils import get_gid_for_sheet_name
        gid = get_gid_for_sheet_name(sheet_id, sheet_name, creds_path)
    except Exception as e:
        return f"Error resolving sheet tab '{sheet_name}': {e}"

    if gid is None:
        return (
            f"Error: tab '{sheet_name}' not found in spreadsheet. "
            "Check google_sheet_name in config.json."
        )

    # Fetch CSV
    try:
        csv_text = _fetch_sheet_csv(sheet_id, gid)
    except Exception as e:
        return f"Error fetching sheet CSV: {e}"

    # Parse and filter
    reader = csv.DictReader(io.StringIO(csv_text))
    fieldnames = list(reader.fieldnames or [])

    if url_column not in fieldnames:
        return (
            f"Error: URL column '{url_column}' not found in sheet. "
            f"Available columns: {fieldnames}"
        )

    matching: list[dict] = []
    skipped = 0
    total_rows = 0

    for row in reader:
        total_rows += 1
        url = (row.get(url_column) or "").strip()
        if not url:
            skipped += 1
            continue
        if _row_passes_sourcing_filter(row, effective_mode, url_prefix):
            matching.append({
                "url": url,
                "office": (row.get("Office") or "").strip(),
                "agency": (row.get("Agency") or "").strip(),
            })
        else:
            skipped += 1

        if num_rows is not None and len(matching) >= num_rows:
            break

    lines = [
        f"preview_sourcing — sheet: '{sheet_name}', mode: '{effective_mode}'",
        f"URL prefix filter: {url_prefix or '(none)'}",
        f"Sheet rows scanned: {total_rows}  |  Matching: {len(matching)}  |  Skipped: {skipped}",
        "",
    ]
    for item in matching:
        office = f"  [{item['office']}]" if item["office"] else ""
        lines.append(f"  {item['url']}{office}")

    if num_rows is not None and len(matching) >= num_rows:
        lines.append(f"  ... (showing first {num_rows}; pass num_rows=None for all)")

    lines.append("")
    lines.append("Note: URL availability and DB deduplication are not checked here.")
    lines.append("Run run_module('sourcing', dry_run=False) to execute.")
    return "\n".join(lines)


# ── Write tools ───────────────────────────────────────────────────────────────

@mcp.tool()
def update_project(drpid: int, fields: dict[str, Any], dry_run: bool = True) -> str:
    """
    Update metadata fields on a project. Returns a diff of old vs new values.

    Updatable fields: title, agency, office, summary, keywords, time_start,
    time_end, data_types, extensions, download_date, collection_notes,
    file_size, status_notes.

    Protected fields (DRPID, source_url, datalumos_id, status, errors,
    warnings, published_url, folder_path) cannot be changed here — use
    dedicated tools for those.

    Args:
        drpid:   The project ID to update.
        fields:  Dict of field names → new values.
        dry_run: If True (default), show what would change without writing.
    """
    bad = set(fields.keys()) & _PROTECTED_FIELDS
    if bad:
        return f"Error: cannot update protected field(s): {sorted(bad)}. Use dedicated tools."

    unknown = set(fields.keys()) - _UPDATABLE_FIELDS
    if unknown:
        return (
            f"Error: unknown field(s): {sorted(unknown)}. "
            f"Updatable fields: {sorted(_UPDATABLE_FIELDS)}"
        )

    try:
        con = _connect()
    except FileNotFoundError as e:
        return f"Error: {e}"

    try:
        row = con.execute("SELECT * FROM projects WHERE DRPID = ?", (drpid,)).fetchone()
        if row is None:
            return f"Error: DRPID {drpid} not found."

        current = _row_to_dict(row)
        diff_lines = []
        for field, new_val in fields.items():
            old_val = current.get(field)
            if old_val != new_val:
                diff_lines.append(f"  {field}: {old_val!r} → {new_val!r}")

        if not diff_lines:
            return f"No changes — all provided values already match DRPID {drpid}."

        if dry_run:
            return (
                f"[DRY RUN] update_project(DRPID={drpid}):\n"
                + "\n".join(diff_lines)
                + "\n\nRun with dry_run=False to apply."
            )

        set_clauses = ", ".join(f"{k} = ?" for k in fields)
        params = list(fields.values()) + [drpid]
        con.execute(f"UPDATE projects SET {set_clauses} WHERE DRPID = ?", params)
        con.commit()
    finally:
        con.close()

    return f"Updated DRPID={drpid}:\n" + "\n".join(diff_lines)


@mcp.tool()
def clear_errors(drpid: int, dry_run: bool = True) -> str:
    """
    Clear the errors field on a project, making it eligible for re-processing.

    Args:
        drpid:   The project ID to clear errors on.
        dry_run: If True (default), show current errors without clearing.
    """
    try:
        con = _connect()
    except FileNotFoundError as e:
        return f"Error: {e}"

    try:
        row = con.execute(
            "SELECT DRPID, status, errors FROM projects WHERE DRPID = ?", (drpid,)
        ).fetchone()
        if row is None:
            return f"Error: DRPID {drpid} not found."

        current_errors = row["errors"]
        if not current_errors:
            return f"DRPID {drpid} has no errors — nothing to clear."

        if dry_run:
            return (
                f"[DRY RUN] clear_errors(DRPID={drpid}):\n"
                f"  Current errors: {current_errors!r}\n"
                f"  Status: {row['status']!r}\n"
                f"\nRun with dry_run=False to clear."
            )

        con.execute("UPDATE projects SET errors = NULL WHERE DRPID = ?", (drpid,))
        con.commit()
    finally:
        con.close()

    return f"Cleared errors on DRPID={drpid}. Project is now eligible for re-processing."


@mcp.tool()
def set_project_status(drpid: int, status: str, dry_run: bool = True) -> str:
    """
    Manually set a project's status. Shows the full current record before any change.

    Common uses: roll back to 'sourced' to re-collect, or advance/reset for testing.

    Args:
        drpid:   The project ID to update.
        status:  New status value (e.g. 'sourced', 'collected', 'uploaded').
        dry_run: If True (default), show what would change without writing.
    """
    try:
        con = _connect()
    except FileNotFoundError as e:
        return f"Error: {e}"

    try:
        row = con.execute("SELECT * FROM projects WHERE DRPID = ?", (drpid,)).fetchone()
        if row is None:
            return f"Error: DRPID {drpid} not found."

        current = _row_to_dict(row)
        old_status = current.get("status")

        if dry_run:
            return (
                f"[DRY RUN] set_project_status(DRPID={drpid}):\n"
                f"  status: {old_status!r} → {status!r}\n\n"
                f"Current record:\n{_format_project(current)}\n\n"
                f"Run with dry_run=False to apply."
            )

        con.execute("UPDATE projects SET status = ? WHERE DRPID = ?", (status, drpid))
        con.commit()
    finally:
        con.close()

    return f"Set status on DRPID={drpid}: {old_status!r} → {status!r}"


@mcp.tool()
def delete_project(drpid: int, dry_run: bool = True) -> str:
    """
    Delete a project record from the database. Does NOT delete files from disk.
    Shows the full current record before deletion.

    Args:
        drpid:   The project ID to delete.
        dry_run: If True (default), show the record without deleting.
    """
    try:
        con = _connect()
    except FileNotFoundError as e:
        return f"Error: {e}"

    try:
        row = con.execute("SELECT * FROM projects WHERE DRPID = ?", (drpid,)).fetchone()
        if row is None:
            return f"Error: DRPID {drpid} not found."

        current = _row_to_dict(row)

        if dry_run:
            return (
                f"[DRY RUN] delete_project(DRPID={drpid}):\n"
                f"Would delete this record (files on disk are NOT deleted):\n\n"
                f"{_format_project(current)}\n\n"
                f"Run with dry_run=False to delete."
            )

        con.execute("DELETE FROM projects WHERE DRPID = ?", (drpid,))
        con.commit()
    finally:
        con.close()

    return (
        f"Deleted DRPID={drpid} from database.\n"
        f"Note: files on disk were not removed."
    )


# ── Verification tools ────────────────────────────────────────────────────────

@mcp.tool()
def verify_module_run(
    module: str,
    expected_count: Optional[int] = None,
    sample_errors: int = 5,
) -> str:
    """
    After running a module, check results: how many projects reached the expected
    output status, how many have errors, and surface a sample of error messages.

    Args:
        module:         Module that was run (used to determine expected output status).
        expected_count: If provided, assert this many projects reached output status.
        sample_errors:  Number of error samples to show (default 5).
    """
    if module not in _MODULES:
        valid = ", ".join(sorted(_MODULES.keys()))
        return f"Error: unknown module {module!r}. Valid: {valid}"

    output_status = _MODULES[module]["output"]
    if output_status is None:
        return (
            f"Module {module!r} has no DB output status "
            f"(e.g. cleanup_inprogress only affects DataLumos). Cannot verify."
        )

    try:
        con = _connect()
    except FileNotFoundError as e:
        return f"Error: {e}"

    try:
        reached = con.execute(
            "SELECT COUNT(*) FROM projects WHERE status = ?", (output_status,)
        ).fetchone()[0]

        errored = con.execute(
            "SELECT COUNT(*) FROM projects WHERE errors IS NOT NULL AND errors != ''"
        ).fetchone()[0]

        error_rows = con.execute(
            "SELECT DRPID, errors FROM projects "
            "WHERE errors IS NOT NULL AND errors != '' "
            "ORDER BY DRPID ASC LIMIT ?",
            (sample_errors,),
        ).fetchall()
    finally:
        con.close()

    lines = [
        f"=== verify_module_run({module!r}) ===",
        f"Expected output status: {output_status!r}",
        f"Projects at {output_status!r}: {reached}",
        f"Projects with errors:   {errored}",
    ]

    if expected_count is not None:
        ok = "PASS" if reached >= expected_count else "FAIL"
        lines.append(f"Expected count check:   {ok} (expected >= {expected_count}, got {reached})")

    if error_rows:
        lines.append(f"\nSample errors (up to {sample_errors}):")
        for row in error_rows:
            lines.append(f"\n  DRPID={row['DRPID']}:")
            for line in (row["errors"] or "").splitlines()[:3]:
                lines.append(f"    {line}")

    return "\n".join(lines)


@mcp.tool()
def check_project_files(drpid: int) -> str:
    """
    List files in a project's output folder, with names, sizes, and extensions.
    Confirms the folder exists.

    Args:
        drpid: The project ID to inspect.
    """
    try:
        con = _connect()
    except FileNotFoundError as e:
        return f"Error: {e}"

    try:
        row = con.execute(
            "SELECT DRPID, status, folder_path FROM projects WHERE DRPID = ?", (drpid,)
        ).fetchone()
    finally:
        con.close()

    if row is None:
        return f"Error: DRPID {drpid} not found."

    folder_path = row["folder_path"]
    if not folder_path:
        return f"DRPID {drpid} has no folder_path set (status: {row['status']!r})."

    folder = Path(folder_path)
    if not folder.exists():
        return (
            f"DRPID {drpid}: folder_path is set but does not exist on disk.\n"
            f"  folder_path: {folder_path}"
        )

    files = sorted(folder.iterdir())
    data_files = [f for f in files if f.is_file()]
    total_bytes = sum(f.stat().st_size for f in data_files)

    lines = [
        f"DRPID {drpid} — {folder_path}",
        f"Status: {row['status']!r}",
        f"Files: {len(data_files)}, Total size: {_format_size(total_bytes)}",
        "",
    ]
    for f in data_files:
        size = f.stat().st_size
        lines.append(f"  {f.name}  ({_format_size(size)})  [{f.suffix or 'no ext'}]")

    if not data_files:
        lines.append("  (folder exists but is empty)")

    return "\n".join(lines)


def _format_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:,.0f} {unit}"
        n //= 1024
    return f"{n:,.0f} TB"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DRP Pipeline Orchestration MCP server")
    parser.add_argument(
        "--transport",
        choices=("stdio", "sse", "streamable-http"),
        default="stdio",
        help="Transport mode. Default 'stdio' is for Claude Code / Desktop; "
             "'sse' or 'streamable-http' serve over HTTP for clients like LibreChat.",
    )
    parser.add_argument("--host", default="0.0.0.0",
                        help="Bind address for sse/streamable-http (default 0.0.0.0).")
    parser.add_argument("--port", type=int, default=8765,
                        help="Port for sse/streamable-http (default 8765).")
    parser.add_argument("--allowed-host", action="append", default=[],
                        help="Extra allowed Host header value (repeatable). "
                             "Accepts 'host' or 'host:port' or 'host:*'.")
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run()
    else:
        # FastMCP reads host/port from its settings object
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        # Extend DNS-rebinding allowed hosts so clients like LibreChat running
        # in Docker can reach us via host.docker.internal. Localhost is already
        # allowed by FastMCP's defaults.
        extra_hosts = [f"host.docker.internal:{args.port}", "host.docker.internal:*"]
        extra_hosts.extend(args.allowed_host)
        mcp.settings.transport_security.allowed_hosts.extend(extra_hosts)
        mcp.settings.transport_security.allowed_origins.extend([
            f"http://host.docker.internal:{args.port}",
            "http://host.docker.internal:*",
        ])
        mcp.run(transport=args.transport)
