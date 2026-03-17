#!/usr/bin/env python3
"""
End-to-end pipeline test for a single project.

Runs the full pipeline (sourcing → cms_collector → upload) for one project
from the DRP spreadsheet, then compares the newly created DataLumos project
(treatment) against the previously created one recorded in the spreadsheet
(control, from column L "Download Location").

Usage (from repo root):
    python tests/e2e_test.py
    python tests/e2e_test.py --title "Value Modifier"
    python tests/e2e_test.py --skip-collect  # if already collected
    python tests/e2e_test.py --skip-upload   # if already uploaded

Results:
    GREEN  - treatment matches control closely
    YELLOW - some differences (missing data or metadata differences)
    RED    - pipeline failure (data missing or significantly different)
"""

import argparse
import csv
import io
import re
import sys
from pathlib import Path
from typing import Optional
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Initialize Args before importing pipeline modules
_argv_backup = sys.argv[:]
sys.argv = [sys.argv[0], "cms_collector"]
from utils.Args import Args
from utils.Logger import Logger
Args.initialize()
sys.argv = _argv_backup
Logger.initialize(log_level="INFO")

from storage import Storage
from utils.sheet_url_utils import get_gid_for_sheet_name


# ── ANSI colors ───────────────────────────────────────────────────────────────

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ── Phase 1: Sourcing ─────────────────────────────────────────────────────────

def phase_sourcing(title_filter: str) -> tuple[int, dict]:
    """
    Find the project row in the spreadsheet and ensure a Storage record exists.

    Returns (drpid, sheet_row) where sheet_row has all spreadsheet columns.
    Raises ValueError if no matching row is found.
    """
    print(f"\n{BOLD}Phase 1: Sourcing{RESET}")

    sheet_id = Args.google_sheet_id
    sheet_name = Args.google_sheet_name
    creds_path = Path(Args.google_credentials)

    gid = get_gid_for_sheet_name(sheet_id, sheet_name, creds_path)
    if gid is None:
        raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet")

    export_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    )
    req = Request(export_url, headers={"User-Agent": "DRPPipeline/1.0"})
    with urlopen(req, timeout=30) as resp:
        csv_text = resp.read().decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    matches = [r for r in rows if title_filter.lower() in (r.get("Title of Site") or "").lower()]

    if not matches:
        raise ValueError(f"No row found with title matching '{title_filter}'")
    if len(matches) > 1:
        titles = [r.get("Title of Site") for r in matches]
        raise ValueError(f"Multiple rows match '{title_filter}': {titles}")

    sheet_row = matches[0]
    source_url = (sheet_row.get("URL") or "").strip()
    if not source_url:
        raise ValueError(f"Row for '{title_filter}' has no URL in column G")

    print(f"  Found: {sheet_row.get('Title of Site')}")
    print(f"  Source URL: {source_url}")
    control_url = (sheet_row.get("Download Location") or "").strip()
    if control_url:
        print(f"  Control (col L): {control_url}")
    else:
        print(f"  Control: (none — no Download Location in spreadsheet)")

    # Re-use existing record if source_url already in Storage
    Storage.initialize(Args.storage_implementation, db_path=Path(Args.db_path))
    existing = _find_by_source_url(source_url)
    if existing:
        drpid = existing["DRPID"]
        print(f"  Re-using existing record DRPID={drpid} (status={existing.get('status')})")
    else:
        drpid = Storage.create_record(source_url)
        Storage.update_record(drpid, {
            "status": "sourced",
            "office": (sheet_row.get("Office") or "").strip(),
            "agency": (sheet_row.get("Agency") or "").strip(),
        })
        print(f"  Created new record DRPID={drpid}")

    return drpid, sheet_row


def _find_by_source_url(url: str) -> Optional[dict]:
    """Return project dict if source_url already exists in Storage, else None."""
    import sqlite3
    db_path = Path(Args.db_path)
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM projects WHERE source_url = ?", (url,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as exc:
        Logger.warning(f"Could not query storage for existing record: {exc}")
        return None


# ── Phase 2: Collection ───────────────────────────────────────────────────────

def phase_collect(drpid: int) -> None:
    """Run CmsGovCollector to download data for the project."""
    print(f"\n{BOLD}Phase 2: Collection{RESET}")

    project = Storage.get(drpid)
    if project and project.get("status") == "collected" and project.get("folder_path"):
        folder = Path(project["folder_path"])
        if folder.exists():
            files = list(folder.iterdir())
            print(f"  Already collected: {folder} ({len(files)} files)")
            return

    print(f"  Running CmsGovCollector for DRPID={drpid}...")
    from collectors.CmsGovCollector import CmsGovCollector
    collector = CmsGovCollector(headless=True)
    collector.run(drpid)

    project = Storage.get(drpid)
    status = project.get("status") if project else "unknown"
    folder = project.get("folder_path") if project else None
    if status == "collected" and folder:
        files = list(Path(folder).iterdir())
        print(f"  Collected: {folder} ({len(files)} files, status={status})")
    else:
        errors = project.get("errors") if project else "unknown"
        raise RuntimeError(f"Collection failed (status={status}): {errors}")


# ── Phase 3: Upload ───────────────────────────────────────────────────────────

def phase_upload(drpid: int) -> str:
    """Upload to DataLumos and return the workspace ID."""
    print(f"\n{BOLD}Phase 3: Upload{RESET}")

    project = Storage.get(drpid)
    if project and project.get("status") == "uploaded" and project.get("datalumos_id"):
        workspace_id = project["datalumos_id"]
        print(f"  Already uploaded: workspace_id={workspace_id}")
        return workspace_id

    # If a previous attempt partially succeeded (workspace created but error during form fill),
    # reset status to "collected" so the uploader will try again with a fresh workspace.
    if project and project.get("status") == "error":
        prior_ws = project.get("datalumos_id")
        if prior_ws:
            print(f"  Previous attempt created workspace {prior_ws} but failed; retrying upload...")
        else:
            print(f"  Previous upload attempt failed; retrying...")
        Storage.update_record(drpid, {"status": "collected", "errors": None, "datalumos_id": None})

    print(f"  Running DataLumosUploader for DRPID={drpid}...")
    from upload.DataLumosUploader import DataLumosUploader
    uploader = DataLumosUploader()
    uploader.run(drpid)

    project = Storage.get(drpid)
    workspace_id = project.get("datalumos_id") if project else None
    if not workspace_id:
        errors = project.get("errors") if project else "unknown"
        raise RuntimeError(f"Upload failed — no datalumos_id set: {errors}")

    print(f"  Uploaded: workspace_id={workspace_id}")
    return workspace_id


# ── Phase 4: Pre-checks (Storage / local) ────────────────────────────────────

def phase_prechecks(drpid: int, workspace_id: str) -> list[tuple[str, str, str]]:
    """
    Quick sanity checks against the local Storage record before running the
    full DataLumos comparison.  Returns list of (name, status, detail) tuples.
    """
    print(f"\n{BOLD}Phase 4a: Pre-checks{RESET}")
    project = Storage.get(drpid)
    checks = []

    # Files downloaded locally
    folder = (project or {}).get("folder_path")
    if folder and Path(folder).exists():
        files = [f for f in Path(folder).iterdir() if f.is_file()]
        checks.append(("Files downloaded", "OK", f"{len(files)} files in {folder}"))
    else:
        checks.append(("Files downloaded", "FAIL", f"No local files ({folder or 'no folder'})"))

    # DataLumos project created
    if workspace_id:
        checks.append(("DataLumos project created", "OK",
                        f"workspace {workspace_id}"))
    else:
        checks.append(("DataLumos project created", "FAIL", "No workspace_id"))

    for name, status, detail in checks:
        icon = {"OK": "✓", "FAIL": "✗"}.get(status, "~")
        color = {"OK": GREEN, "FAIL": RED}.get(status, YELLOW)
        print(f"  {color}{icon} [{status}]{RESET}  {name}: {detail}")

    return checks


# ── Phase 5: Full DataLumos comparison ───────────────────────────────────────

def phase_compare(workspace_id: str, control_id: str) -> tuple[dict, dict, list]:
    """
    Use an authenticated browser to read both DataLumos projects and compare
    every field.  Returns (treatment_data, control_data, checks).
    """
    print(f"\n{BOLD}Phase 4b: DataLumos comparison{RESET}")
    from tests.compare_datalumos import run_comparison
    return run_comparison(workspace_id, control_id)


# ── Phase 6: Report ───────────────────────────────────────────────────────────

def phase_report(prechecks: list, treatment: dict, control: dict,
                 checks: list, workspace_id: str, control_url: Optional[str]) -> str:
    """Print the combined report and return overall rating."""
    from tests.compare_datalumos import print_report, overall_rating

    # Pre-checks failures are reported inline in phase_prechecks;
    # roll any FAIL pre-checks into the main checks list so they affect the rating.
    all_checks = list(prechecks) + list(checks)

    print(f"\n{BOLD}Phase 5: Results{RESET}")
    print_report(treatment, control, checks)

    # If pre-checks failed, downgrade overall
    rating = overall_rating(all_checks)
    color = {"GREEN": GREEN, "YELLOW": YELLOW, "RED": RED}.get(rating, "")
    print(f"  {BOLD}(including pre-checks: {color}{rating}{RESET}{BOLD}){RESET}")
    if workspace_id:
        print(f"\n  Treatment: https://www.datalumos.org/datalumos/workspace"
              f"?goToLevel=project&goToPath=/datalumos/{workspace_id}#")
    if control_url:
        print(f"  Control:   {control_url}")
    print()
    return rating


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end pipeline test for one project")
    parser.add_argument(
        "--title", default="Value Modifier",
        help="Title of Site to search for (default: 'Value Modifier')"
    )
    parser.add_argument(
        "--skip-collect", action="store_true",
        help="Skip collection phase (use if already collected)"
    )
    parser.add_argument(
        "--skip-upload", action="store_true",
        help="Skip upload phase (use if already uploaded)"
    )
    args = parser.parse_args()

    print(f"{BOLD}DRP Pipeline End-to-End Test{RESET}")
    print(f"Project: {args.title!r}")

    try:
        drpid, sheet_row = phase_sourcing(args.title)

        if not args.skip_collect:
            phase_collect(drpid)
        else:
            print(f"\n{BOLD}Phase 2: Collection{RESET} (skipped)")

        workspace_id = None
        if not args.skip_upload:
            workspace_id = phase_upload(drpid)
        else:
            print(f"\n{BOLD}Phase 3: Upload{RESET} (skipped)")
            project = Storage.get(drpid)
            workspace_id = (project or {}).get("datalumos_id")

        control_url = (sheet_row.get("Download Location") or "").strip() or None
        control_id = re.search(r"/project/(\d+)", control_url).group(1) if control_url else None

        prechecks = phase_prechecks(drpid, workspace_id or "")

        if workspace_id and control_id:
            treatment_data, control_data, dl_checks = phase_compare(workspace_id, control_id)
        else:
            print(f"\n{BOLD}Phase 4b: DataLumos comparison{RESET} (skipped — "
                  f"{'no workspace_id' if not workspace_id else 'no control URL'})")
            treatment_data, control_data, dl_checks = {}, {}, []

        rating = phase_report(prechecks, treatment_data, control_data, dl_checks,
                              workspace_id or "", control_url)

        if rating == "RED":
            sys.exit(1)

    except Exception as exc:
        print(f"\n{RED}{BOLD}FATAL ERROR: {exc}{RESET}")
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
