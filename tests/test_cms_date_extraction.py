#!/usr/bin/env python3
"""
Verify CMS date extraction through the full collector pipeline.

Uses the orchestration MCP (mcp_server.server) to run cms_collector on a
project and verify that time_start and time_end are set in the DB record.

Unlike the original version which tested CmsGovCollector._extract_date_range
directly, this tests the full collector path end-to-end: the project must be
at status "sourced", cms_collector runs via subprocess (same path as production),
and the result is read back from the database.

Usage (from repo root):
    python tests/test_cms_date_extraction.py --drpid 12
    python tests/test_cms_date_extraction.py --url https://data.cms.gov/...
    python tests/test_cms_date_extraction.py --drpid 1 --rerun
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from mcp_server.server import _connect, clear_errors, get_project, run_module, set_project_status


def find_drpid_for_url(url: str) -> int | None:
    """Return the DRPID for the given source_url, or None if not found."""
    try:
        con = _connect()
        row = con.execute(
            "SELECT DRPID FROM projects WHERE source_url = ?", (url,)
        ).fetchone()
        con.close()
        return row["DRPID"] if row else None
    except Exception as e:
        print(f"  Error looking up URL: {e}")
        return None


def _read_date_fields(drpid: int) -> dict | None:
    """Read status, time_start, time_end, errors for a project."""
    try:
        con = _connect()
        row = con.execute(
            "SELECT status, time_start, time_end, errors FROM projects WHERE DRPID = ?",
            (drpid,),
        ).fetchone()
        con.close()
        return dict(row) if row else None
    except Exception as e:
        print(f"  Error reading project: {e}")
        return None


def check_drpid(drpid: int, rerun: bool = False) -> bool:
    """
    Run cms_collector on a project and verify time_start/time_end are set.

    If the project is already "collected" and --rerun is not set, reports the
    stored dates without re-running. With --rerun, rolls back to "sourced" first.

    Returns True if time_start is present in the DB record after collection.
    """
    print(f"\n=== test_cms_date_extraction: DRPID={drpid} ===\n")
    print(get_project(drpid))

    row = _read_date_fields(drpid)
    if row is None:
        print(f"\nError: DRPID {drpid} not found in database.")
        return False

    status = row["status"]

    # Already collected and no rerun requested — just report stored dates
    if status == "collected" and not rerun:
        print(f"\nAlready collected.")
        print(f"  time_start : {row['time_start']!r}")
        print(f"  time_end   : {row['time_end']!r}")
        found = bool(row["time_start"])
        print(f"\n  {'OK — dates extracted' if found else 'MISSING — use --rerun to re-collect'}")
        return found

    # Roll back if collected+rerun, or stuck in error
    if status in ("collected", "error") or (status == "sourced" and row["errors"]):
        print(f"\nRolling back DRPID={drpid} to 'sourced'...")
        print(clear_errors(drpid, dry_run=False))
        print(set_project_status(drpid, "sourced", dry_run=False))
        status = "sourced"

    if status != "sourced":
        print(f"\nCannot run collector: status={status!r} (need 'sourced').")
        print("Use --rerun to roll back a collected project, or source this URL first.")
        return False

    # Run the full collector via the orchestration MCP
    print(f"\nRunning cms_collector via run_module()...")
    print(run_module("cms_collector", dry_run=False, start_drpid=drpid, num_rows=1))

    # Read result
    row = _read_date_fields(drpid)
    if row is None:
        print("Error: could not read updated project record.")
        return False

    print(f"\nResult:")
    print(f"  status     : {row['status']!r}")
    print(f"  time_start : {row['time_start']!r}")
    print(f"  time_end   : {row['time_end']!r}")
    if row["errors"]:
        print(f"  errors     : {row['errors']!r}")

    found = bool(row["time_start"])
    print(f"\n  {'OK — dates extracted' if found else 'MISSING — date extraction failed'}")
    return found


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Test CMS date extraction through the full collector pipeline"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--drpid", type=int, help="DRPID of a project in the DB")
    group.add_argument("--url", help="CMS dataset URL (must already be in the DB)")
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="Roll back to 'sourced' and re-collect even if already collected",
    )
    args = parser.parse_args()

    drpid = args.drpid
    if drpid is None:
        drpid = find_drpid_for_url(args.url)
        if drpid is None:
            print(f"URL not found in database: {args.url}")
            print("Run preview_sourcing() then run_module('sourcing') to add it first.")
            sys.exit(1)

    ok = check_drpid(drpid, rerun=args.rerun)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
