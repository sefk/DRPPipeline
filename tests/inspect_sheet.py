#!/usr/bin/env python3
"""
Inspect rows from the configured Google Sheet.

Prints column names and row values for a given row number or title match.
Useful for understanding spreadsheet structure and finding project details.

Usage (from repo root):
    python tests/inspect_sheet.py --row 13
    python tests/inspect_sheet.py --title "Value Modifier"
    python tests/inspect_sheet.py --all   # print all rows
"""

import argparse
import csv
import io
import sys
from pathlib import Path
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.Args import Args
from utils.Logger import Logger
from utils.sheet_url_utils import get_gid_for_sheet_name

# Args.initialize() requires a module name as argv[1]; use 'noop' for tools
_argv_backup = sys.argv[:]
sys.argv = [sys.argv[0], "noop"]
Args.initialize()
sys.argv = _argv_backup
Logger.initialize(log_level="WARNING")


def fetch_sheet_csv() -> tuple[list[str], list[dict]]:
    """Fetch the configured Google Sheet and return (fieldnames, rows)."""
    sheet_id = Args.google_sheet_id
    sheet_name = Args.google_sheet_name
    creds_path = Path(Args.google_credentials)

    gid = get_gid_for_sheet_name(sheet_id, sheet_name, creds_path)
    if gid is None:
        print(f"ERROR: Sheet '{sheet_name}' not found in spreadsheet.")
        sys.exit(1)

    export_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    )
    req = Request(export_url, headers={"User-Agent": "DRPPipeline/1.0"})
    with urlopen(req, timeout=30) as resp:
        csv_text = resp.read().decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    fieldnames = list(reader.fieldnames or [])
    return fieldnames, rows


def col_letter(idx: int) -> str:
    """Convert 0-based column index to spreadsheet letter (A, B, ..., Z, AA, ...)."""
    letters = ""
    idx += 1  # 1-based
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def print_row(row_num: int, row: dict, fieldnames: list[str]) -> None:
    """Print a single row with column letter, name, and value."""
    print(f"\n{'='*60}")
    print(f"Row {row_num} (spreadsheet row {row_num + 1}, including header row)")
    print(f"{'='*60}")
    for idx, col in enumerate(fieldnames):
        val = row.get(col, "")
        if val or col.strip():  # skip blank column headers with blank values
            letter = col_letter(idx)
            display_col = col if col.strip() else f"(blank col {idx+1})"
            print(f"  {letter:3s}  {display_col:<35s}  {val!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect rows from the DRP Google Sheet")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--row", type=int, help="1-based row number in the sheet (not counting header)")
    group.add_argument("--title", type=str, help="Filter by 'Title of Site' column (case-insensitive substring)")
    group.add_argument("--all", action="store_true", dest="all_rows", help="Print all rows")
    args = parser.parse_args()

    print(f"Fetching sheet '{Args.google_sheet_name}' from spreadsheet {Args.google_sheet_id}...")
    fieldnames, rows = fetch_sheet_csv()
    print(f"Found {len(rows)} data rows, {len(fieldnames)} columns.")
    print(f"\nColumn headers:")
    for idx, col in enumerate(fieldnames):
        if col.strip():
            print(f"  {col_letter(idx):3s}  {col}")

    if args.all_rows:
        for i, row in enumerate(rows, start=1):
            print_row(i, row, fieldnames)
    elif args.row is not None:
        idx = args.row - 1
        if idx < 0 or idx >= len(rows):
            print(f"ERROR: Row {args.row} out of range (1-{len(rows)})")
            sys.exit(1)
        print_row(args.row, rows[idx], fieldnames)
    elif args.title:
        matches = [
            (i + 1, row) for i, row in enumerate(rows)
            if args.title.lower() in (row.get("Title of Site") or "").lower()
        ]
        if not matches:
            print(f"No rows found with title matching '{args.title}'")
            sys.exit(1)
        for row_num, row in matches:
            print_row(row_num, row, fieldnames)


if __name__ == "__main__":
    main()
