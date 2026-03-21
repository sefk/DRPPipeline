"""
Tally claimed columns across every tab in the configured Google Sheet.

For each worksheet, finds columns where row 1 or row 2 includes the whole word
\"claimed\" (case-insensitive; e.g. \"Person who claimed\", \"Claimed (add your name)\").
\"Unclaimed\" / \"disclaimed\" are ignored. Counts non-empty cells below the header row(s),
aggregates across all tabs. Prints URL-but-unclaimed row counts by tab (using
sourcing_url_column), sheets missing that URL header, then total entries, unique claimants,
sheets with no claimed header, and \"count<TAB>name\" sorted by count descending.

Run from repo root:

    python debug/tally_claimed_all_tabs.py

Uses config.json: google_sheet_id, google_credentials, and sourcing_url_column (default ``URL``)
for the \"URL filled but all Claimed columns empty\" per-tab tally. Fetches the workbook once as XLSX.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.Args import Args
from utils.sheet_claimed_tally import ClaimedTallyReport, tally_claimed_across_tabs


def _print_report(result: ClaimedTallyReport) -> None:
    print(f"Total claimed entries (non-empty cells): {result.total_claimed_entries}")
    print(f"Unique claimants: {result.unique_claimant_count}")
    print()
    if result.url_column_name:
        print(
            f'Rows with "{result.url_column_name}" filled but all "claimed" header columns empty (by tab):'
        )
        if result.unclaimed_url_rows_by_sheet:
            for title, n in result.unclaimed_url_rows_by_sheet:
                print(f"  {n}\t{title}")
        else:
            print("  (no tabs with both a URL column and a claimed header column)")
        print()
        print(
            f'Sheets with a claimed column but no "{result.url_column_name}" header in rows 1 or 2:'
        )
        if result.sheets_without_url_column:
            for title in result.sheets_without_url_column:
                print(f"  {title}")
        else:
            print("  (none)")
        print()
    print('Sheets with no column header in rows 1 or 2 containing the word "claimed":')
    if result.sheets_without_claimed_column:
        for title in result.sheets_without_claimed_column:
            print(f"  {title}")
    else:
        print("  (none)")
    print()
    print("By claimant (count descending):")
    if not result.tally:
        print('  (no non-empty cells in any "claimed" header column)')
    else:
        for name, count in result.tally.most_common():
            print(f"{count}\t{name}")


def main() -> None:
    Args.initialize()
    sheet_id = (getattr(Args, "google_sheet_id", None) or "").strip()
    creds_raw = getattr(Args, "google_credentials", None)
    if not sheet_id or not creds_raw:
        print("Set google_sheet_id and google_credentials in config.", file=sys.stderr)
        sys.exit(1)
    creds_path = Path(creds_raw)
    if not creds_path.is_file():
        print(f"Credentials file not found: {creds_path}", file=sys.stderr)
        sys.exit(1)

    url_col = (getattr(Args, "sourcing_url_column", None) or "URL").strip()
    result = tally_claimed_across_tabs(
        sheet_id, creds_path, url_column_name=url_col if url_col else None
    )
    _print_report(result)


if __name__ == "__main__":
    main()
