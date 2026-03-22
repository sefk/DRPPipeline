"""
Training data importer for the collector training system.

Imports human-verified ground truth from:
  1. A pre-scraped JSON file (fastest, no network)
  2. A Google Sheets CSV export (requires sheet to be published/accessible)
     + DataLumos web scraping (requires Playwright + login)
  3. A dict list passed directly by caller

The import is idempotent: re-running updates existing records rather than
creating duplicates (keyed on run_id + source_url + annotator).
"""
from __future__ import annotations

import csv
import io
import json
import re
import random
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

from collector_training.schema import get_connection, DEFAULT_DB_PATH

PROJECT_ROOT = Path(__file__).parent.parent

# Google Sheets CSV export URL template
_SHEETS_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/{sheet_id}/export"
    "?format=csv&gid={gid}"
)

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}


# ── Time period parsing ─────────────────────────────────────────────────────

def _parse_time_period(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Split a DataLumos time period string into (time_start, time_end).

    Handles formats like:
      "1/1/2016 – 12/31/2016"
      "2016"
      "2016 – 2020"
    """
    if not raw:
        return None, None
    raw = raw.strip()
    # Split on en-dash, em-dash, or hyphen surrounded by spaces
    parts = re.split(r"\s*[–—-]\s*", raw, maxsplit=1)
    start = parts[0].strip() or None
    end = parts[1].strip() if len(parts) > 1 else start
    return start, end


# ── Normalization ───────────────────────────────────────────────────────────

def _normalize_ground_truth(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a DataLumos project dict (from compare_datalumos.py scraping)
    into the ground truth schema used by the scorer.
    """
    time_start, time_end = _parse_time_period(raw.get("time_period"))

    # agency may be a list or a string; keep as-is for scorer
    agency = raw.get("agency")
    if isinstance(agency, list):
        agency = ", ".join(agency)

    # keywords may be a list; keep as-is (scorer handles both)
    keywords = raw.get("keywords")
    if isinstance(keywords, list):
        keywords = ", ".join(keywords)

    return {
        "title":               raw.get("title"),
        "summary":             raw.get("summary"),
        "agency":              agency,
        "keywords":            keywords,
        "time_start":          time_start,
        "time_end":            time_end,
        "data_types":          raw.get("data_types"),
        "files":               raw.get("files"),          # list of {"name": ...}
        "collection_notes":    raw.get("collection_notes"),
        "geographic_coverage": raw.get("geographic_coverage"),
    }


# ── Google Sheets fetch ─────────────────────────────────────────────────────

def _fetch_sheet_rows(sheet_id: str, gid: str) -> list[dict[str, str]]:
    """
    Fetch a Google Sheets tab as CSV and return list of row dicts.
    The sheet must be published or world-readable.
    """
    url = _SHEETS_CSV_URL.format(sheet_id=sheet_id, gid=gid)
    resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=30)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    return list(reader)


def _extract_datalumos_workspace_id(url: str) -> Optional[str]:
    """
    Extract the DataLumos workspace numeric ID from a URL or string.

    Handles:
      https://www.datalumos.org/datalumos/project/244264/version/V1/view
      https://www.datalumos.org/datalumos/project/238570/view
      https://www.datalumos.org/datalumos/workspace?...goToPath=/datalumos/246966
      246966   (bare ID)
    """
    if not url:
        return None
    # /project/<id>/... (newer URL format used in spreadsheet Download Location)
    m = re.search(r"/project/(\d+)", url)
    if m:
        return m.group(1)
    # workspace?goToPath=.../datalumos/<id> (older format)
    m = re.search(r"/datalumos/(\d+)", url)
    if m:
        return m.group(1)
    # bare numeric ID
    m = re.match(r"^\d+$", str(url).strip())
    if m:
        return str(url).strip()
    return None


def _scrape_datalumos_project(project_id: str) -> Optional[dict[str, Any]]:
    """
    Scrape a DataLumos public project view page.

    Uses the public /project/{id}/version/V1/view URL — no authentication
    required, works for any project regardless of ownership.
    Returns None if scraping fails.
    """
    try:
        import sys
        sys.path.insert(0, str(PROJECT_ROOT))
        from tests.compare_datalumos import read_project_public_view
        return read_project_public_view(project_id)
    except Exception as exc:
        print(f"Warning: could not scrape DataLumos project {project_id}: {exc}")
        return None


# ── Core import logic ───────────────────────────────────────────────────────

def _upsert_example(
    con: sqlite3.Connection,
    run_id: int,
    source_url: str,
    annotator: Optional[str],
    control_datalumos_id: Optional[str],
    ground_truth: dict[str, Any],
    data_size_bytes: Optional[int] = None,
) -> int:
    """
    Insert or update a training example. Returns example_id.
    """
    gt_json = json.dumps(ground_truth)
    now = datetime.now(timezone.utc).isoformat()

    row = con.execute(
        "SELECT example_id FROM training_examples "
        "WHERE run_id=? AND source_url=? AND (annotator=? OR (annotator IS NULL AND ? IS NULL))",
        (run_id, source_url, annotator, annotator),
    ).fetchone()

    if row:
        con.execute(
            "UPDATE training_examples SET ground_truth_json=?, imported_at=?, "
            "control_datalumos_id=?, data_size_bytes=? WHERE example_id=?",
            (gt_json, now, control_datalumos_id, data_size_bytes, row["example_id"]),
        )
        return row["example_id"]
    else:
        cur = con.execute(
            "INSERT INTO training_examples "
            "(run_id, source_url, annotator, control_datalumos_id, ground_truth_json, "
            "is_validation, data_size_bytes, imported_at) "
            "VALUES (?,?,?,?,?,0,?,?)",
            (run_id, source_url, annotator, control_datalumos_id, gt_json,
             data_size_bytes, now),
        )
        return cur.lastrowid


def assign_train_validation_split(
    run_id: int,
    validation_pct: float = 0.2,
    db_path: Optional[Path] = None,
    seed: int = 42,
) -> tuple[int, int]:
    """
    Randomly assign training/validation split to imported examples.
    Returns (n_train, n_validation).

    Default: 80% train / 20% validation.
    For small sets (< 10 examples), uses 1-2 examples for validation.
    """
    con = get_connection(db_path)
    try:
        rows = con.execute(
            "SELECT example_id FROM training_examples WHERE run_id=? ORDER BY example_id",
            (run_id,),
        ).fetchall()
        ids = [r["example_id"] for r in rows]
        n = len(ids)
        if n == 0:
            return 0, 0

        rng = random.Random(seed)
        rng.shuffle(ids)

        if n < 10:
            n_val = min(2, max(1, int(n * validation_pct)))
        else:
            n_val = max(1, int(n * validation_pct))

        val_ids = set(ids[:n_val])
        for eid in ids:
            con.execute(
                "UPDATE training_examples SET is_validation=? WHERE example_id=?",
                (1 if eid in val_ids else 0, eid),
            )
        con.commit()
        return n - n_val, n_val
    finally:
        con.close()


# ── Public entry points ─────────────────────────────────────────────────────

def import_from_list(
    run_id: int,
    examples: list[dict[str, Any]],
    db_path: Optional[Path] = None,
) -> int:
    """
    Import training examples from a list of dicts.

    Each dict must have 'source_url' and 'ground_truth' keys. Optional keys:
    'annotator', 'control_datalumos_id', 'data_size_bytes'.

    Returns the number of records inserted/updated.
    """
    con = get_connection(db_path)
    count = 0
    try:
        for ex in examples:
            source_url = ex.get("source_url")
            if not source_url:
                continue
            ground_truth = ex.get("ground_truth") or {}
            if not isinstance(ground_truth, dict):
                ground_truth = {}
            _upsert_example(
                con=con,
                run_id=run_id,
                source_url=source_url,
                annotator=ex.get("annotator"),
                control_datalumos_id=ex.get("control_datalumos_id"),
                ground_truth=_normalize_ground_truth(ground_truth),
                data_size_bytes=ex.get("data_size_bytes"),
            )
            count += 1
        con.commit()
    finally:
        con.close()
    return count


def import_from_json_file(
    run_id: int,
    json_path: Path,
    db_path: Optional[Path] = None,
) -> int:
    """
    Import training examples from a JSON file.

    File format: list of dicts with 'source_url', 'ground_truth', etc.
    """
    with open(json_path, encoding="utf-8") as f:
        examples = json.load(f)
    return import_from_list(run_id, examples, db_path)


def import_from_spreadsheet(
    run_id: int,
    sheet_id: str,
    sheet_gid: str,
    url_column: str = "URL",
    status_column: str = "Status",
    done_value: str = "DONE",
    download_location_column: str = "Download Location",
    annotator_column: Optional[str] = None,
    max_rows: Optional[int] = None,
    db_path: Optional[Path] = None,
    scrape_datalumos: bool = True,
) -> int:
    """
    Import training data from a Google Sheets CSV export.

    For each DONE row: scrapes the DataLumos project for ground truth.
    Requires the sheet to be world-readable and Playwright + DataLumos login
    if scrape_datalumos=True.

    Returns number of records inserted/updated.
    """
    rows = _fetch_sheet_rows(sheet_id, sheet_gid)
    count = 0

    for row in rows:
        if max_rows is not None and count >= max_rows:
            break

        status = row.get(status_column, "").strip().upper()
        if status != done_value.upper():
            continue

        source_url = row.get(url_column, "").strip()
        if not source_url:
            continue

        annotator = row.get(annotator_column, "").strip() if annotator_column else None
        dl_location = row.get(download_location_column, "").strip()
        workspace_id = _extract_datalumos_workspace_id(dl_location)

        ground_truth: dict[str, Any] = {}
        if scrape_datalumos and workspace_id:
            scraped = _scrape_datalumos_project(workspace_id)
            if scraped:
                ground_truth = _normalize_ground_truth(scraped)

        con = get_connection(db_path)
        try:
            _upsert_example(
                con=con,
                run_id=run_id,
                source_url=source_url,
                annotator=annotator or None,
                control_datalumos_id=workspace_id,
                ground_truth=ground_truth,
            )
            con.commit()
        finally:
            con.close()

        count += 1
        print(f"  Imported {count}: {source_url}")

    return count


def list_examples(
    run_id: int,
    include_validation: bool = True,
    db_path: Optional[Path] = None,
) -> list[dict[str, Any]]:
    """
    Return all training examples for a run as dicts with parsed ground_truth_json.
    """
    con = get_connection(db_path)
    try:
        query = "SELECT * FROM training_examples WHERE run_id=?"
        if not include_validation:
            query += " AND is_validation=0"
        query += " ORDER BY example_id"
        rows = con.execute(query, (run_id,)).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            if d.get("ground_truth_json"):
                d["ground_truth"] = json.loads(d["ground_truth_json"])
            result.append(d)
        return result
    finally:
        con.close()
