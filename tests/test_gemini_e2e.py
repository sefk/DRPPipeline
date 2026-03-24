#!/usr/bin/env python3
"""
End-to-end smoke test: DataLumos scraper + Gemini 2.5 Flash refinement.

Tests:
  1. read_project_public_view() — Playwright headless scrape of a known DataLumos project
  2. SimpleRefiner("gemini-2.5-flash").refine() — Gemini API call with current collector code

Usage (from repo root):
    python tests/test_gemini_e2e.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Initialize Args before importing pipeline modules
_argv_backup = sys.argv[:]
sys.argv = [sys.argv[0], "cms_collector"]
from utils.Args import Args
from utils.Logger import Logger
Args.initialize()
sys.argv = _argv_backup
Logger.initialize(log_level="WARNING")

BOLD  = "\033[1m"
GREEN = "\033[92m"
RED   = "\033[91m"
RESET = "\033[0m"

# Known DataLumos project with good ground truth (from run 10 training examples)
TEST_PROJECT_ID = "227659"

COLLECTOR_FILE = Path(__file__).resolve().parents[1] / "collectors" / "CmsGovCollector.py"


def test_datalumos_scraper() -> dict:
    """Scrape a known DataLumos public project view page."""
    print(f"\n{BOLD}Step 1: DataLumos scraper (Playwright){RESET}")
    print(f"  Project ID: {TEST_PROJECT_ID}")

    from tests.compare_datalumos import read_project_public_view
    data = read_project_public_view(TEST_PROJECT_ID)

    title = data.get("title") or "(none)"
    files = data.get("files") or []
    agency = data.get("agency") or "(none)"

    print(f"  Title:  {title[:80]}")
    print(f"  Agency: {agency[:80]}")
    print(f"  Files:  {len(files)} file(s)")
    if files:
        for f in files[:3]:
            print(f"    - {f.get('name', '?')}")

    assert title and title != "(none)", "Scraper returned no title"
    print(f"  {GREEN}PASS{RESET}")
    return data


def test_gemini_refiner() -> None:
    """Call SimpleRefiner with gemini-2.5-flash on the current collector."""
    print(f"\n{BOLD}Step 2: Gemini 2.5 Flash refiner{RESET}")
    print(f"  Model: gemini-2.5-flash")

    from collector_training.trainer import SimpleRefiner

    collector_code = COLLECTOR_FILE.read_text(encoding="utf-8")
    print(f"  Collector: {len(collector_code):,} chars")

    refiner = SimpleRefiner("gemini-2.5-flash")

    # Minimal inputs — just enough to form a valid prompt
    improved_code, usage = refiner.refine(
        collector_code=collector_code,
        aggregate_score=0.753,
        worst_cases=[],
        field_breakdown={"files": 0.44, "title": 0.62, "summary": 0.60,
                         "agency": 0.62, "keywords": 0.02},
        interface_spec="(smoke test — no interface spec)",
        refinement_strategy="Smoke test: return the code unchanged.",
    )

    print(f"  Input tokens:  {usage['input_tokens']:,}")
    print(f"  Output tokens: {usage['output_tokens']:,}")
    print(f"  Cost:          ${usage['cost_usd']:.4f}")
    print(f"  Output length: {len(improved_code):,} chars")

    assert improved_code, "Refiner returned empty code"
    assert usage["output_tokens"] > 0, "No output tokens recorded"
    print(f"  {GREEN}PASS{RESET}")


def main() -> None:
    print(f"{BOLD}DRP — DataLumos scraper + Gemini 2.5 Flash E2E smoke test{RESET}")
    errors = []

    try:
        test_datalumos_scraper()
    except Exception as exc:
        print(f"  {RED}FAIL: {exc}{RESET}")
        import traceback; traceback.print_exc()
        errors.append(f"DataLumos scraper: {exc}")

    try:
        test_gemini_refiner()
    except Exception as exc:
        print(f"  {RED}FAIL: {exc}{RESET}")
        import traceback; traceback.print_exc()
        errors.append(f"Gemini refiner: {exc}")

    print()
    if errors:
        print(f"{RED}{BOLD}FAILED ({len(errors)} error(s)):{RESET}")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print(f"{GREEN}{BOLD}All checks passed.{RESET}")


if __name__ == "__main__":
    main()
