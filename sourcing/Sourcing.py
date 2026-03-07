"""
Sourcing module for DRP Pipeline.

Obtains candidate source URLs from configured sources (e.g. DRP Data_Inventories
spreadsheet), performs duplicate and availability checks, and creates storage
records. Duplicate-in-storage is not created; an Error is logged. Other outcomes
create a row with status: dupe_in_DL, not_found (URL 404 or equivalent), sourced (good), or error.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, List, Tuple

from storage import Storage
from .SpreadsheetCandidateFetcher import SpreadsheetCandidateFetcher

if TYPE_CHECKING:
    from storage.StorageProtocol import StorageProtocol


def _check_one(
    item: Tuple[int, str, str, str], timeout: int
) -> Tuple[int, str, str, str, int, str, Any, bool, Any]:
    """Fetch URL and return (new_drpid, url, office, agency, status_code, body, content_type, is_logical_404, exception)."""
    from utils.url_utils import fetch_page_body

    new_drpid, url, office, agency = item
    try:
        status_code, body, content_type, is_logical_404 = fetch_page_body(url, timeout=timeout)
        return (new_drpid, url, office, agency, status_code, body, content_type, is_logical_404, None)
    except Exception as e:
        return (new_drpid, url, office, agency, -1, "", None, False, e)


class Sourcing:
    """
    Orchestrates sourcing of candidate URLs: fetch from configured sources,
    check duplicates and availability, create storage records.
    """

    def __init__(self) -> None:
        """
        Initialize Sourcing. 
        """
        # Storage methods are accessed directly on the Storage class, no need to store instance
        pass

    def run(self, drpid: int) -> None:
        """
        Process configured sources: obtain candidate URLs, create storage records.
        Duplicate URL already in storage: no row created, Error logged. Other
        outcomes create a row with status: dupe_in_DL, not_found (404 or equivalent), sourced (good), or error.

        Args:
            drpid: DRPID of project to process. Use -1 for sourcing (no specific project).
        """
        from utils.Args import Args
        from utils.Logger import Logger
        from duplicate_checking import DuplicateChecker

        num_rows = Args.num_rows
        rows, skipped_count = self.get_candidate_urls(limit=num_rows)
        timeout = int(getattr(Args, "sourcing_fetch_timeout", 15) or 15)
        workers = max(1, int(Args.max_workers or 1))

        successfully_added = 0
        dupes_in_storage = 0
        dupes_in_datalumos = 0
        not_found_count = 0
        error_count = 0
        assigned_ids: List[int] = []
        checker = DuplicateChecker()
        to_fetch: List[Tuple[int, str, str, str]] = []

        for row in rows:
            url = row["url"]
            office = row.get("office", "")
            agency = row.get("agency", "")

            if checker.exists_in_storage(url):
                dupes_in_storage += 1
                Logger.error(f"Duplicate source URL already in storage, skipping (no row created): {url}")
                continue

            new_drpid = Storage.create_record(url)
            assigned_ids.append(new_drpid)

            # Check datalumos (turned off for now; structure ready)
            if False:  # checker.exists_in_datalumos(url):
                status = "dupe_in_DL"
                dupes_in_datalumos += 1
                Storage.update_record(new_drpid, {"status": status, "office": office, "agency": agency})
                continue

            to_fetch.append((new_drpid, url, office, agency))

        total = len(to_fetch)
        if total > 0:
            Logger.info(
                f"Sourcing: checking availability for {total} URLs (max_workers={workers}, timeout={timeout}s)"
            )

        # Check URL availability (parallel or sequential) with shorter timeout to avoid long delays
        results: List[Tuple[int, str, str, str, int, str, Any, bool, Any]] = []
        if workers <= 1:
            for i, item in enumerate(to_fetch, 1):
                results.append(_check_one(item, timeout))
                if total <= 20 or i % 10 == 0 or i == total:
                    Logger.info(f"Sourcing progress: {i}/{total} URLs checked")
        else:
            completed = 0
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(_check_one, item, timeout) for item in to_fetch]
                for future in as_completed(futures):
                    results.append(future.result())
                    completed += 1
                    if total <= 20 or completed % 10 == 0 or completed == total:
                        Logger.info(f"Sourcing progress: {completed}/{total} URLs checked")

        for result in results:
            new_drpid, url, office, agency, status_code, _body, _ct, _logical_404, exc = result
            if exc is not None:
                error_count += 1
                Storage.update_record(
                    new_drpid,
                    {"status": "error", "office": office, "agency": agency, "errors": str(exc)},
                )
            elif status_code == 404:
                not_found_count += 1
                Storage.update_record(
                    new_drpid,
                    {"status": "not_found", "office": office, "agency": agency},
                )
            else:
                successfully_added += 1
                Storage.update_record(
                    new_drpid,
                    {"status": "sourced", "office": office, "agency": agency},
                )

        id_range_str = ""
        if assigned_ids:
            min_id = min(assigned_ids)
            max_id = max(assigned_ids)
            id_range_str = f" (DRPID: {min_id})" if min_id == max_id else f" (DRPIDs: {min_id}-{max_id})"

        Logger.info(
            f"Sourcing complete: {successfully_added} good (sourcing){id_range_str}, "
            f"{dupes_in_storage} dupe_in_storage (skipped, no row), {dupes_in_datalumos} dupe_in_DL, "
            f"{not_found_count} not_found, {error_count} errors, {skipped_count} skipped by filtering"
        )

    def get_candidate_urls(self, limit: int | None = None) -> tuple[list[dict[str, str]], int]:
        """
        Obtain candidate source URLs and Office/Agency from the configured spreadsheet.

        Delegates to SpreadsheetCandidateFetcher. Limit from orchestrator.

        Returns:
            Tuple of (list of dicts with keys url, office, agency; count of skipped rows)
        """
        fetcher = SpreadsheetCandidateFetcher()
        return fetcher.get_candidate_urls(limit=limit)
