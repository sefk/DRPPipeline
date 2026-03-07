"""
API module for save operations (metadata + PDF generation).

Serves: POST /api/save. Updates project metadata in Storage and optionally
generates PDFs for checked scoreboard pages via Playwright (headless Chromium).
Streams progress as SAVING, DONE, ERROR lines. Yields comment lines (#) as
heartbeats during long operations so the connection is not dropped by timeouts.
"""

import json
import os
import queue
import threading
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

from utils.file_utils import folder_extensions_and_size, format_file_size, sanitize_filename
from utils.url_utils import is_valid_url

from interactive_collector.pdf_utils import page_title_or_h1, unique_pdf_basename


def save_metadata(
    drpid: int,
    folder_path_str: str,
    title: str,
    summary: str,
    keywords: str,
    agency: str,
    office: str,
    time_start: str,
    time_end: str,
    download_date: str,
    status_notes: Optional[str] = None,
    status_override: Optional[str] = None,
) -> None:
    """
    Update the project record in Storage with metadata and folder stats (extensions, file_size).

    Args:
        drpid: Project ID.
        folder_path_str: Path to output folder.
        title, summary, keywords, agency, office: Metadata fields.
        time_start, time_end: Date range.
        download_date: When data was downloaded.
        status_override: If set, use instead of default "collected" (e.g. "collector_hold - reason").
    """
    from interactive_collector.collector_state import get_db_path

    # Ensure Storage is initialized (reuse projects module logic).
    try:
        from storage import Storage
        Storage.list_eligible_projects(None, 0)
    except RuntimeError:
        try:
            from utils.Logger import Logger
            if not getattr(Logger, "_initialized", False):
                Logger.initialize(log_level="WARNING")
        except Exception:
            pass
        from storage import Storage
        Storage.initialize("StorageSQLLite", db_path=get_db_path())

    from storage import Storage
    values: Dict[str, Any] = {
        "status": (status_override or "collected").strip() or "collected",
        "errors": None,
        "title": title,
        "status_notes": (status_notes or "").strip() or None,
        "agency": agency,
        "office": office,
        "summary": summary,
        "keywords": keywords,
        "time_start": time_start,
        "time_end": time_end,
        "download_date": download_date,
    }
    if folder_path_str:
        values["folder_path"] = folder_path_str
        folder_path = Path(folder_path_str)
        if folder_path.is_dir():
            exts_list, total_bytes = folder_extensions_and_size(folder_path)
            values["extensions"] = ", ".join(exts_list) if exts_list else ""
            values["file_size"] = format_file_size(total_bytes)
        else:
            values["extensions"] = ""
            values["file_size"] = ""
    else:
        values["extensions"] = ""
        values["file_size"] = ""
    try:
        Storage.update_record(drpid, values)
    except ValueError as e:
        from utils.Logger import Logger
        Logger.warning(f"save_metadata: record not found or invalid for DRPID={drpid}: {e}")


# PDF generation: use "commit" so we only wait for the navigation to commit (response received).
# Some pages never fire domcontentloaded in headless (e.g. FDA REMS); commit avoids that.
# Default is headed (visible browser); set DRP_PDF_HEADLESS=1 to run headless.
_PDF_NAVIGATION_WAIT = "commit"
_PDF_NAVIGATION_TIMEOUT_MS = 30000  # commit is fast; this covers slow servers only
_PDF_SETTLE_MS = 5000  # initial pause after commit
_PDF_WAIT_FOR_CONTENT_MS = 35000  # wait for load event; then print


def _pdf_headless() -> bool:
    """False = headed (default, visible browser); True only when DRP_PDF_HEADLESS=1 (or true, etc.)."""
    v = (os.environ.get("DRP_PDF_HEADLESS") or "").strip().lower()
    return v in ("1", "true", "yes", "on")
_PDF_PRINT_TIMEOUT_MS = 90000
_HEARTBEAT_INTERVAL = 15.0  # seconds; yield a comment line so connection is not dropped


def _run_pdf_worker(
    folder_path: Path,
    urls: List[str],
    indices: List[str],
    progress_queue: "queue.Queue[Tuple[str, ...]]",
) -> None:
    """Run Playwright PDF generation and put progress items on the queue."""
    from playwright.sync_api import sync_playwright

    total = len(indices)
    saved: List[str] = []
    used_basenames: Dict[str, int] = {}
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=_pdf_headless())
            for current, idx_str in enumerate(indices, 1):
                try:
                    idx = int(idx_str)
                    if idx < 0 or idx >= len(urls):
                        continue
                    url = urls[idx]
                    if not url or not is_valid_url(url):
                        continue
                    progress_queue.put(("SAVING", url, str(current), str(total)))
                    page = browser.new_page()
                    try:
                        page.set_default_timeout(_PDF_PRINT_TIMEOUT_MS)
                        page.goto(
                            url,
                            wait_until=_PDF_NAVIGATION_WAIT,
                            timeout=_PDF_NAVIGATION_TIMEOUT_MS,
                        )
                        page.wait_for_timeout(_PDF_SETTLE_MS)
                        # Wait for load event so JS-rendered content and <title> are ready (e.g. FDA REMS)
                        try:
                            page.wait_for_load_state("load", timeout=_PDF_WAIT_FOR_CONTENT_MS)
                        except Exception:
                            pass
                        page.wait_for_timeout(2000)  # extra for post-load paint
                        base = page_title_or_h1(page, url)
                        if not base:
                            base = "page"
                        pdf_name = unique_pdf_basename(base, used_basenames, folder_path)
                        pdf_path = folder_path / pdf_name
                        page.pdf(path=str(pdf_path), print_background=True)
                        saved.append(pdf_name)
                    finally:
                        page.close()
                except (ValueError, Exception) as e:
                    msg = str(e).strip() or type(e).__name__
                    progress_queue.put(("ERROR", msg[:200]))
            browser.close()
    except Exception as e:
        msg = str(e).strip() or type(e).__name__
        progress_queue.put(("ERROR", msg[:200]))
    progress_queue.put(("DONE", str(len(saved))))


def generate_save_progress(
    folder_path: Path,
    urls: List[str],
    indices: List[str],
    *,
    drpid: int | None = None,
    folder_path_str: str = "",
    metadata: Dict[str, str] | None = None,
) -> Generator[str, None, None]:
    """
    Generator that yields progress lines for the PDF save operation.

    Yields: SAVING\\t{url}\\t{current}\\t{total}\\n then DONE\\t{count}\\n or ERROR\\t{msg}\\n.
    Yields #\\n as a heartbeat during long operations so the connection is not dropped.

    If drpid, folder_path_str, and metadata are provided, saves metadata (including folder
    stats) to Storage after PDFs are written, so one save happens after the folder is final.
    """
    progress_queue: queue.Queue[Tuple[str, ...]] = queue.Queue()
    worker = threading.Thread(
        target=_run_pdf_worker,
        args=(folder_path, urls, indices, progress_queue),
        daemon=True,
    )
    worker.start()
    done_sent = False
    while not done_sent:
        try:
            item = progress_queue.get(timeout=_HEARTBEAT_INTERVAL)
        except queue.Empty:
            yield "#\n"
            continue
        kind = item[0]
        if kind == "SAVING":
            yield f"SAVING\t{item[1]}\t{item[2]}\t{item[3]}\n"
        elif kind == "ERROR":
            yield f"ERROR\t{item[1]}\n"
        elif kind == "DONE":
            done_sent = True
            worker.join(timeout=1.0)
            if drpid is not None and folder_path_str and metadata:
                from interactive_collector.collector_state import get_scoreboard

                board = get_scoreboard()
                notes_lines = [f"  {n.get('url', '')} -> {n.get('status_label', '')}" for n in board if n.get("url")]
                status_notes = "\n".join(notes_lines) if notes_lines else None
                save_metadata(
                    drpid,
                    folder_path_str,
                    title=metadata.get("title", ""),
                    summary=metadata.get("summary", ""),
                    keywords=metadata.get("keywords", ""),
                    agency=metadata.get("agency", ""),
                    office=metadata.get("office", ""),
                    time_start=metadata.get("time_start", ""),
                    time_end=metadata.get("time_end", ""),
                    download_date=metadata.get("download_date", ""),
                    status_notes=status_notes,
                )
            yield f"DONE\t{item[1]}\n"
