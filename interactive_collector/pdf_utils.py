"""
Shared PDF utilities for the interactive collector.

Page title extraction and unique PDF basename generation.
"""

from pathlib import Path
from typing import Any, Dict, Optional

from utils.file_utils import sanitize_filename


def page_title_or_h1(page: Any, url: str = "") -> str:
    """
    Get page <title> or first <h1> text from a Playwright page.

    Falls back to URL-derived segment (path or hostname) when title/h1 empty.
    Returns empty string if neither available.

    Args:
        page: Playwright Page object
        url: Optional URL for fallback when title/h1 empty

    Returns:
        Non-empty string when available, else ""
    """
    try:
        title = page.title()
        if title and title.strip():
            return title.strip()
        try:
            title = page.evaluate(
                "() => document.querySelector('title')?.textContent?.trim() || document.title?.trim() || ''"
            )
            if title:
                return title
        except Exception:
            pass
        try:
            h1 = page.locator("h1").first.text_content(timeout=2000)
            if h1 and h1.strip():
                return h1.strip()
        except Exception:
            pass
        if url:
            from urllib.parse import urlparse

            parsed = urlparse(url)
            path = (parsed.path or "").rstrip("/")
            if path:
                segment = path.split("/")[-1]
                if segment and len(segment) < 80:
                    return segment
            if parsed.netloc:
                return parsed.netloc.split(".")[0] or parsed.netloc
    except Exception:
        pass
    return ""


def unique_pdf_basename(
    base: str,
    used: Dict[str, int],
    folder_path: Optional[Path] = None,
) -> str:
    """
    Return a unique sanitized basename: base.pdf or base_1.pdf, base_2.pdf, etc.

    When folder_path is set, skips names that already exist in the folder (no overwrite).

    Args:
        base: Base name (e.g. page title)
        used: Mutable dict tracking usage counts per key (by lowercase base)
        folder_path: Optional; when set, also skips existing files in folder

    Returns:
        Unique filename like "My_Page.pdf" or "Dataset_1.pdf"
    """
    safe = sanitize_filename(base, max_length=80)
    if not safe:
        safe = "page"
    key = safe.lower()
    n = used.get(key, 0)
    while True:
        name = f"{safe}.pdf" if n == 0 else f"{safe}_{n}.pdf"
        if folder_path is not None and (folder_path / name).exists():
            n += 1
            continue
        used[key] = n + 1
        return name
