#!/usr/bin/env python3
"""
Compare two DataLumos projects side-by-side using an authenticated browser session.

Reads all field values (title, agency, subject terms, geographic coverage, time period,
data types, summary, original URL, files) from both projects and compares them.

Usage (from repo root):
    python tests/compare_datalumos.py --treatment 246966 --control 244264
    python tests/compare_datalumos.py --treatment 246966 --control 244264 --save-html
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_argv_backup = sys.argv[:]
sys.argv = [sys.argv[0], "upload"]
from utils.Args import Args
from utils.Logger import Logger
Args.initialize()
sys.argv = _argv_backup
Logger.initialize(log_level="WARNING")

from upload.DataLumosBrowserSession import DataLumosBrowserSession
from upload.DataLumosFormFiller import DataLumosFormFiller

WORKSPACE_URL = "https://www.datalumos.org/datalumos/workspace"
PUBLIC_VIEW_URL = "https://www.datalumos.org/datalumos/project/{project_id}/version/V1/view"


# ── Public view JS extractor ──────────────────────────────────────────────────
# Scrapes the read-only public project view page (no authentication required).
# All metadata fields use the pattern:
#   <div class="panel-heading"><strong>Label:</strong><a>help link</a> value text</div>
# Files are in a table.table-striped with file links in the first <td>.

_EXTRACT_PUBLIC_VIEW_JS = """
() => {
    const clean = (text) => {
        if (!text) return null;
        return text.split('\\n')
            .map(l => l.trim())
            .filter(l => l)
            .join('\\n')
            .trim() || null;
    };

    // Title from h1
    const titleEl = document.querySelector('h1');
    const title = titleEl ? titleEl.innerText.trim() : null;

    // Files: table.table-striped — file name in <a> inside the first <td>
    const fileRows = Array.from(document.querySelectorAll('table.table-striped tbody tr'));
    const files = fileRows.map(tr => {
        const a = tr.querySelector('td a');
        if (!a) return null;
        const name = a.innerText.trim();
        const href = a.getAttribute('href') || '';
        return name ? { name, href } : null;
    }).filter(Boolean);

    // Generic metadata extractor: all fields share the panel-heading pattern
    const fieldMap = {};
    for (const el of document.querySelectorAll('div.panel-heading')) {
        const strong = el.querySelector('strong');
        if (!strong) continue;
        const labelText = strong.innerText.replace(/:$/, '').trim().toLowerCase();
        // Clone and strip the label and help-link <a> to isolate the value text
        const clone = el.cloneNode(true);
        clone.querySelectorAll('strong, a').forEach(e => e.remove());
        const value = clean(clone.innerText);
        if (value) fieldMap[labelText] = value;
    }

    // Subject Terms: split on semicolons into a list (consistent with workspace extractor)
    const kwRaw = fieldMap['subject terms'];
    const keywords = kwRaw
        ? kwRaw.split(/[;\\n]+/).map(k => k.trim()).filter(Boolean)
        : null;

    return {
        title:                title || fieldMap['project title'] || null,
        summary:              fieldMap['summary'] || null,
        agency:               fieldMap['agency'] || null,
        keywords:             keywords,
        geographic_coverage:  fieldMap['geographic coverage'] || null,
        time_period:          fieldMap['time period(s)'] || fieldMap['time period'] || null,
        data_types:           fieldMap['data type(s)'] || fieldMap['data types'] || null,
        collection_notes:     fieldMap['collection notes'] || null,
        files,
    };
}
"""

# ── ANSI colors ───────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BOLD   = "\033[1m"
DIM    = "\033[90m"
RESET  = "\033[0m"


# ── JavaScript extractor ──────────────────────────────────────────────────────

# Runs in the browser context; returns a structured dict of field values.
# Selectors confirmed from live DOM inspection (IDs have dynamic React suffixes,
# so we use prefix or class selectors rather than exact IDs).
_EXTRACT_JS = """
() => {
    const clean = (text) => {
        if (!text) return null;
        return text.split('\\n')
            .map(l => l.trim())
            .filter(l => l && !/^(edit|add value|no value|\u00d7|remove|collapse all|expand all)$/i.test(l))
            .join('\\n')
            .trim() || null;
    };

    // Title: the <h1> element, not document.title (which is always "Workspace")
    const titleEl = document.querySelector('h1');
    const title = titleEl ? clean(titleEl.innerText) : null;

    // Summary: div[data-type="wysihtml5"] with id starting "dcterms_description"
    // (not the edit-button anchor whose id starts with "edit-")
    const summaryEl = document.querySelector('div[data-type="wysihtml5"][id^="dcterms_description"]');
    const summary = summaryEl ? clean(summaryEl.innerText) : null;

    // Original URL: div.editable with id starting "imeta_sourceURL" (not "edit-imeta_sourceURL")
    const urlEl = document.querySelector('div.editable[id^="imeta_sourceURL"]');
    const original_url = urlEl ? clean(urlEl.innerText) : null;

    // Geographic coverage: #dcterms_location_0 (exact ID, no dynamic suffix)
    // If it has class "editable-empty" it's blank.
    const geoEl = document.querySelector('#dcterms_location_0');
    const geographic_coverage = (geoEl && !geoEl.classList.contains('editable-empty'))
        ? clean(geoEl.innerText) : null;

    // Data types: span.label.label-primary.dataTypesTags
    const dataTypeEls = document.querySelectorAll('span.label.label-primary.dataTypesTags');
    const data_types = dataTypeEls.length > 0
        ? Array.from(dataTypeEls).map(e => e.innerText.trim()).filter(Boolean).join(', ')
        : null;

    // Keywords: select2 chips — strip leading × (the remove button is first in DOM)
    const keywordEls = document.querySelectorAll(
        '.select2-selection__choice__display, .select2-selection__rendered li.select2-selection__choice'
    );
    const keywords = Array.from(keywordEls)
        .map(e => e.innerText?.trim().replace(/^\u00d7\\s*/, '').replace(/\u00d7\\s*$/, '').trim())
        .filter(Boolean);

    // Agency: saved org entries are direct div children of the agency section's
    // wrapper div inside #groupAttr0. Each entry's text is "OrgName   edit   remove".
    // Structure: #groupAttr0 .panel-body > div:first-child > div:first-child > div[n>=5]
    const agencyWrapper = document.querySelector(
        '#groupAttr0 .panel-body > div:first-child > div:first-child'
    );
    let agencies = [];
    if (agencyWrapper) {
        const entryDivs = Array.from(agencyWrapper.querySelectorAll(':scope > div'));
        agencies = entryDivs
            .map(d => {
                // Grab text up to the "   edit" action link
                const txt = (d.innerText || '').split(/\\s{2,}edit\\s/)[0].trim();
                return txt;
            })
            .filter(s => s && s.length > 2
                && !/^(add value|reorder|\u00d7|remove|no value)$/i.test(s));
        agencies = [...new Set(agencies)];
    }

    // Collection notes: div[data-type="wysihtml5"] with id starting "imeta_collectionNotes"
    const notesEl = document.querySelector('div[data-type="wysihtml5"][id^="imeta_collectionNotes"]');
    const collection_notes = notesEl ? clean(notesEl.innerText) : null;

    // Time period: the property wrapper id contains "timePeriods" and "propertyWrapper".
    // Entry text format: "1/1/2016 – 12/31/2016   edit   remove"
    // Extract text before the "   edit" action.
    const tpEls = Array.from(document.querySelectorAll('[id*="timePeriods"][id*="propertyWrapper"]'));
    let time_period_raw = null;
    const tpValues = tpEls
        .map(el => (el.innerText || '').split(/\\s{2,}edit\\s/)[0].trim())
        .filter(s => s && s.length > 1);
    if (tpValues.length > 0) {
        time_period_raw = tpValues.join('; ');
    }

    // Files: listed in table.table-hover (file manager table in workspace).
    // Each row: [checkbox, name, type, size, lastModified, actions]
    const fileRows = Array.from(document.querySelectorAll('table.table-hover tbody tr'));
    const files = fileRows
        .map(tr => {
            const tds = tr.querySelectorAll('td');
            if (tds.length < 2) return null;
            const name = tds[1].innerText.trim();
            return name ? { name, href: '' } : null;
        })
        .filter(Boolean);

    return {
        title,
        agency:               agencies.length > 0 ? agencies : null,
        summary,
        original_url,
        keywords:             keywords.length > 0 ? keywords : null,
        geographic_coverage,
        time_period:          time_period_raw,
        data_types,
        collection_notes,
        files,
    };
}
"""


# ── Project reader ────────────────────────────────────────────────────────────

def read_project(session: DataLumosBrowserSession, workspace_id: str,
                 save_html: Optional[Path] = None) -> dict:
    """
    Navigate to a DataLumos workspace project and extract all field values.

    Args:
        session: Authenticated DataLumosBrowserSession.
        workspace_id: Numeric workspace/project ID.
        save_html: Optional path to save the raw page HTML for debugging.

    Returns:
        Dict with keys: title, agency, summary, original_url, keywords,
        geographic_coverage, time_period, data_types, collection_notes, files.
    """
    page = session.ensure_browser()
    session.ensure_authenticated()

    url = f"{WORKSPACE_URL}?goToLevel=project&goToPath=/datalumos/{workspace_id}#"
    Logger.info(f"Navigating to workspace {workspace_id}: {url}")
    page.goto(url, wait_until="load", timeout=120000)

    from upload.DataLumosAuthenticator import wait_for_human_verification
    wait_for_human_verification(page, timeout=60000)

    # Expand all sections so all field values are visible
    filler = DataLumosFormFiller(page, timeout=Args.upload_timeout)
    filler.wait_for_obscuring_elements()
    try:
        filler.expand_all_sections()
    except Exception as exc:
        Logger.warning(f"expand_all_sections failed: {exc}")

    filler.wait_for_obscuring_elements()
    page.wait_for_timeout(2000)

    if save_html:
        html = page.content()
        save_html.write_text(html, encoding="utf-8")
        Logger.info(f"Saved page HTML to {save_html}")

    data = page.evaluate(_EXTRACT_JS)
    data["_workspace_id"] = workspace_id
    data["_url"] = url
    return data


def read_project_public_view(project_id: str, headless: bool = True,
                             timeout: int = 120000) -> dict:
    """
    Scrape a DataLumos public project view page without authentication.

    Works for any project regardless of ownership. Uses a standalone Playwright
    session (does not require DataLumosBrowserSession or credentials).

    Args:
        project_id: Numeric project/workspace ID.
        headless:   Run browser in headless mode (default True).
        timeout:    Page load timeout in milliseconds.

    Returns:
        Dict with keys: title, summary, agency, keywords, geographic_coverage,
        time_period, data_types, collection_notes, files.
    """
    from playwright.sync_api import sync_playwright

    url = PUBLIC_VIEW_URL.format(project_id=project_id)
    Logger.info(f"Scraping public view for project {project_id}: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        context.set_default_timeout(timeout)
        page = context.new_page()
        try:
            page.goto(url, wait_until="networkidle", timeout=timeout)
            data = page.evaluate(_EXTRACT_PUBLIC_VIEW_JS)
            data["_project_id"] = project_id
            data["_url"] = url
            return data
        finally:
            browser.close()


# ── Comparison logic ──────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Normalise whitespace and case for fuzzy comparison."""
    return re.sub(r"\s+", " ", (s or "").lower()).strip()


def _set_from(value) -> set[str]:
    """Convert a string or list value to a set of normalised tokens."""
    if not value:
        return set()
    items = value if isinstance(value, list) else [value]
    result = set()
    for item in items:
        for part in re.split(r"[\n,;]+", str(item)):
            p = _norm(part)
            if p:
                result.add(p)
    return result


def compare_field(name: str, treatment, control) -> tuple[str, str]:
    """
    Compare one field between treatment and control.

    Returns (status, detail) where status is OK / WARN / FAIL / SKIP.
    """
    t_empty = treatment is None or (isinstance(treatment, list) and len(treatment) == 0) \
              or (isinstance(treatment, str) and not treatment.strip())
    c_empty = control is None or (isinstance(control, list) and len(control) == 0) \
              or (isinstance(control, str) and not control.strip())

    if t_empty and c_empty:
        return "SKIP", "Both empty"
    if c_empty:
        return "SKIP", f"Control empty — treatment: {_preview(treatment)}"
    if t_empty:
        return "FAIL", f"Missing in treatment (control has: {_preview(control)})"

    # Files: compare by name set
    if name == "Files":
        t_names = {_norm(f["name"]) for f in (treatment or [])}
        c_names = {_norm(f["name"]) for f in (control or [])}
        if not t_names:
            return "FAIL", f"No files in treatment; control has {len(c_names)}"
        only_control = c_names - t_names
        only_treatment = t_names - c_names
        if not only_control and not only_treatment:
            return "OK", f"{len(t_names)} files match"
        detail_parts = []
        if only_control:
            detail_parts.append(f"missing from treatment: {sorted(only_control)}")
        if only_treatment:
            detail_parts.append(f"only in treatment: {sorted(only_treatment)}")
        severity = "FAIL" if len(only_control) > len(c_names) / 2 else "WARN"
        return severity, "; ".join(detail_parts)

    # Lists / sets (agency, keywords)
    if isinstance(treatment, list) or isinstance(control, list):
        t_set = _set_from(treatment)
        c_set = _set_from(control)
        if t_set == c_set:
            return "OK", f"{sorted(t_set)}"
        only_c = c_set - t_set
        only_t = t_set - c_set
        detail_parts = []
        if only_c:
            detail_parts.append(f"missing: {sorted(only_c)}")
        if only_t:
            detail_parts.append(f"extra: {sorted(only_t)}")
        return "WARN", "; ".join(detail_parts)

    # Scalars: normalised string comparison
    t_norm = _norm(str(treatment))
    c_norm = _norm(str(control))

    if t_norm == c_norm:
        return "OK", _preview(treatment)

    # Check substring match (partial match)
    if t_norm in c_norm or c_norm in t_norm:
        return "WARN", f"Partial match — treatment: {_preview(treatment)} | control: {_preview(control)}"

    # Special case for summary: compare presence and length
    if name == "Summary":
        t_len = len(t_norm)
        c_len = len(c_norm)
        ratio = min(t_len, c_len) / max(t_len, c_len) if max(t_len, c_len) > 0 else 0
        if ratio > 0.5:
            return "WARN", f"Different text (treatment {t_len} chars, control {c_len} chars)"
        return "FAIL", f"Very different (treatment {t_len} chars vs control {c_len} chars)"

    return "WARN", f"treatment: {_preview(treatment)} | control: {_preview(control)}"


def _preview(value, max_len: int = 80) -> str:
    if value is None:
        return "(none)"
    if isinstance(value, list):
        text = ", ".join(str(v) for v in value[:5])
        if len(value) > 5:
            text += f", … ({len(value)} total)"
        return text
    s = str(value)
    return s[:max_len] + "…" if len(s) > max_len else s


def compare_projects(treatment: dict, control: dict) -> list[tuple[str, str, str]]:
    """
    Compare all fields between two project dicts.

    Returns list of (field_name, status, detail) tuples.
    """
    checks = []

    def check(name, t_key, c_key=None):
        c_key = c_key or t_key
        t_val = treatment.get(t_key)
        c_val = control.get(c_key)
        status, detail = compare_field(name, t_val, c_val)
        checks.append((name, status, detail))

    check("Title",                "title")
    check("Agency",               "agency")
    check("Summary",              "summary")
    check("Subject Terms",        "keywords")
    check("Geographic Coverage",  "geographic_coverage")
    check("Time Period",          "time_period")
    check("Data Types",           "data_types")
    check("Original URL",         "original_url")

    # Collection Notes: strip "(Downloaded YYYY-MM-DD)" before comparing since
    # treatment and control are collected on different dates.
    _dl_re = re.compile(r"\s*\(Downloaded[^)]*\)\s*", re.IGNORECASE)
    t_notes = _dl_re.sub("", treatment.get("collection_notes") or "").strip() or None
    c_notes = _dl_re.sub("", control.get("collection_notes") or "").strip() or None
    status, detail = compare_field("Collection Notes", t_notes, c_notes)
    checks.append(("Collection Notes", status, detail))

    # Files comparison
    t_files = treatment.get("files") or []
    c_files = control.get("files") or []
    status, detail = compare_field("Files", t_files, c_files)
    checks.append(("Files", status, detail))

    return checks


def overall_rating(checks: list[tuple[str, str, str]]) -> str:
    fails = sum(1 for _, s, _ in checks if s == "FAIL")
    warns = sum(1 for _, s, _ in checks if s == "WARN")
    if fails > 0:
        return "RED"
    if warns > 0:
        return "YELLOW"
    return "GREEN"


# ── Report ────────────────────────────────────────────────────────────────────

_ICONS = {"OK": "✓", "WARN": "~", "FAIL": "✗", "SKIP": "-"}
_COLORS = {"OK": GREEN, "WARN": YELLOW, "FAIL": RED, "SKIP": DIM}


def print_report(treatment: dict, control: dict,
                 checks: list[tuple[str, str, str]]) -> None:
    t_id = treatment.get("_workspace_id", "?")
    c_id = control.get("_workspace_id", "?")

    print(f"\n{BOLD}DataLumos Project Comparison{RESET}")
    print(f"  Treatment : workspace {t_id}")
    print(f"  Control   : workspace {c_id}")
    print(f"{'─'*65}")

    for name, status, detail in checks:
        color = _COLORS.get(status, "")
        icon  = _ICONS.get(status, "?")
        print(f"  {color}{icon} [{status:4s}]{RESET}  {name}")
        if detail and status != "OK":
            # Indent detail lines
            for line in detail.split("; "):
                print(f"            {DIM}{line}{RESET}")
        elif status == "OK" and detail and detail != "(none)":
            print(f"            {DIM}{detail[:100]}{RESET}")

    print(f"{'─'*65}")
    rating = overall_rating(checks)
    color = {"GREEN": GREEN, "YELLOW": YELLOW, "RED": RED}[rating]
    fails  = sum(1 for _, s, _ in checks if s == "FAIL")
    warns  = sum(1 for _, s, _ in checks if s == "WARN")
    skips  = sum(1 for _, s, _ in checks if s == "SKIP")
    oks    = sum(1 for _, s, _ in checks if s == "OK")
    summary = ", ".join(filter(None, [
        f"{oks} OK" if oks else "",
        f"{warns} warnings" if warns else "",
        f"{fails} failures" if fails else "",
        f"{skips} skipped" if skips else "",
    ]))
    print(f"\n  {BOLD}Overall: {color}{rating}{RESET}  ({summary})\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_comparison(treatment_id: str, control_id: str,
                   save_html: bool = False) -> tuple[dict, dict, list]:
    """
    Read both projects from DataLumos and compare them.

    Returns (treatment_data, control_data, checks).
    """
    import tempfile
    session = DataLumosBrowserSession()
    try:
        if save_html:
            html_dir = Path(tempfile.mkdtemp(prefix="drp_compare_"))
            print(f"Saving HTML to {html_dir}")
        else:
            html_dir = None

        print(f"Reading treatment project {treatment_id}...")
        t_html = html_dir / f"treatment_{treatment_id}.html" if html_dir else None
        treatment = read_project(session, treatment_id, save_html=t_html)

        print(f"Reading control project {control_id}...")
        c_html = html_dir / f"control_{control_id}.html" if html_dir else None
        control = read_project(session, control_id, save_html=c_html)
    finally:
        session.close()

    checks = compare_projects(treatment, control)
    return treatment, control, checks


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare two DataLumos projects")
    parser.add_argument("--treatment", required=True, help="Treatment workspace/project ID")
    parser.add_argument("--control",   required=True, help="Control workspace/project ID")
    parser.add_argument("--save-html", action="store_true",
                        help="Save raw page HTML to debug/ for inspection")
    args = parser.parse_args()

    treatment, control, checks = run_comparison(
        args.treatment, args.control, save_html=args.save_html
    )
    print_report(treatment, control, checks)

    rating = overall_rating(checks)
    sys.exit(0 if rating in ("GREEN", "YELLOW") else 1)


if __name__ == "__main__":
    main()
