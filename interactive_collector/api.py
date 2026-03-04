"""
Flask Blueprint for Interactive Collector JSON API.

Serves the SPA with: projects, projects/load, scoreboard, save, download-file.
"""

import json
import sys
from pathlib import Path
from typing import Any

from urllib.parse import unquote

from flask import Blueprint, Response, request
import requests

from interactive_collector.api_download import generate_download_progress
from interactive_collector.api_projects import (
    ensure_output_folder,
    folder_path_for_drpid,
    get_first_eligible,
    get_next_eligible_after,
    get_project_by_drpid,
)
from interactive_collector.api_save import generate_save_progress, save_metadata
from interactive_collector.api_scoreboard import add_download, add_to_scoreboard, clear_scoreboard, get_scoreboard_tree, get_scoreboard_urls
from interactive_collector.collector_state import (
    get_metadata_from_page,
    get_result_by_drpid,
    get_scoreboard,
    set_metadata_from_page,
)
from utils.file_utils import sanitize_filename
from utils.url_utils import BROWSER_HEADERS, is_valid_url

api_bp = Blueprint("api", __name__, url_prefix="/api")


def _str_or_none(x: Any) -> str | None:
    """Return stripped string or None if missing/empty."""
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


@api_bp.route("/projects/first", methods=["GET"])
def projects_first() -> Any:
    """
    Return the first eligible project (prereq=sourcing, no errors).

    Returns:
        JSON project dict or 404.
    """
    proj = get_first_eligible()
    if not proj:
        return {"error": "No eligible project"}, 404
    return proj


@api_bp.route("/projects/next", methods=["GET"])
def projects_next() -> Any:
    """
    Return the next eligible project after current_drpid.

    Query: current_drpid (required).

    Returns:
        JSON project dict or 404.
    """
    current = request.args.get("current_drpid", "").strip()
    if not current:
        return {"error": "current_drpid required"}, 400
    try:
        current_drpid = int(current)
    except ValueError:
        return {"error": "Invalid current_drpid"}, 400
    proj = get_next_eligible_after(current_drpid)
    if not proj:
        return {"error": "No next project"}, 404
    return proj


@api_bp.route("/projects/<int:drpid>", methods=["GET"])
def projects_get(drpid: int) -> Any:
    """
    Return the project record for the given DRPID.

    Returns:
        JSON project dict or 404.
    """
    proj = get_project_by_drpid(drpid)
    if not proj:
        return {"error": "Project not found"}, 404
    return proj


@api_bp.route("/projects/load", methods=["POST"])
def projects_load() -> Any:
    """
    Load a project for the collector. Does not fetch the source URL.
    Ensures output folder and clears scoreboard.
    Expects JSON: {drpid} or no body for first eligible.
    Returns project + folder_path + scoreboard.
    """
    if request.is_json:
        data = request.get_json() or {}
    else:
        data = {}
    drpid_val = data.get("drpid")
    if drpid_val is not None:
        try:
            drpid = int(drpid_val)
        except (ValueError, TypeError):
            return {"error": "Invalid drpid"}, 400
        proj = get_project_by_drpid(drpid)
    else:
        proj = get_first_eligible()
    if not proj:
        return {"error": "No project found"}, 404
    drpid = proj["DRPID"]
    source_url = (proj.get("source_url") or "").strip()
    folder_path = ensure_output_folder(drpid)
    clear_scoreboard()

    metadata = {
        "title": (proj.get("title") or "").strip(),
        "summary": (proj.get("summary") or "").strip(),
        "keywords": (proj.get("keywords") or "").strip(),
        "agency": (proj.get("agency") or "").strip(),
        "office": (proj.get("office") or "").strip(),
        "time_start": (proj.get("time_start") or "").strip(),
        "time_end": (proj.get("time_end") or "").strip(),
        "download_date": (proj.get("download_date") or "").strip(),
    }
    if not metadata["download_date"]:
        from datetime import date
        metadata["download_date"] = date.today().isoformat()

    return {
        "DRPID": drpid,
        "source_url": source_url,
        "folder_path": folder_path,
        "scoreboard": get_scoreboard_tree(),
        "scoreboard_urls": get_scoreboard_urls(),
        "metadata": metadata,
    }


def _unique_pdf_basename_for_folder(base: str, folder_path: Path) -> str:
    """Return unique sanitized PDF basename (base.pdf or base_1.pdf, etc.)."""
    safe = sanitize_filename(base, max_length=80) if base else "page"
    if not safe:
        safe = "page"
    for i in range(1000):
        name = f"{safe}.pdf" if i == 0 else f"{safe}_{i}.pdf"
        if not (folder_path / name).exists():
            return name
    return f"{safe}_999.pdf"


@api_bp.route("/extension/save-pdf", methods=["POST", "OPTIONS"])
def extension_save_pdf() -> Any:
    """
    Receive PDF from browser extension; write to output folder and add to scoreboard.

    Expects multipart form: drpid, url, referrer (optional), pdf (file).
    CORS: allow extension origin.
    """
    if request.method == "OPTIONS":
        return "", 204, {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }
    drpid_str = (request.form.get("drpid") or "").strip()
    url = (request.form.get("url") or "").strip()
    referrer = (request.form.get("referrer") or "").strip() or None
    page_title = (request.form.get("title") or request.form.get("page_title") or "").strip()
    pdf_file = request.files.get("pdf")

    if not drpid_str:
        return {"error": "drpid required"}, 400
    if not url or not is_valid_url(url):
        return {"error": "valid url required"}, 400
    if not pdf_file:
        return {"error": "pdf file required"}, 400
    try:
        drpid = int(drpid_str)
    except (ValueError, TypeError):
        return {"error": "invalid drpid"}, 400

    folder_path_str = get_result_by_drpid().get(drpid, {}).get("folder_path")
    if not folder_path_str:
        folder_path_str = ensure_output_folder(drpid)
    if not folder_path_str:
        return {"error": "no output folder for project"}, 400

    folder_path = Path(folder_path_str)
    if not folder_path.is_dir():
        return {"error": "output folder not found"}, 400

    # Prefer page title from extension for a helpful filename; fallback to URL-derived base
    from urllib.parse import urlparse
    if page_title and len(page_title) <= 80:
        base = sanitize_filename(page_title, max_length=80) or "page"
    else:
        parsed = urlparse(url)
        path = (parsed.path or "").rstrip("/")
        base = path.split("/")[-1] if path else (parsed.netloc or "page").split(".")[0]
        if not base or len(base) > 80:
            base = (parsed.netloc or "page").split(".")[0] or "page"
        base = sanitize_filename(base, max_length=80) if base else "page"
    if base.lower().endswith(".pdf"):
        base = base[:-4].rstrip("._")
    if not base:
        base = "page"
    basename = _unique_pdf_basename_for_folder(base, folder_path)
    dest = folder_path / basename

    try:
        pdf_file.save(str(dest))
    except OSError as e:
        return {"error": str(e)[:200]}, 500

    add_to_scoreboard(url, referrer, "OK", page_title or None)
    return (
        {"ok": True, "filename": basename, "path": str(dest)},
        200,
        {"Access-Control-Allow-Origin": "*"},
    )


@api_bp.route("/proxy", methods=["GET"])
def proxy_resource() -> Any:
    """
    Fetch an external URL and stream it back so the iframe can load CSS/JS/images
    from our origin (avoids CSP and cross-origin issues in srcdoc).
    """
    raw = request.args.get("url", "").strip()
    url = unquote(raw) if raw else ""
    if not url or not is_valid_url(url):
        return {"error": "Invalid or missing url"}, 400
    try:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        return {"error": str(e)}, 502
    content_type = resp.headers.get("Content-Type") or "application/octet-stream"
    if ";" in content_type:
        content_type = content_type.split(";")[0].strip()

    body = resp.content
    headers = {
        "Content-Type": content_type,
        "Access-Control-Allow-Origin": "*",
    }
    return Response(body, status=200, headers=headers, mimetype=content_type)


@api_bp.route("/no-links", methods=["POST"])
def no_links_route() -> Any:
    """
    Mark the current project (DRPID) as having no live links.

    Expects JSON/form: {drpid}.
    Updates Storage: status = 'no_links'.
    """
    if request.is_json:
        data = request.get_json() or {}
    else:
        data = {"drpid": (request.form.get("drpid") or "").strip()}
    drpid_val = data.get("drpid")
    if not drpid_val:
        return {"error": "drpid required"}, 400
    try:
        drpid = int(drpid_val)
    except (ValueError, TypeError):
        return {"error": "Invalid drpid"}, 400
    from interactive_collector.api_projects import _ensure_storage
    _ensure_storage()
    from storage import Storage
    try:
        Storage.update_record(drpid, {"status": "no_links"})
    except ValueError:
        return {"error": "Project not found"}, 404
    return {"ok": True}


@api_bp.route("/metadata-from-page", methods=["GET", "POST", "OPTIONS"])
def metadata_from_page_route() -> Any:
    """
    GET: Return and consume stored metadata for drpid (extracted from Copy & Open page only).
    POST: Store metadata sent by extension from the Copy & Open target page (not subsequent pages).
    CORS: allow extension content script (origin is the catalog page, e.g. catalog.data.gov).
    """
    if request.method == "OPTIONS":
        return "", 204, {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Private-Network": "true",
        }
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Private-Network": "true",
    }
    if request.method == "POST":
        if request.is_json:
            data = request.get_json() or {}
        else:
            data = {k: request.form.get(k) for k in ("drpid", "title", "summary", "keywords", "agency", "office", "time_start", "time_end", "download_date")}
        drpid_val = data.get("drpid")
        if not drpid_val:
            return {"error": "drpid required"}, 400, cors_headers
        try:
            drpid = int(drpid_val)
        except (ValueError, TypeError):
            return {"error": "Invalid drpid"}, 400, cors_headers
        payload = {
            k: (data.get(k) or "").strip()
            for k in ("title", "summary", "keywords", "agency", "office", "time_start", "time_end", "download_date")
        }
        payload = {k: v for k, v in payload.items() if v}
        set_metadata_from_page(drpid, payload)
        return {"ok": True}, 200, cors_headers
    # GET
    drpid_val = request.args.get("drpid", "").strip()
    if not drpid_val:
        return {"error": "drpid required"}, 400, cors_headers
    try:
        drpid = int(drpid_val)
    except (ValueError, TypeError):
        return {"error": "Invalid drpid"}, 400, cors_headers
    meta = get_metadata_from_page(drpid)
    return {"metadata": meta}, 200, cors_headers


@api_bp.route("/downloads-watcher/start", methods=["POST"])
def downloads_watcher_start() -> Any:
    """
    Start watching the user's Downloads folder. New files are moved to the project output folder.

    Expects JSON/form: {drpid}. Triggered when user clicks Copy & Open.
    """
    if request.is_json:
        data = request.get_json() or {}
    else:
        data = {"drpid": (request.form.get("drpid") or "").strip()}
    drpid_val = data.get("drpid")
    if drpid_val is None or drpid_val == "":
        return {"error": "drpid required", "ok": False}, 400
    try:
        drpid = int(drpid_val)
    except (ValueError, TypeError):
        return {"error": "Invalid drpid", "ok": False}, 400
    folder_path_str = get_result_by_drpid().get(drpid, {}).get("folder_path")
    if not folder_path_str:
        folder_path_str = ensure_output_folder(drpid)
    if not folder_path_str:
        return {"error": "No output folder for project", "ok": False}, 400
    output_folder = Path(folder_path_str)

    def on_new_file(dest_path_str: str, dest_path: Path, size: int) -> None:
        ext = (dest_path.suffix or "").lstrip(".") or ""
        add_download(
            url=f"Downloads:{dest_path.name}",
            referrer=None,
            file_path=dest_path_str,
            file_size=size,
            extension=ext,
            filename=dest_path.name,
        )

    from interactive_collector.downloads_watcher import start_watching
    ok, msg = start_watching(drpid, output_folder, on_new_file)
    if not ok:
        return {"error": msg, "ok": False}, 400
    return {"ok": True, "message": msg}


@api_bp.route("/downloads-watcher/stop", methods=["POST"])
def downloads_watcher_stop() -> Any:
    """Stop watching the Downloads folder. Triggered when user clicks Collection complete."""
    from interactive_collector.downloads_watcher import stop_watching
    ok, msg = stop_watching()
    return {"ok": ok, "message": msg}


@api_bp.route("/downloads-watcher/status", methods=["GET"])
def downloads_watcher_status() -> Any:
    """Return whether the Downloads watcher is currently active."""
    from interactive_collector.downloads_watcher import is_watching
    return {"watching": is_watching()}


@api_bp.route("/scoreboard", methods=["GET"])
def scoreboard_get() -> Any:
    """Return the current scoreboard tree."""
    return {"scoreboard": get_scoreboard_tree(), "urls": get_scoreboard_urls()}


@api_bp.route("/scoreboard/clear", methods=["POST"])
def scoreboard_clear() -> Any:
    """Clear the scoreboard."""
    clear_scoreboard()
    return {"scoreboard": [], "urls": []}


@api_bp.route("/scoreboard/add", methods=["POST"])
def scoreboard_add() -> Any:
    """
    Add a URL to the scoreboard.

    Expects JSON/form: {url, referrer?, status_label}.
    """
    if request.is_json:
        data = request.get_json() or {}
    else:
        data = {
            "url": (request.form.get("url") or "").strip(),
            "referrer": (request.form.get("referrer") or "").strip() or None,
            "status_label": (request.form.get("status_label") or "OK").strip(),
        }
    url = _str_or_none(data.get("url")) or ""
    if not url:
        return {"error": "url required"}, 400
    referrer = _str_or_none(data.get("referrer"))
    status_label = _str_or_none(data.get("status_label")) or "OK"
    add_to_scoreboard(url, referrer, status_label)
    return {"scoreboard": get_scoreboard_tree(), "urls": get_scoreboard_urls()}


@api_bp.route("/save", methods=["POST"])
def save_route() -> Any:
    """
    Save metadata to DB and optionally generate PDFs for checked scoreboard pages.

    Expects form: drpid, folder_path, scoreboard_urls_json (JSON array),
    save_url (list of indices), metadata_*.
    Streams progress as text/plain (SAVING, DONE, ERROR).
    """
    drpid_str = (request.form.get("drpid") or "").strip()
    folder_path_str = (request.form.get("folder_path") or "").strip()
    urls_json = (request.form.get("scoreboard_urls_json") or "[]").strip()
    indices = request.form.getlist("save_url")

    # Save metadata once: after PDFs when we generate them, otherwise now.
    will_generate_pdfs = bool(folder_path_str and indices)
    metadata = {
        "title": (request.form.get("metadata_title") or "").strip(),
        "summary": (request.form.get("metadata_summary") or "").strip(),
        "keywords": (request.form.get("metadata_keywords") or "").strip(),
        "agency": (request.form.get("metadata_agency") or "").strip(),
        "office": (request.form.get("metadata_office") or "").strip(),
        "time_start": (request.form.get("metadata_time_start") or "").strip(),
        "time_end": (request.form.get("metadata_time_end") or "").strip(),
        "download_date": (request.form.get("metadata_download_date") or "").strip(),
    }
    if drpid_str and not will_generate_pdfs:
        try:
            drpid = int(drpid_str)
            board = get_scoreboard()
            notes_lines = [f"  {n.get('url', '')} -> {n.get('status_label', '')}" for n in board if n.get("url")]
            status_notes = "\n".join(notes_lines) if notes_lines else None
            save_metadata(
                drpid,
                folder_path_str,
                title=metadata["title"],
                summary=metadata["summary"],
                keywords=metadata["keywords"],
                agency=metadata["agency"],
                office=metadata["office"],
                time_start=metadata["time_start"],
                time_end=metadata["time_end"],
                download_date=metadata["download_date"],
                status_notes=status_notes,
            )
        except (ValueError, TypeError):
            pass

    if not folder_path_str or not indices:
        # Metadata-only save: stream DONE so frontend can finish
        def empty_stream():
            yield "DONE\t0\n"

        return Response(
            empty_stream(),
            mimetype="text/plain; charset=utf-8",
            headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
        )


@api_bp.route("/skip", methods=["POST"])
def skip_route() -> Any:
    """
    Update the project like Save but set status to "collector hold - {reason}".

    Expects form or JSON: drpid, reason, folder_path (optional), metadata_*.
    """
    if request.is_json:
        data = request.get_json() or {}
        drpid_val = data.get("drpid")
        reason = (data.get("reason") or "").strip()
        folder_path_str = (data.get("folder_path") or "").strip()
        metadata = {
            "title": (data.get("metadata_title") or "").strip(),
            "summary": (data.get("metadata_summary") or "").strip(),
            "keywords": (data.get("metadata_keywords") or "").strip(),
            "agency": (data.get("metadata_agency") or "").strip(),
            "office": (data.get("metadata_office") or "").strip(),
            "time_start": (data.get("metadata_time_start") or "").strip(),
            "time_end": (data.get("metadata_time_end") or "").strip(),
            "download_date": (data.get("metadata_download_date") or "").strip(),
        }
    else:
        drpid_val = (request.form.get("drpid") or "").strip()
        reason = (request.form.get("reason") or "").strip()
        folder_path_str = (request.form.get("folder_path") or "").strip()
        metadata = {
            "title": (request.form.get("metadata_title") or "").strip(),
            "summary": (request.form.get("metadata_summary") or "").strip(),
            "keywords": (request.form.get("metadata_keywords") or "").strip(),
            "agency": (request.form.get("metadata_agency") or "").strip(),
            "office": (request.form.get("metadata_office") or "").strip(),
            "time_start": (request.form.get("metadata_time_start") or "").strip(),
            "time_end": (request.form.get("metadata_time_end") or "").strip(),
            "download_date": (request.form.get("metadata_download_date") or "").strip(),
        }
    if not reason:
        return {"error": "reason required", "ok": False}, 400
    if not drpid_val:
        return {"error": "drpid required", "ok": False}, 400
    try:
        drpid = int(drpid_val)
    except (ValueError, TypeError):
        return {"error": "Invalid drpid", "ok": False}, 400
    if not folder_path_str:
        folder_path_str = get_result_by_drpid().get(drpid, {}).get("folder_path") or ""
        if not folder_path_str:
            folder_path_str = ensure_output_folder(drpid) or ""
    board = get_scoreboard()
    notes_lines = [f"  {n.get('url', '')} -> {n.get('status_label', '')}" for n in board if n.get("url")]
    status_notes = "\n".join(notes_lines) if notes_lines else None
    try:
        save_metadata(
            drpid,
            folder_path_str,
            title=metadata["title"],
            summary=metadata["summary"],
            keywords=metadata["keywords"],
            agency=metadata["agency"],
            office=metadata["office"],
            time_start=metadata["time_start"],
            time_end=metadata["time_end"],
            download_date=metadata["download_date"],
            status_notes=status_notes,
            status_override=f"collector hold - {reason}",
        )
        return {"ok": True}
    except (ValueError, RuntimeError) as e:
        return {"error": str(e)[:200], "ok": False}, 500

    try:
        urls = json.loads(urls_json)
    except json.JSONDecodeError:
        return {"error": "Invalid scoreboard_urls_json"}, 400

    folder_path = Path(folder_path_str)
    if not folder_path.is_dir():
        return {"error": "Output folder not found"}, 400

    try:
        drpid_for_stats = int(drpid_str) if drpid_str else None
    except (ValueError, TypeError):
        drpid_for_stats = None

    def stream() -> Any:
        for line in generate_save_progress(
            folder_path,
            urls,
            indices,
            drpid=drpid_for_stats,
            folder_path_str=folder_path_str,
            metadata=metadata if will_generate_pdfs and drpid_for_stats is not None else None,
        ):
            sys.stderr.write(line)
            sys.stderr.flush()
            yield line

    return Response(
        stream(),
        mimetype="text/plain; charset=utf-8",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


@api_bp.route("/download-file", methods=["POST"])
def download_file_route() -> Any:
    """
    Download a non-HTML URL to the project output folder.

    Expects form: url, drpid, referrer?.
    Streams progress (SAVING, PROGRESS, DONE).
    Creates output folder on demand if missing.
    """
    url = (request.form.get("url") or "").strip()
    drpid_str = (request.form.get("drpid") or "").strip()
    referrer = (request.form.get("referrer") or "").strip() or None

    if not url or not is_valid_url(url):
        return "Invalid URL", 400
    try:
        drpid = int(drpid_str)
    except (ValueError, TypeError):
        return "Invalid DRPID", 400

    folder_path = get_result_by_drpid().get(drpid, {}).get("folder_path")
    if not folder_path:
        folder_path = ensure_output_folder(drpid)
    if not folder_path:
        return "No output folder for this project", 400

    def stream() -> Any:
        for line in generate_download_progress(url, folder_path, drpid, referrer):
            sys.stderr.write(line)
            sys.stderr.flush()
            yield line

    return Response(
        stream(),
        mimetype="text/plain; charset=utf-8",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )
