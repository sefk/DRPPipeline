"""
Flask app for Interactive Collector.

Multi-pane layout: scoreboard (left), Source pane, Linked pane.
Links open in the Linked pane so you can see where you came from.

When the pipeline DB is available (config DRP_DB_PATH or default drp_pipeline.db),
the app loads the first eligible project (prereq=sourcing, no errors) on start,
and supports Next (next eligible) and Load by DRPID.

When the original source page is retrieved and we have a DRPID, an output folder
is created (or emptied) and folder_path is stored in _result and in Storage.
Save button converts checked scoreboard pages to PDF in that folder.
"""

import html
import json
import logging
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urljoin

import requests
from flask import Flask, redirect, request, render_template_string, send_from_directory, url_for

from interactive_collector.collector_state import get_result_by_drpid as _get_result_by_drpid
from interactive_collector.collector_state import get_scoreboard as _get_scoreboard
from utils.Args import Args
from utils.file_utils import create_output_folder, sanitize_filename
from utils.url_utils import (
    body_looks_like_html,
    body_looks_like_xml,
    fetch_page_body,
    is_non_html_response,
    is_valid_url,
    is_displayable_content_type,
    resolve_catalog_resource_url,
    BROWSER_HEADERS,
)

# When run via Flask (e.g. flask --app interactive_collector.app run), the Typer CLI
# is not used so Args.initialize() is never called. Initialize from config only so
# db_path, base_output_dir, etc. are loaded from config.json (default ./config.json).
if not getattr(Args, "_initialized", False):
    Args.initialize_from_config()

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100MB for extension PDF uploads
# Register API blueprints for SPA.
from interactive_collector.api import api_bp
from interactive_collector.api_pipeline import pipeline_bp
app.register_blueprint(api_bp)
app.register_blueprint(pipeline_bp)


class _QuietRequestLogFilter(logging.Filter):
    """Suppress Werkzeug request logs for noisy polling endpoints."""

    _QUIET_PATHS = ("/api/scoreboard", "/api/downloads-watcher/status", "/api/metadata-from-page")

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
            return not any(path in msg for path in self._QUIET_PATHS)
        except Exception:
            return True


logging.getLogger("werkzeug").addFilter(_QuietRequestLogFilter())

# Shared state: use collector_state so legacy routes and API share the same data.
_scoreboard = _get_scoreboard()
_result_by_drpid = _get_result_by_drpid()

# Default DB path when not set by orchestrator (standalone run).
DEFAULT_DB_PATH = "drp_pipeline.db"
# Default base output dir when not set by orchestrator (Windows).
DEFAULT_BASE_OUTPUT_DIR = r"C:\Documents\DataRescue\DRPData"

# Content-Type to file extension for downloads (lowercase type -> extension with dot).
_CONTENT_TYPE_EXT: Dict[str, str] = {
    "application/pdf": ".pdf",
    "text/csv": ".csv",
    "application/csv": ".csv",
    "application/zip": ".zip",
    "application/x-zip-compressed": ".zip",
    "application/json": ".json",
    "application/xml": ".xml",
    "text/xml": ".xml",
    "text/plain": ".txt",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
}


def _get_db_path() -> Path:
    """Return db path from Args (when pipeline has initialized it) or default."""
    try:
        from utils.Args import Args
        if getattr(Args, "_initialized", False):
            return Path(Args.db_path)
    except Exception:
        pass
    return Path(DEFAULT_DB_PATH)


def _get_base_output_dir() -> Path:
    """Return base output dir from Args (when pipeline has initialized it) or default."""
    try:
        from utils.Args import Args
        if getattr(Args, "_initialized", False):
            return Path(Args.base_output_dir)
    except Exception:
        pass
    return Path(DEFAULT_BASE_OUTPUT_DIR)


def _ensure_storage(flask_app: Flask) -> None:
    """
    Initialize Storage if not already, using Args.db_path or default.
    Ensures Logger is initialized first (Storage implementations use Logger).
    Idempotent; safe to call on every request that needs Storage.
    """
    from storage import Storage
    try:
        Storage.list_eligible_projects(None, 0)
    except RuntimeError:
        # Logger is used by StorageSQLLite; initialize if not already (e.g. standalone run)
        try:
            from utils.Logger import Logger
            if not getattr(Logger, "_initialized", False):
                Logger.initialize(log_level="WARNING")
        except Exception:
            pass
        path = _get_db_path()
        Storage.initialize("StorageSQLLite", db_path=path)


def _get_first_eligible(flask_app: Flask) -> Optional[Dict[str, Any]]:
    """Return the first eligible project (prereq=sourcing, no errors) or None."""
    _ensure_storage(flask_app)
    from storage import Storage
    projects = Storage.list_eligible_projects("sourced", 1)
    return projects[0] if projects else None


def _get_next_eligible_after(flask_app: Flask, current_drpid: int) -> Optional[Dict[str, Any]]:
    """Return the next eligible project after current_drpid, or None."""
    _ensure_storage(flask_app)
    from storage import Storage
    # Fetch a chunk and find first with DRPID > current_drpid
    projects = Storage.list_eligible_projects("sourced", 200)
    for proj in projects:
        if proj["DRPID"] > current_drpid:
            return proj
    return None


def _get_project_by_drpid(flask_app: Flask, drpid: int) -> Optional[Dict[str, Any]]:
    """Return the project record for the given DRPID, or None."""
    _ensure_storage(flask_app)
    from storage import Storage
    return Storage.get(drpid)


def _ensure_output_folder_for_drpid(flask_app: Flask, drpid: int) -> Optional[str]:
    """
    Create or empty the output folder for this DRPID; store in _result (no DB update yet).
    Returns folder_path string or None if creation failed.
    """
    if drpid in _result_by_drpid and _result_by_drpid[drpid].get("folder_path"):
        return _result_by_drpid[drpid]["folder_path"]
    base_path = _get_base_output_dir()
    folder_path = create_output_folder(base_path, drpid)
    if not folder_path:
        return None
    path_str = str(folder_path)
    _result_by_drpid[drpid] = {"folder_path": path_str}
    return path_str


def _normalize_date_yyyy_mm_dd(s: str) -> Optional[str]:
    """Convert a date string to YYYY-MM-DD for display; return None if not parseable."""
    s = (s or "").strip()
    if not s:
        return None
    if re.match(r"^\d{4}$", s):
        return f"{s}-01-01"
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            dt = date(y, mo, d)
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
    try:
        dt = datetime.strptime(s[:10], "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        pass
    return None


def _folder_path_for_drpid(display_drpid: Optional[str]) -> Optional[str]:
    """Return folder_path from _result_by_drpid for the given display_drpid, or None."""
    if not display_drpid:
        return None
    try:
        drpid = int(display_drpid)
        return _result_by_drpid.get(drpid, {}).get("folder_path")
    except (ValueError, TypeError):
        return None


from utils.file_utils import folder_extensions_and_size, format_file_size
from interactive_collector.pdf_utils import page_title_or_h1, unique_pdf_basename

# Re-export for tests that import from app
_folder_extensions_and_size = folder_extensions_and_size
_format_file_size = format_file_size
_unique_pdf_basename = unique_pdf_basename


_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Interactive Collector</title>
<style>
  body { margin: 0; font-family: sans-serif; }
  .top { padding: 8px; background: #eee; border-bottom: 1px solid #ccc; }
  .top .drpid { font-weight: bold; margin-right: 4px; }
  .top .top-sep { margin: 0 8px; color: #999; }
  .top input[type="url"] { width: 50%; min-width: 300px; }
  .main { display: flex; height: calc(100vh - 50px); }
  .left-col { display: flex; flex-direction: column; width: 440px; min-width: 180px; max-width: 50%; background: #fafafa; min-height: 0; flex-shrink: 0; }
  .scoreboard { flex: 0 0 auto; height: 200px; min-height: 60px; overflow: auto; padding: 8px; font-size: 12px; }
  .scoreboard h3 { margin: 0 0 8px 0; }
  .splitter { flex-shrink: 0; background: #e0e0e0; transition: background 0.1s; }
  .splitter:hover { background: #b0b0b0; }
  .splitter-v { width: 6px; cursor: col-resize; min-width: 6px; }
  .splitter-h { height: 6px; cursor: row-resize; min-height: 6px; }
  .metadata-pane { flex: 1; min-height: 120px; overflow: auto; padding: 8px; font-size: 12px; }
  .metadata-pane h3 { margin: 0 0 8px 0; }
  .metadata-pane label { display: block; margin-top: 8px; margin-bottom: 2px; font-weight: 600; }
  .metadata-pane input[type="text"], .metadata-pane input[type="date"] { width: 100%; box-sizing: border-box; padding: 4px; max-width: 100%; }
  .metadata-pane textarea { width: 100%; box-sizing: border-box; padding: 4px; padding-bottom: 14px; max-width: 100%; resize: none; font-family: inherit; font-size: 12px; min-height: 2.5em; }
  .metadata-richtext-wrap { width: 100%; box-sizing: border-box; max-width: 100%; border: 1px solid #ccc; border-radius: 2px; background: #fff; position: relative; }
  .metadata-richtext-toolbar { padding: 2px 4px; border-bottom: 1px solid #ccc; background: #f0f0f0; display: flex; gap: 2px; flex-wrap: wrap; }
  .metadata-richtext-toolbar button { padding: 2px 8px; font-size: 12px; cursor: pointer; border: 1px solid #aaa; background: #fff; border-radius: 2px; }
  .metadata-richtext-toolbar button:hover { background: #e8e8e8; }
  .metadata-richtext-editor { width: 100%; box-sizing: border-box; min-height: 60px; height: 200px; overflow-y: auto; padding: 6px 8px; padding-bottom: 14px; font-family: inherit; font-size: 12px; outline: none; }
  .metadata-richtext-editor:empty::before { content: attr(data-placeholder); color: #999; }
  .metadata-pane .metadata-richtext { width: 100%; box-sizing: border-box; min-height: 60px; padding: 4px; font-family: inherit; }
  .resizer-wrap { position: relative; display: inline-block; width: 100%; }
  .triangle-resizer { position: absolute; right: 2px; bottom: 2px; width: 0; height: 0; border-left: 6px solid transparent; border-top: 8px solid #888; cursor: nwse-resize; z-index: 1; }
  .triangle-resizer:hover { border-top-color: #555; }
  .metadata-keywords-wrap { position: relative; width: 100%; }
  .scoreboard ul { list-style: none; padding-left: 12px; margin: 0; }
  .scoreboard li { margin: 4px 0; word-break: break-all; }
  .scoreboard .url { color: #06c; }
  .scoreboard a.url { text-decoration: none; }
  .scoreboard a.url:hover { text-decoration: underline; }
  .scoreboard .status-404 { color: #c00; }
  .scoreboard .status-ok { color: #080; }
  .scoreboard-cb { margin-right: 6px; vertical-align: middle; }
  .btn-save { background: #c00; color: white; border: none; padding: 4px 10px; cursor: pointer; font-size: 12px; border-radius: 3px; margin-left: 8px; }
  .btn-save:hover { background: #a00; }
  #save-progress-modal { display: none; position: fixed; z-index: 9999; left: 0; top: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); }
  #save-progress-modal.show { display: flex; align-items: center; justify-content: center; }
  #save-progress-dialog { background: white; padding: 24px; border-radius: 8px; max-width: 90%; min-width: 320px; box-shadow: 0 4px 20px rgba(0,0,0,0.3); }
  #save-progress-message { margin: 12px 0; white-space: pre-wrap; word-break: break-all; }
  #save-progress-ok { margin-top: 16px; padding: 8px 20px; cursor: pointer; display: none; }
  .panes { flex: 1; display: flex; flex-direction: row; min-width: 0; }
  .pane { flex: 1; min-width: 120px; min-height: 0; border: 1px solid #ccc; margin: 4px; display: flex; flex-direction: column; overflow: hidden; }
  .pane.source-pane { resize: horizontal; max-width: 80%; }
  .pane-header { padding: 4px 8px; background: #e8e8e8; font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .pane-header-label { font-weight: bold; }
  .pane-back { margin-right: 6px; font-weight: normal; }
  .pane-header-sep { margin-right: 4px; }
  .pane-header-url { font-weight: normal; }
  .pane-iframe { flex: 1; width: 100%; border: none; min-height: 200px; }
  .pane-empty { flex: 1; padding: 16px; color: #666; font-size: 14px; }
</style>
</head>
<body>
  <div class="top">
    {% if drpid is not none %}
    <span class="drpid">DRPID: {{ drpid }}</span>
    <span class="top-sep">|</span>
    <form method="get" action="/" style="display:inline;" class="top-form">
      <input type="hidden" name="next" value="1" />
      <input type="hidden" name="current_drpid" value="{{ drpid }}" />
      <button type="submit">Next</button>
    </form>
    <span class="top-sep">|</span>
    {% endif %}
    <form method="get" action="/" style="display:inline;" class="top-form">
      <label for="url">URL:</label>
      <input type="url" id="url" name="url" value="{{ initial_url or '' }}" size="60" placeholder="https://example.com" />
      <button type="submit">Go</button>
    </form>
    <span class="top-sep">|</span>
    <form method="get" action="/" style="display:inline;" class="top-form">
      <label for="load_drpid">Load DRPID:</label>
      <input type="number" id="load_drpid" name="load_drpid" value="" placeholder="e.g. 1" min="1" style="width:70px;" />
      <button type="submit">Load</button>
    </form>
  </div>
  <div class="main">
    <div class="left-col" id="left-col">
      <div class="scoreboard" id="scoreboard">
        <h3>Scoreboard{% if folder_path %} <button type="submit" form="scoreboard-save-form" class="btn-save">Save</button>{% endif %}</h3>
        {% if folder_path %}
        <form id="scoreboard-save-form" method="post" action="{{ url_for('save') }}" onsubmit="return false;">
          <input type="hidden" name="drpid" value="{{ drpid or '' }}" />
          <input type="hidden" name="folder_path" value="{{ (folder_path or '') | e }}" />
          <input type="hidden" name="scoreboard_urls_json" value="{{ scoreboard_urls_json | e }}" />
          <input type="hidden" name="return_to" id="save-return-to" value="" />
          {{ scoreboard_html | safe }}
        </form>
        {% else %}
        {{ scoreboard_html | safe }}
        {% endif %}
      </div>
      <div class="splitter splitter-h" id="splitter-scoreboard" title="Drag to resize scoreboard"></div>
      <div class="metadata-pane">
        <h3>Metadata</h3>
        <label for="metadata-title">Title</label>
        <input type="text" id="metadata-title" name="metadata_title" form="scoreboard-save-form" value="{{ (metadata_title or '') | e }}" />
        <label for="metadata-summary-editor">Description</label>
        <input type="hidden" id="metadata-summary" name="metadata_summary" form="scoreboard-save-form" value="" />
          <div class="metadata-richtext-wrap resizer-wrap" id="summary-wrap">
            <div class="metadata-richtext-toolbar" aria-label="Formatting">
              <button type="button" data-cmd="bold" title="Bold">B</button>
              <button type="button" data-cmd="italic" title="Italic">I</button>
              <button type="button" data-cmd="underline" title="Underline">U</button>
              <button type="button" data-cmd="insertUnorderedList" title="Bullet list">• List</button>
            </div>
            <div id="metadata-summary-editor" class="metadata-richtext-editor" contenteditable="true" data-placeholder="Summary" role="textbox"></div>
            <div class="triangle-resizer" id="resize-summary" title="Drag to resize" aria-label="Resize"></div>
          </div>
          <script>
          (function() {
            var editor = document.getElementById("metadata-summary-editor");
            var hidden = document.getElementById("metadata-summary");
            var initial = {{ (metadata_summary or '') | tojson }};
            if (initial && typeof initial === "string") editor.innerHTML = initial;
            function sync() { if (hidden) hidden.value = editor.innerHTML; }
            editor.addEventListener("input", sync);
            editor.addEventListener("blur", sync);
            document.querySelectorAll(".metadata-richtext-toolbar button").forEach(function(btn) {
              btn.addEventListener("click", function() {
                var cmd = this.getAttribute("data-cmd");
                editor.focus();
                document.execCommand(cmd, false, null);
                sync();
              });
            });
            sync();
          })();
          </script>
          <label for="metadata-keywords">Keywords</label>
          <div class="metadata-keywords-wrap" id="keywords-wrap">
            <textarea id="metadata-keywords" name="metadata_keywords" form="scoreboard-save-form" rows="2">{{ (metadata_keywords or '') | e }}</textarea>
            <div class="triangle-resizer" id="resize-keywords" title="Drag to resize" aria-label="Resize"></div>
          </div>
          <script>
          (function() {
            var kw = document.getElementById("metadata-keywords");
            if (kw) kw.addEventListener("paste", function(e) {
              e.preventDefault();
              var text = (e.clipboardData || window.clipboardData).getData("text/plain") || "";
              var trimmed = text.replace(/\\s+/g, " ").trim();
              if (/\\s/.test(trimmed) && !/[;,]/.test(trimmed)) {
                var parts = trimmed.split(/\\s+/).filter(Boolean);
                text = parts.join("; ");
              }
              var start = kw.selectionStart, end = kw.selectionEnd, val = kw.value;
              kw.value = val.slice(0, start) + text + val.slice(end);
              kw.selectionStart = kw.selectionEnd = start + text.length;
            });
          })();
          </script>
          <label for="metadata-agency">Agency</label>
          <input type="text" id="metadata-agency" name="metadata_agency" form="scoreboard-save-form" value="{{ (metadata_agency or '') | e }}" />
          <label for="metadata-office">Office</label>
          <input type="text" id="metadata-office" name="metadata_office" form="scoreboard-save-form" value="{{ (metadata_office or '') | e }}" />
          <label for="metadata-time-start">Start Date</label>
          <input type="text" id="metadata-time-start" name="metadata_time_start" form="scoreboard-save-form" value="{{ (metadata_time_start or '') | e }}" placeholder="YYYY-MM-DD or YYYY" />
          <label for="metadata-time-end">End Date</label>
          <input type="text" id="metadata-time-end" name="metadata_time_end" form="scoreboard-save-form" value="{{ (metadata_time_end or '') | e }}" placeholder="YYYY-MM-DD or YYYY" />
          <label for="metadata-download-date">Download date</label>
          <input type="text" id="metadata-download-date" name="metadata_download_date" form="scoreboard-save-form" value="{{ (metadata_download_date or '') | e }}" placeholder="YYYY-MM-DD" />
          <script>
          (function() {
            var KEY_PREFIX = "metadata_draft_";
            function getDrpid() {
              var el = document.querySelector("input[name=\"drpid\"]");
              return el ? (el.value || "").trim() : "";
            }
            function getDraft() {
              var drpid = getDrpid();
              if (!drpid) return null;
              try {
                var raw = sessionStorage.getItem(KEY_PREFIX + drpid);
                return raw ? JSON.parse(raw) : null;
              } catch (e) { return null; }
            }
            function saveDraft() {
              var drpid = getDrpid();
              if (!drpid) return;
              var summaryEl = document.getElementById("metadata-summary-editor");
              var hidden = document.getElementById("metadata-summary");
              var summary = (summaryEl ? summaryEl.innerHTML : "") || (hidden ? hidden.value : "");
              var draft = {
                title: (document.getElementById("metadata-title") && document.getElementById("metadata-title").value) || "",
                summary: summary,
                keywords: (document.getElementById("metadata-keywords") && document.getElementById("metadata-keywords").value) || "",
                agency: (document.getElementById("metadata-agency") && document.getElementById("metadata-agency").value) || "",
                office: (document.getElementById("metadata-office") && document.getElementById("metadata-office").value) || "",
                time_start: (document.getElementById("metadata-time-start") && document.getElementById("metadata-time-start").value) || "",
                time_end: (document.getElementById("metadata-time-end") && document.getElementById("metadata-time-end").value) || "",
                download_date: (document.getElementById("metadata-download-date") && document.getElementById("metadata-download-date").value) || ""
              };
              try { sessionStorage.setItem(KEY_PREFIX + drpid, JSON.stringify(draft)); } catch (e) {}
            }
            function restoreDraft() {
              var draft = getDraft();
              if (!draft) return;
              var titleEl = document.getElementById("metadata-title");
              if (titleEl && draft.title !== undefined) titleEl.value = draft.title;
              var summaryEl = document.getElementById("metadata-summary-editor");
              var hidden = document.getElementById("metadata-summary");
              if (draft.summary !== undefined) {
                if (summaryEl) summaryEl.innerHTML = draft.summary;
                if (hidden) hidden.value = draft.summary;
              }
              var kw = document.getElementById("metadata-keywords");
              if (kw && draft.keywords !== undefined) kw.value = draft.keywords;
              var agency = document.getElementById("metadata-agency");
              if (agency && draft.agency !== undefined) agency.value = draft.agency;
              var office = document.getElementById("metadata-office");
              if (office && draft.office !== undefined) office.value = draft.office;
              var ts = document.getElementById("metadata-time-start");
              if (ts && draft.time_start !== undefined) ts.value = draft.time_start;
              var te = document.getElementById("metadata-time-end");
              if (te && draft.time_end !== undefined) te.value = draft.time_end;
              var dd = document.getElementById("metadata-download-date");
              if (dd && draft.download_date !== undefined) dd.value = draft.download_date;
            }
            restoreDraft();
            var form = document.getElementById("scoreboard-save-form");
            if (form) {
              form.addEventListener("input", saveDraft);
              form.addEventListener("change", saveDraft);
            }
            var editor = document.getElementById("metadata-summary-editor");
            if (editor) editor.addEventListener("input", saveDraft);
          })();
          </script>
      </div>
    </div>
    <div class="splitter splitter-v" id="splitter-left-col" title="Drag to resize left column"></div>
    <div class="panes">
      <div class="pane source-pane">
        <div class="pane-header" title="{{ (source_display_url or '') | e }}">Source: {{ (source_display_url or '—') | e }}</div>
        {% if source_srcdoc %}
        <iframe name="source" class="pane-iframe" srcdoc="{{ source_srcdoc | safe }}" sandbox="allow-same-origin allow-scripts allow-forms allow-top-navigation-by-user-activation" title="Source page"></iframe>
        {% else %}
        <div class="pane-empty">{{ source_pane_message or "Enter a URL and click Go, or click a link to open it in the Linked pane." }}</div>
        {% endif %}
      </div>
      <div class="pane">
        <div class="pane-header" title="{{ (linked_display_url or '') | e }}"><span class="pane-header-label">Linked:</span>{% if linked_back_url %} <a href="{{ linked_back_url | e }}" class="pane-back" title="Back to previous page">← Back</a><span class="pane-header-sep"> </span>{% endif %}<span class="pane-header-url">{{ (linked_display_url or '—') | e }}</span></div>
        {% if linked_srcdoc %}
        <iframe name="linked" class="pane-iframe" srcdoc="{{ linked_srcdoc | safe }}" sandbox="allow-same-origin allow-scripts allow-forms allow-top-navigation-by-user-activation" title="Linked page"></iframe>
        {% else %}
        <div class="pane-empty">
          {{ linked_pane_message or "Click a link in Source (or Linked) to open it here." }}
        </div>
        {% endif %}
      </div>
    </div>
  </div>
  <script>
  (function() {
    function dragSplitter(splitterEl, onMove) {
      if (!splitterEl) return;
      splitterEl.addEventListener("mousedown", function(e) {
        if (e.button !== 0) return;
        e.preventDefault();
        function move(e2) { onMove(e2); }
        function stop() { document.removeEventListener("mousemove", move); document.removeEventListener("mouseup", stop); document.body.style.cursor = ""; document.body.style.userSelect = ""; }
        document.body.style.cursor = getComputedStyle(splitterEl).cursor;
        document.body.style.userSelect = "none";
        document.addEventListener("mousemove", move);
        document.addEventListener("mouseup", stop);
      });
    }
    var leftCol = document.getElementById("left-col");
    var main = leftCol && leftCol.closest(".main");
    dragSplitter(document.getElementById("splitter-left-col"), function(e) {
      if (!leftCol || !main) return;
      var r = main.getBoundingClientRect();
      var w = Math.max(180, Math.min(r.width * 0.5, e.clientX - r.left));
      leftCol.style.width = w + "px";
    });
    var scoreboard = document.getElementById("scoreboard");
    dragSplitter(document.getElementById("splitter-scoreboard"), function(e) {
      if (!scoreboard) return;
      var r = scoreboard.getBoundingClientRect();
      var h = Math.max(60, e.clientY - r.top);
      scoreboard.style.height = h + "px";
    });
    function dragTriangleResizer(gripId, targetId, minH, setHeight) {
      var grip = document.getElementById(gripId);
      var target = document.getElementById(targetId);
      if (!grip || !target) return;
      grip.addEventListener("mousedown", function(e) {
        if (e.button !== 0) return;
        e.preventDefault();
        var startY = e.clientY, startH = target.offsetHeight;
        function move(e2) {
          var dy = e2.clientY - startY;
          var newH = Math.max(minH, startH + dy);
          target.style.height = newH + "px";
          if (setHeight) setHeight(target, newH);
        }
        function stop() {
          document.removeEventListener("mousemove", move);
          document.removeEventListener("mouseup", stop);
          document.body.style.cursor = "";
          document.body.style.userSelect = "";
        }
        document.body.style.cursor = "nwse-resize";
        document.body.style.userSelect = "none";
        document.addEventListener("mousemove", move);
        document.addEventListener("mouseup", stop);
      });
    }
    dragTriangleResizer("resize-summary", "metadata-summary-editor", 60, function(el, h) { el.style.maxHeight = h + "px"; });
    dragTriangleResizer("resize-keywords", "metadata-keywords", 28);
  })();
  </script>
  <script>
  (function() {
    var dirty = false;
    var savingMetadata = false;
    var saveForm = document.getElementById("scoreboard-save-form");
    if (saveForm) {
      saveForm.addEventListener("submit", function() { savingMetadata = true; });
      ["input", "change"].forEach(function(ev) {
        saveForm.addEventListener(ev, function() { dirty = true; }, true);
      });
    }
    var editor = document.getElementById("metadata-summary-editor");
    if (editor) editor.addEventListener("input", function() { dirty = true; });
    window.addEventListener("beforeunload", function(e) {
      if (dirty && !savingMetadata) e.returnValue = "Are you sure you want to leave? Your changes may not be saved.";
    });
    document.querySelectorAll("form[method=\"get\"]").forEach(function(f) {
      if (f.querySelector("input[name=\"url\"]") || f.querySelector("input[name=\"next\"]") || f.querySelector("input[name=\"load_drpid\"]")) {
        f.addEventListener("submit", function(ev) {
          if (dirty && !savingMetadata && !confirm("Are you sure you want to leave? Your changes may not be saved.")) ev.preventDefault();
        });
      }
    });
  })();
  </script>
  <div id="save-progress-modal">
    <div id="save-progress-dialog">
      <strong>Saving PDFs</strong>
      <div id="save-progress-message">Starting...</div>
      <button type="button" id="save-progress-ok">Close</button>
    </div>
  </div>
  {% if show_auto_download %}
  <span id="auto-download-data" data-url="{{ linked_binary_url | e }}" data-drpid="{{ (drpid or '') | e }}" data-referrer="{{ (linked_binary_referrer or '') | e }}" style="display:none"></span>
  <script>
  (function(){
    var el = document.getElementById("auto-download-data");
    if (!el) return;
    var url = el.getAttribute("data-url") || "";
    var drpid = (el.getAttribute("data-drpid") || "").trim();
    var referrer = el.getAttribute("data-referrer") || "";
    if (!url || !drpid) return;
    var modal = document.getElementById("save-progress-modal");
    var msg = document.getElementById("save-progress-message");
    var closeBtn = document.getElementById("save-progress-ok");
    if (!modal || !msg) return;
    var titleEl = modal.querySelector("strong");
    if (titleEl) titleEl.textContent = "Downloading file";
    modal.classList.add("show");
    if (closeBtn) closeBtn.style.display = "none";
    msg.textContent = "Downloading...";
    var reloadOnClose = false;
    if (closeBtn) closeBtn.onclick = function() {
      modal.classList.remove("show");
      if (reloadOnClose) {
        var u = new URL(window.location.href);
        u.searchParams.set("downloaded", "1");
        window.location.href = u.toString();
      }
    };
    var fd = new FormData();
    fd.append("url", url);
    fd.append("drpid", drpid);
    fd.append("referrer", referrer);
    fetch("{{ url_for('download_file') }}", { method: "POST", body: fd })
      .then(function(res) {
        if (!res.ok) {
          return res.text().then(function(t) {
            msg.textContent = "Error " + res.status + ": " + (t || res.statusText || "request failed");
            if (closeBtn) closeBtn.style.display = "inline-block";
          });
        }
        if (!res.body) throw new Error("No body");
        var reader = res.body.getReader();
        var decoder = new TextDecoder();
        var buf = "";
        function read() {
          return reader.read().then(function(r) {
            if (r.done) return;
            buf += decoder.decode(r.value, { stream: true });
            var lines = buf.split("\\n");
            buf = lines.pop();
            for (var i = 0; i < lines.length; i++) {
              var line = lines[i];
              if (line.startsWith("SAVING\\t")) msg.textContent = "Downloading " + (line.split("\\t")[1] || "");
              else if (line.startsWith("PROGRESS\\t")) {
                var p = line.split("\\t");
                var w = parseInt(p[1], 10);
                var t = p[2];
                msg.textContent = t ? "Downloaded " + (w < 1024 ? w + " B" : (w < 1024*1024 ? (w/1024).toFixed(1) + " KB" : (w/(1024*1024)).toFixed(1) + " MB")) + " / " + (parseInt(t,10) < 1024*1024 ? (parseInt(t,10)/1024).toFixed(1) + " KB" : (parseInt(t,10)/(1024*1024)).toFixed(1) + " MB") : "Downloaded " + (w < 1024 ? w + " B" : (w/(1024*1024)).toFixed(1) + " MB");
              } else if (line.startsWith("DONE\\t")) {
                var doneParts = line.split("\\t");
                var savedName = doneParts[1] ? doneParts[1].trim() : "";
                var extDisplay = (doneParts[3] && doneParts[3].trim()) ? " " + doneParts[3].trim() : "";
                msg.textContent = savedName ? "Download complete. Saved as " + savedName : "Download complete.";
                if (closeBtn) closeBtn.style.display = "inline-block";
                var parentLi = null;
                var urls = document.querySelectorAll(".scoreboard .url");
                for (var i = 0; i < urls.length; i++) {
                  if (urls[i].getAttribute("title") === referrer) {
                    parentLi = urls[i].closest("li");
                    break;
                  }
                }
                var ul = parentLi ? (parentLi.querySelector("ul") || (function() { var u = document.createElement("ul"); parentLi.appendChild(u); return u; })()) : document.querySelector(".scoreboard ul");
                if (ul) {
                  var li = document.createElement("li");
                  var spanUrl = document.createElement("span");
                  spanUrl.className = "url";
                  spanUrl.title = savedName;
                  spanUrl.textContent = savedName || "file";
                  var spanStatus = document.createElement("span");
                  spanStatus.className = "status-ok";
                  spanStatus.textContent = "(DL" + extDisplay + ")";
                  li.appendChild(spanUrl);
                  li.appendChild(document.createTextNode(" "));
                  li.appendChild(spanStatus);
                  ul.appendChild(li);
                } else {
                  reloadOnClose = true;
                }
                return;
              } else if (line.startsWith("ERROR\\t")) {
                msg.textContent = "Error: " + (line.split("\\t")[1] || "unknown");
                if (closeBtn) closeBtn.style.display = "inline-block";
                return;
              }
            }
            return read();
          });
        }
        return read();
      })
      .catch(function(e) {
        msg.textContent = "Error: " + (e.message || "request failed");
        if (closeBtn) closeBtn.style.display = "inline-block";
      });
  })();
  </script>
  {% endif %}
  <script>
  (function() {
    var form = document.getElementById("scoreboard-save-form");
    var modal = document.getElementById("save-progress-modal");
    var messageEl = document.getElementById("save-progress-message");
    var okBtn = document.getElementById("save-progress-ok");
    var reloadOnClose = false;

    function showModal(title) {
      if (!modal) { alert(title || "Progress"); return; }
      var strong = modal.querySelector("strong");
      if (strong) strong.textContent = title || "Progress";
      modal.classList.add("show");
      if (okBtn) okBtn.style.display = "none";
    }
    function hideModal() {
      if (modal) modal.classList.remove("show");
      if (reloadOnClose) {
        reloadOnClose = false;
        window.location.reload();
      }
    }

    if (form) {
      form.addEventListener("submit", function(ev) {
        ev.preventDefault();
        var returnTo = document.getElementById("save-return-to");
        if (returnTo) returnTo.value = window.location.search || "";
        showModal("Saving PDFs");
        messageEl.textContent = "Starting...";
        var formData = new FormData(form);
        fetch("{{ url_for('save') }}", { method: "POST", body: formData, redirect: "follow" })
          .then(function(res) {
            if (res.redirected && res.url) {
              window.location.href = res.url;
              return;
            }
            if (!res.body) throw new Error("No body");
            var reader = res.body.getReader();
            var decoder = new TextDecoder();
            var buf = "";
            function read() {
              return reader.read().then(function(r) {
                if (r.done) return;
                buf += decoder.decode(r.value, { stream: true });
                var lines = buf.split("\\n");
                buf = lines.pop();
                for (var i = 0; i < lines.length; i++) {
                  var line = lines[i];
                  if (line.startsWith("SAVING\\t")) {
                    var parts = line.split("\\t");
                    if (parts.length >= 4) messageEl.textContent = "Saving " + parts[1] + " " + parts[2] + "/" + parts[3];
                  } else if (line.startsWith("SUMMARY\\t")) {
                    try {
                      var summary = JSON.parse(line.slice(8));
                      var s = "Downloaded files:\\n";
                      for (var j = 0; j < summary.length; j++) {
                        var f = summary[j];
                        var sz = (f.size !== undefined) ? (f.size < 1024 ? f.size + " B" : (f.size < 1024*1024 ? (f.size/1024).toFixed(1) + " KB" : (f.size/(1024*1024)).toFixed(1) + " MB") : "";
                        s += (f.filename || f.path || "?") + " — " + sz + "\\n";
                      }
                      messageEl.textContent = s;
                    } catch (_) {}
                  } else if (line.startsWith("TOTAL_SIZE\\t")) {
                    var total = parseInt(line.split("\\t")[1], 10) || 0;
                    var totalStr = total < 1024 ? total + " B" : (total < 1024*1024 ? (total/1024).toFixed(1) + " KB" : (total/(1024*1024)).toFixed(1) + " MB");
                    messageEl.textContent = (messageEl.textContent || "") + "\\nTotal size: " + totalStr;
                  } else if (line.startsWith("DONE\\t")) {
                    var n = line.split("\\t")[1] || "0";
                    messageEl.textContent = (messageEl.textContent ? messageEl.textContent + "\\n\\n" : "") + "Saved " + n + " file(s).";
                    okBtn.style.display = "inline-block";
                    return;
                  } else if (line.startsWith("ERROR\\t")) {
                    messageEl.textContent = "Error: " + (line.split("\\t")[1] || "unknown");
                    okBtn.style.display = "inline-block";
                    return;
                  }
                }
                return read();
              });
            }
            return read();
          })
          .catch(function(e) {
            messageEl.textContent = "Error: " + (e.message || "request failed");
            okBtn.style.display = "inline-block";
          });
      });
    }

    function runDownloadBinary(btn) {
      window.__downloadBinaryClicked = true;
      try {
        showModal("Downloading file");
      } catch (e) {
        alert("Download: " + (e.message || e));
        return;
      }
      if (!modal || !messageEl) {
        alert("Downloading file (modal not found)");
        return;
      }
      if (!btn) return;
      var url = btn.getAttribute("data-url");
      var referrer = btn.getAttribute("data-referrer") || "";
      var drpid = (btn.getAttribute("data-drpid") || "").trim();
      if (!drpid) {
        var el = document.querySelector('input[name="drpid"]');
        drpid = el ? (el.value || "").trim() : "";
      }
      if (!url || !drpid) {
        messageEl.textContent = "No project (DRPID) set. Load a project (Load DRPID) first.";
        if (okBtn) okBtn.style.display = "inline-block";
        return;
      }
      messageEl.textContent = "Downloading...";
      var fd = new FormData();
      fd.append("url", url);
      fd.append("drpid", drpid);
      fd.append("referrer", referrer);
      var downloadUrl = "{{ url_for('download_file') }}";
      fetch(downloadUrl, { method: "POST", body: fd })
        .then(function(res) {
          if (!res.ok) {
            return res.text().then(function(t) {
              messageEl.textContent = "Error " + res.status + ": " + (t || res.statusText || "request failed");
              okBtn.style.display = "inline-block";
            });
          }
          if (!res.body) throw new Error("No body");
          var reader = res.body.getReader();
          var decoder = new TextDecoder();
          var buf = "";
          function read() {
            return reader.read().then(function(r) {
              if (r.done) return;
              buf += decoder.decode(r.value, { stream: true });
              var lines = buf.split("\\n");
              buf = lines.pop();
              for (var i = 0; i < lines.length; i++) {
                var line = lines[i];
                if (line.startsWith("SAVING\\t")) messageEl.textContent = "Downloading " + line.split("\\t")[1];
                else if (line.startsWith("PROGRESS\\t")) {
                  var p = line.split("\\t");
                  var w = parseInt(p[1], 10);
                  var t = p[2];
                  messageEl.textContent = t ? "Downloaded " + (w < 1024 ? w + " B" : (w < 1024*1024 ? (w/1024).toFixed(1) + " KB" : (w/(1024*1024)).toFixed(1) + " MB")) + " / " + (parseInt(t,10) < 1024*1024 ? (parseInt(t,10)/1024).toFixed(1) + " KB" : (parseInt(t,10)/(1024*1024)).toFixed(1) + " MB") : "Downloaded " + (w < 1024 ? w + " B" : (w/(1024*1024)).toFixed(1) + " MB");
                } else if (line.startsWith("DONE\\t")) {
                  messageEl.textContent = "Download complete.";
                  okBtn.style.display = "inline-block";
                  reloadOnClose = true;
                  return;
                } else if (line.startsWith("ERROR\\t")) {
                  messageEl.textContent = "Error: " + (line.split("\\t")[1] || "unknown");
                  okBtn.style.display = "inline-block";
                  return;
                }
              }
              return read();
            });
          }
          return read();
        })
        .catch(function(e) {
          messageEl.textContent = "Error: " + (e.message || "request failed");
          okBtn.style.display = "inline-block";
        });
    }
    window.__downloadBinary = runDownloadBinary;

    var downloadBtn = document.getElementById("btn-download-binary");
    if (downloadBtn) {
      downloadBtn.addEventListener("click", function(ev) {
        ev.preventDefault();
        ev.stopPropagation();
        runDownloadBinary(ev.currentTarget);
      });
    }
    document.addEventListener("click", function(ev) {
      var btn = ev.target && ev.target.closest && ev.target.closest(".btn-download-binary");
      if (btn) { ev.preventDefault(); ev.stopPropagation(); runDownloadBinary(btn); }
    }, true);

    if (okBtn) okBtn.addEventListener("click", function() { hideModal(); });
  })();
  </script>
</body>
</html>
"""


def _base_url_for_page(page_url: str) -> str:
    """Return the base URL (directory) for resolving relative URLs on the page."""
    if page_url.endswith("/"):
        return page_url
    return urljoin(page_url + "/", "..")


def _inject_base_into_html(html_body: str, page_url: str) -> str:
    """
    Inject <base href="..."> so relative CSS/JS/images load in the iframe.

    Tries: (1) inside first <head>; (2) after <html> as new <head>; (3) prepend at start.
    """
    base_href = _base_url_for_page(page_url)
    if not base_href.endswith("/"):
        base_href = base_href + "/"
    base_escaped = base_href.replace("&", "&amp;").replace('"', "&quot;")
    base_tag = f'<base href="{base_escaped}">'

    # (1) Inject into existing <head>
    with_head = re.sub(r"(<head[^>]*>)", r"\1" + base_tag, html_body, count=1, flags=re.IGNORECASE)
    if with_head != html_body:
        return with_head

    # (2) No <head>: insert <head><base...></head> after <html>
    with_html_head = re.sub(
        r"(<html[^>]*>)",
        r"\1<head>" + base_tag + "</head>",
        html_body,
        count=1,
        flags=re.IGNORECASE,
    )
    if with_html_head != html_body:
        return with_html_head

    # (3) No <html>: prepend base at start so relative URLs still resolve
    return base_tag + html_body


def _rewrite_links_to_app(
    html_body: str,
    page_url: str,
    app_root_url: str,
    source_url: str,
    current_page_url: str,
    drpid: Optional[str] = None,
) -> str:
    """
    Rewrite <a href="..."> so clicks load in the Linked pane. Builds
    ?source_url=...&linked_url=...&referrer=... so the new page opens in Linked
    and both panes are re-rendered. If drpid is set, appends it to preserve current project.
    Only rewrites http/https links.
    """
    def repl(match: re.Match) -> str:
        before_href = match.group(1)
        quote_char = match.group(2)
        href_value = match.group(3).strip()
        after_href = match.group(4)
        if not href_value or href_value.startswith("#") or href_value.startswith("javascript:"):
            return match.group(0)
        if href_value.startswith("mailto:") or href_value.startswith("tel:"):
            return match.group(0)
        absolute_url = urljoin(page_url, href_value)
        if not absolute_url.startswith("http://") and not absolute_url.startswith("https://"):
            return match.group(0)
        # Resolve catalog links when clicked (in the route), not here, to avoid slow page loads.
        params = (
            "source_url=" + quote(source_url, safe="")
            + "&linked_url=" + quote(absolute_url, safe="")
            + "&referrer=" + quote(current_page_url, safe="")
        )
        if drpid:
            params += "&drpid=" + quote(drpid, safe="")
        app_url = app_root_url.rstrip("/") + "/?" + params
        escaped = app_url.replace("&", "&amp;").replace('"', "&quot;")
        return f'<a {before_href} target="_top" href={quote_char}{escaped}{quote_char} {after_href}>'

    return re.sub(
        r"<a\s+([^>]*?)href\s*=\s*([\"'])([^\"']*)\2([^>]*)>",
        repl,
        html_body,
        flags=re.IGNORECASE,
    )


def _extension_from_content_type(content_type: Optional[str]) -> str:
    """Return extension with leading dot from Content-Type, or empty string."""
    if not content_type or ";" in content_type:
        content_type = (content_type or "").split(";")[0].strip().lower()
    return _CONTENT_TYPE_EXT.get(content_type, "")


def _filename_from_content_disposition(header_value: Optional[str]) -> Optional[str]:
    """Extract filename from Content-Disposition header (filename= or filename*=)."""
    if not header_value:
        return None
    # filename*=UTF-8''encoded
    if "filename*=" in header_value:
        try:
            part = header_value.split("filename*=")[-1].strip().strip(";")
            if part.lower().startswith("utf-8''"):
                from urllib.parse import unquote
                return unquote(part[7:])
            return part.strip("\"'")
        except Exception:
            pass
    # filename="name"
    if "filename=" in header_value:
        try:
            part = header_value.split("filename=", 1)[-1].strip().strip(";").strip("\"'")
            if part:
                return part
        except Exception:
            pass
    return None


def _filename_from_url(url: str) -> str:
    """Last path segment or 'download'."""
    from urllib.parse import urlparse, unquote
    path = urlparse(url).path or ""
    name = (path.rstrip("/").split("/")[-1] or "download")
    return unquote(name)


def _unique_download_basename(base: str, ext: str, used: Dict[str, int]) -> str:
    """Return unique sanitized basename: base.ext or base_1.ext, etc."""
    if not base or base == "download":
        base = "download"
    base = sanitize_filename(base, max_length=80)
    if ext and not base.lower().endswith(ext.lower()):
        base = base + ext
    key = base.lower()
    n = used.get(key, 0)
    used[key] = n + 1
    if n == 0:
        return base
    # insert _n before the extension
    if "." in base:
        stem, suffix = base.rsplit(".", 1)
        return f"{stem}_{n}.{suffix}"
    return f"{base}_{n}"


def _status_label(status_code: int, is_logical_404: bool) -> str:
    """Return human-readable status for display."""
    if status_code == 404:
        return "404 (logical)" if is_logical_404 else "404"
    if status_code == 200:
        return "OK"
    if status_code < 0:
        return f"Error ({status_code})"
    return str(status_code)


def _scoreboard_add(url: str, referrer: Optional[str], status_label: str) -> None:
    """Append a node to the in-memory scoreboard. Marks as dupe if this URL is already present at any level."""
    existing_urls = {n["url"] for n in _scoreboard}
    is_dupe = url in existing_urls
    _scoreboard.append({"url": url, "referrer": referrer, "status_label": status_label, "is_dupe": is_dupe})


def _scoreboard_add_download(
    url: str,
    referrer: Optional[str],
    file_path: str,
    file_size: int,
    extension: str,
    filename: Optional[str] = None,
) -> None:
    """Append a downloaded-file entry to the scoreboard (no checkbox, DL flag + extension)."""
    _scoreboard.append({
        "url": url,
        "referrer": referrer,
        "status_label": "DL",
        "is_dupe": False,
        "is_download": True,
        "file_path": file_path,
        "file_size": file_size,
        "extension": extension or "",
        "filename": filename or "",
    })


def _scoreboard_tree() -> List[Dict[str, Any]]:
    """Return scoreboard as a tree. One node per entry (no merging by URL) so original stays OK, dupes shown separately."""
    nodes = [
        {
            "url": n["url"],
            "referrer": n["referrer"],
            "status_label": n["status_label"],
            "is_dupe": n.get("is_dupe", False),
            "idx": i,
            "children": [],
            "is_download": n.get("is_download", False),
            "file_path": n.get("file_path"),
            "file_size": n.get("file_size", 0),
            "extension": n.get("extension", ""),
            "filename": n.get("filename", ""),
        }
        for i, n in enumerate(_scoreboard)
    ]
    url_to_first_idx: Dict[str, int] = {}
    for i, n in enumerate(nodes):
        if n["url"] not in url_to_first_idx:
            url_to_first_idx[n["url"]] = i
    for n in nodes:
        ref = n["referrer"]
        if ref and ref in url_to_first_idx:
            nodes[url_to_first_idx[ref]]["children"].append(n)
    roots = [n for n in nodes if n["referrer"] is None or n["referrer"] not in url_to_first_idx]
    return roots


def _scoreboard_render_html(
    app_root: str,
    current_source_url: str,
    drpid: Optional[str] = None,
    for_save_form: bool = False,
    linked_url_not_checked: Optional[str] = None,
) -> str:
    """Render scoreboard tree as HTML (nested ul). Checkbox checked for original source and OK non-dupes (except linked_url_not_checked)."""
    roots = _scoreboard_tree()
    if not roots:
        return "<p><em>No pages yet.</em></p>"

    original_source_url = next((n["url"] for n in _scoreboard if n.get("referrer") is None), _scoreboard[0]["url"] if _scoreboard else "")

    def render_node(node: Dict[str, Any]) -> str:
        url = node["url"]
        url_short = url[:80] + ("..." if len(url) > 80 else "")
        status = node["status_label"]
        is_dupe = node.get("is_dupe", False)
        is_download = node.get("is_download", False)
        ext = (node.get("extension") or "").strip()
        if is_download and ext and not ext.startswith("."):
            ext = "." + ext
        status_display = (status + " " + ext).strip() if is_download else (status + " (dupe)" if is_dupe else status)
        status_class = "status-404" if "404" in status and not is_download else "status-ok"
        referrer = node.get("referrer") or current_source_url
        is_ok = "OK" in status
        base_checked = (url == original_source_url and is_ok) or (is_ok and not is_dupe)
        checked = base_checked and (not linked_url_not_checked or url != linked_url_not_checked)
        idx = node.get("idx", 0)
        if is_download:
            cb = ""  # No checkbox for downloads
        elif for_save_form:
            cb = f'<input type="checkbox" class="scoreboard-cb" name="save_url" value="{idx}" {"checked" if checked else ""} />'
        else:
            cb = f'<input type="checkbox" class="scoreboard-cb" {"checked" if checked else ""} />'
        if is_download:
            display_text = (node.get("filename") or "").strip() or url_short
            link = f'<span class="url" title="{html.escape(url)}">{html.escape(display_text)}</span>'
        elif current_source_url:
            params = f"source_url={quote(current_source_url)}&linked_url={quote(url)}&referrer={quote(referrer)}&from_scoreboard=1"
            if drpid:
                params += f"&drpid={quote(drpid, safe='')}"
            link = f'<a class="url" href="{html.escape(app_root)}/?{params}" title="{html.escape(url)}">{html.escape(url_short)}</a>'
        else:
            link = f'<span class="url" title="{html.escape(url)}">{html.escape(url_short)}</span>'
        line = f'<li>{cb} {link} <span class="{status_class}">({html.escape(status_display)})</span></li>'
        if node.get("children"):
            children_html = "".join(render_node(c) for c in node["children"])
            line += f"<ul>{children_html}</ul>"
        return line

    items = "".join(render_node(r) for r in roots)
    return f"<ul>{items}</ul>"


def _metadata_for_template(flask_app: Flask, display_drpid: Optional[str], src_h1: Optional[str] = None) -> Dict[str, str]:
    """Build metadata dict for the index template (title, summary, keywords, agency, office, download_date)."""
    project: Optional[Dict[str, Any]] = None
    if display_drpid:
        try:
            project = _get_project_by_drpid(flask_app, int(display_drpid))
        except (ValueError, TypeError):
            pass
    title = (src_h1 or "").strip() or ((project.get("title") or "") if project else "") or ""
    summary = (project.get("summary") or "") if project else ""
    keywords = (project.get("keywords") or "") if project else ""
    agency = (project.get("agency") or "") if project else ""
    office = (project.get("office") or "") if project else ""
    download_date = (project.get("download_date") or "") if project else ""
    if not download_date and display_drpid:
        download_date = date.today().isoformat()
    else:
        download_date = _normalize_date_yyyy_mm_dd(download_date) or download_date
    time_start = (project.get("time_start") or "") if project else ""
    time_end = (project.get("time_end") or "") if project else ""
    return {
        "metadata_title": title,
        "metadata_summary": summary,
        "metadata_keywords": keywords,
        "metadata_agency": agency,
        "metadata_office": office,
        "metadata_download_date": download_date,
        "metadata_time_start": time_start,
        "metadata_time_end": time_end,
    }


def _h1_from_html(html_body: Any) -> str:
    """Extract text of the first <h1> in the HTML; empty string if none."""
    if isinstance(html_body, bytes):
        html_body = html_body.decode("utf-8", errors="replace")
    if not (html_body or "").strip():
        return ""
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html_body, re.DOTALL | re.IGNORECASE)
    if not m:
        return ""
    inner = m.group(1).strip()
    inner = re.sub(r"<[^>]+>", "", inner)
    return (inner or "").strip()


def _prepare_pane_content(
    url_param: str,
    app_root: str,
    source_url: str,
    drpid: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], str, str, bool]:
    """
    Fetch url_param, inject base, rewrite links. Uses url_utils.is_non_html_response.
    Returns (safe_srcdoc, body_message, status_label, h1_text, is_binary).
    """
    status_code, body, content_type, is_logical_404 = fetch_page_body(url_param)
    status_label = _status_label(status_code, is_logical_404)
    h1_text = _h1_from_html(body or "") if body else ""
    pane_is_binary = is_non_html_response(content_type, body)

    # If body is actually HTML (e.g. NCBI pages served as XML), display it
    if body_looks_like_html(body):
        pass  # treat as displayable below
    elif content_type and not is_displayable_content_type(content_type):
        return None, f"Binary content ({html.escape(content_type)}). Not displayed.", status_label, "", True
    elif body_looks_like_xml(body):
        return None, "XML content. Not displayed.", status_label, "", True
    if (body or "").strip() == "" and is_displayable_content_type(content_type):
        return None, "Content could not be displayed (possibly binary or wrong encoding).", status_label, "", False

    body_with_base = _inject_base_into_html(body or "", url_param)
    body_rewritten = _rewrite_links_to_app(
        body_with_base, url_param, app_root, source_url, url_param, drpid=drpid
    )
    safe_srcdoc = body_rewritten.replace("&", "&amp;").replace('"', "&quot;")
    return safe_srcdoc, None, status_label, h1_text, False


# Path to built React SPA (served at /).
_FRONTEND_DIST = Path(__file__).resolve().parent / "frontend" / "dist"


def _serve_spa(subpath: str = "") -> Any:
    """Serve the React SPA (main page with log and collector panes)."""
    dist_str = str(_FRONTEND_DIST)
    if not _FRONTEND_DIST.is_dir():
        return (
            f"Frontend not built. Run: npm run build (in interactive_collector/frontend)",
            503,
            {"Content-Type": "text/plain; charset=utf-8"},
        )
    if not subpath:
        return send_from_directory(dist_str, "index.html")
    return send_from_directory(dist_str, subpath)


@app.route("/legacy")
@app.route("/legacy/")
def legacy_index() -> str:
    """
    Legacy server-rendered three-pane collector (scoreboard, Source, Linked).
    Main app is now the SPA at /.
    """
    app_root = request.url_root.rstrip("/") or request.host_url.rstrip("/")
    url_param = request.args.get("url", "").strip()
    source_url_param = request.args.get("source_url", "").strip()
    linked_url_param = request.args.get("linked_url", "").strip()
    referrer_param = request.args.get("referrer", "").strip()
    from_scoreboard = request.args.get("from_scoreboard", "").strip()
    drpid_param = request.args.get("drpid", "").strip()
    display_drpid: Optional[str] = drpid_param if drpid_param else None

    # Next: get next eligible project after current_drpid and redirect
    next_param = request.args.get("next", "").strip()
    current_drpid_param = request.args.get("current_drpid", "").strip()
    if next_param and current_drpid_param:
        try:
            current_drpid = int(current_drpid_param)
            proj = _get_next_eligible_after(app, current_drpid)
            if proj:
                return redirect(
                    url_for("legacy_index", url=proj.get("source_url") or "", drpid=proj["DRPID"])
                )
            # No next: stay on same project (or could show message)
        except ValueError:
            pass

    # Load by DRPID: fetch project and redirect to its URL
    load_drpid_param = request.args.get("load_drpid", "").strip()
    if load_drpid_param and not url_param and not source_url_param and not linked_url_param:
        try:
            load_drpid = int(load_drpid_param)
            proj = _get_project_by_drpid(app, load_drpid)
            if proj and proj.get("source_url"):
                return redirect(
                    url_for("legacy_index", url=proj["source_url"], drpid=load_drpid)
                )
            # Not found or no URL: fall through to show form (with message later if desired)
        except ValueError:
            pass

    # First load (no URL params): get first eligible from Storage and populate
    if not url_param and not source_url_param and not linked_url_param:
        try:
            proj = _get_first_eligible(app)
            if proj:
                url_param = (proj.get("source_url") or "").strip()
                if url_param:
                    display_drpid = str(proj["DRPID"])
        except RuntimeError:
            pass

    # Initial load: single url=
    if url_param and not source_url_param and not linked_url_param:
        _scoreboard.clear()
        if not is_valid_url(url_param):
            folder_path = _folder_path_for_drpid(display_drpid)
            return render_template_string(
                _INDEX_HTML,
                initial_url=html.escape(url_param),
                scoreboard_html=_scoreboard_render_html(app_root, "", drpid=display_drpid, for_save_form=bool(folder_path)),
                source_srcdoc=None,
                linked_srcdoc=None,
                source_pane_message="Invalid URL. Provide a valid http:// or https:// URL.",
                source_display_url=None,
                linked_display_url=None,
                linked_back_url=None,
                drpid=display_drpid,
                folder_path=folder_path,
                scoreboard_urls_json=json.dumps([n["url"] for n in _scoreboard]),
                show_auto_download=False,
                **_metadata_for_template(app, display_drpid, None),
            )
        safe_srcdoc, body_message, status_label, src_h1, _ = _prepare_pane_content(
            url_param, app_root, url_param, drpid=display_drpid
        )
        _scoreboard_add(url_param, None, status_label)
        source_srcdoc = safe_srcdoc if body_message is None else None
        if body_message and safe_srcdoc is None:
            source_srcdoc = None
        # When source page was retrieved and we have a DRPID, create/empty output folder and set folder_path
        folder_path: Optional[str] = None
        if display_drpid and body_message is None:
            try:
                folder_path = _ensure_output_folder_for_drpid(app, int(display_drpid))
            except (ValueError, TypeError):
                pass
        if folder_path is None:
            folder_path = _folder_path_for_drpid(display_drpid)
        scoreboard_urls_json = json.dumps([n["url"] for n in _scoreboard])
        return render_template_string(
            _INDEX_HTML,
            initial_url=html.escape(url_param),
            scoreboard_html=_scoreboard_render_html(app_root, url_param, drpid=display_drpid, for_save_form=bool(folder_path)),
            source_srcdoc=source_srcdoc,
            linked_srcdoc=None,
            source_pane_message=body_message,
            source_display_url=url_param,
            linked_display_url=None,
            linked_back_url=None,
            drpid=display_drpid,
            folder_path=folder_path,
            scoreboard_urls_json=scoreboard_urls_json,
            show_auto_download=False,
            **_metadata_for_template(app, display_drpid, src_h1),
        )

    # Link click: source_url + linked_url + referrer
    if source_url_param and linked_url_param and referrer_param:
        if not is_valid_url(source_url_param) or not is_valid_url(linked_url_param):
            folder_path = _folder_path_for_drpid(display_drpid)
            return render_template_string(
                _INDEX_HTML,
                initial_url=html.escape(source_url_param),
                scoreboard_html=_scoreboard_render_html(app_root, source_url_param, drpid=display_drpid, for_save_form=bool(folder_path)),
                source_srcdoc=None,
                linked_srcdoc=None,
                source_pane_message=None,
                source_display_url=None,
                linked_display_url=None,
                linked_back_url=None,
                drpid=display_drpid,
                folder_path=folder_path,
                scoreboard_urls_json=json.dumps([n["url"] for n in _scoreboard]),
                show_auto_download=False,
                **_metadata_for_template(app, display_drpid, None),
            )
        # Fetch both panes. For catalog.data.gov links, resolve on click and show the resolved URL in the Linked pane (skip the relay).
        src_srcdoc, src_pane_message, src_status, src_h1, _ = _prepare_pane_content(
            source_url_param, app_root, source_url_param, drpid=display_drpid
        )
        linked_url_for_fetch = linked_url_param
        linked_display_url = linked_url_param
        resolved_linked: Optional[str] = None
        if linked_url_param.startswith("https://catalog.data.gov"):
            resolved_linked = resolve_catalog_resource_url(linked_url_param)
            if resolved_linked:
                linked_url_for_fetch = resolved_linked
                linked_display_url = resolved_linked
        linked_srcdoc, linked_pane_message, linked_status, _, linked_is_binary = _prepare_pane_content(
            linked_url_for_fetch, app_root, source_url_param, drpid=display_drpid
        )
        if not any(n["url"] == source_url_param for n in _scoreboard):
            _scoreboard_add(source_url_param, None, src_status)
        if not from_scoreboard and not linked_is_binary:
            _scoreboard_add(linked_url_for_fetch, referrer_param, linked_status)
        # else: from_scoreboard click, or binary (DL entry added when download completes)
        folder_path = _folder_path_for_drpid(display_drpid)
        if folder_path is None and display_drpid:
            try:
                folder_path = _ensure_output_folder_for_drpid(app, int(display_drpid))
            except (ValueError, TypeError):
                pass
        linked_srcdoc_for_display = linked_srcdoc
        linked_display_url_when_binary: Optional[str] = None
        if linked_is_binary and referrer_param:
            if referrer_param == source_url_param:
                linked_srcdoc_for_display = src_srcdoc
                linked_display_url_when_binary = source_url_param
            elif is_valid_url(referrer_param):
                ref_srcdoc, _ref_msg, _ref_status, _, _ = _prepare_pane_content(
                    referrer_param, app_root, source_url_param, drpid=display_drpid
                )
                if ref_srcdoc:
                    linked_srcdoc_for_display = ref_srcdoc
                    linked_display_url_when_binary = referrer_param
                else:
                    linked_srcdoc_for_display = src_srcdoc
                    linked_display_url_when_binary = source_url_param
            else:
                linked_srcdoc_for_display = src_srcdoc
                linked_display_url_when_binary = source_url_param
        elif linked_is_binary:
            linked_srcdoc_for_display = src_srcdoc
            linked_display_url_when_binary = source_url_param
        if linked_display_url_when_binary is not None:
            linked_display_url = linked_display_url_when_binary
        show_auto_download = (
            linked_is_binary
            and bool(folder_path)
            and bool(linked_url_for_fetch)
            and not request.args.get("downloaded")
        )
        linked_back_url = None
        if linked_srcdoc_for_display and referrer_param and referrer_param != source_url_param:
            _kwargs = {"source_url": source_url_param, "linked_url": referrer_param, "referrer": source_url_param, "from_scoreboard": "1"}
            if display_drpid:
                _kwargs["drpid"] = display_drpid
            linked_back_url = url_for("legacy_index", **_kwargs)
        return render_template_string(
            _INDEX_HTML,
            initial_url=html.escape(source_url_param),
            scoreboard_html=_scoreboard_render_html(
                app_root, source_url_param, drpid=display_drpid, for_save_form=bool(folder_path),
                linked_url_not_checked=linked_url_for_fetch if linked_is_binary else None,
            ),
            source_srcdoc=src_srcdoc,
            linked_srcdoc=linked_srcdoc_for_display,
            source_pane_message=src_pane_message,
            linked_pane_message=linked_pane_message,
            source_display_url=source_url_param,
            linked_display_url=linked_display_url,
            linked_back_url=linked_back_url,
            drpid=display_drpid,
            folder_path=folder_path,
            scoreboard_urls_json=json.dumps([n["url"] for n in _scoreboard]),
            linked_is_binary=linked_is_binary,
            linked_binary_url=linked_url_for_fetch if linked_is_binary else None,
            linked_binary_referrer=referrer_param if linked_is_binary else None,
            show_auto_download=show_auto_download,
            **_metadata_for_template(app, display_drpid, src_h1),
        )

    # No URL: show form and empty panes
    folder_path = _folder_path_for_drpid(display_drpid)
    return render_template_string(
        _INDEX_HTML,
        initial_url="",
        scoreboard_html=_scoreboard_render_html(app_root, "", drpid=display_drpid, for_save_form=bool(folder_path)),
        source_srcdoc=None,
        linked_srcdoc=None,
        source_pane_message=None,
        source_display_url=None,
        linked_display_url=None,
        linked_back_url=None,
        drpid=display_drpid,
        folder_path=folder_path,
        scoreboard_urls_json=json.dumps([n["url"] for n in _scoreboard]),
        show_auto_download=False,
        **_metadata_for_template(app, display_drpid, None),
    )


@app.route("/extension/launcher")
def extension_launcher() -> Any:
    """
    Launcher page for the browser extension. Encodes DRPID and target URL.

    Query params: drpid (required), url (required, https).
    Returns minimal HTML that redirects to url after ~500ms.
    The extension content script runs on this page, reads drpid/url, stores in
    chrome.storage, then the redirect proceeds. User pastes this URL in the
    extended browser to start a collection session.
    """
    drpid_param = request.args.get("drpid", "").strip()
    url_param = request.args.get("url", "").strip()
    if not drpid_param:
        return "<!DOCTYPE html><html><body><p>Missing drpid.</p></body></html>", 400
    if not url_param:
        return "<!DOCTYPE html><html><body><p>Missing url.</p></body></html>", 400
    try:
        drpid = int(drpid_param)
    except ValueError:
        return "<!DOCTYPE html><html><body><p>Invalid drpid.</p></body></html>", 400
    if not url_param.startswith("https://"):
        return "<!DOCTYPE html><html><body><p>URL must be https.</p></body></html>", 400
    proj = _get_project_by_drpid(app, drpid)
    if not proj:
        return "<!DOCTYPE html><html><body><p>Project not found.</p></body></html>", 404
    folder_path = _folder_path_for_drpid(str(drpid))
    if not folder_path:
        try:
            folder_path = _ensure_output_folder_for_drpid(app, drpid)
        except (ValueError, TypeError):
            pass
    if not folder_path:
        return "<!DOCTYPE html><html><body><p>Could not create output folder.</p></body></html>", 500
    # Extension reads drpid and url from window.location.search; we redirect after short delay
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Redirecting...</title></head>
<body><p>Redirecting...</p>
<script>setTimeout(function(){{ window.location.href = {json.dumps(url_param)}; }}, 500);</script>
</body></html>"""


@app.route("/")
def index() -> Any:
    """Serve the React SPA (main page) at root."""
    return _serve_spa("")


@app.route("/<path:subpath>")
def spa_assets(subpath: str) -> Any:
    """Serve SPA static assets (e.g. /assets/...) or index.html for client paths."""
    return _serve_spa(subpath)


def _generate_save_progress(
    folder_path: Path,
    urls: List[str],
    indices: List[str],
):
    """
    Generator that yields progress lines for the save operation (legacy route).
    Delegates to api_save.generate_save_progress so heartbeats and timeouts are shared.
    """
    from interactive_collector.api_save import generate_save_progress
    yield from generate_save_progress(folder_path, urls, indices)


# Progress report interval for large file downloads (MB).
_DOWNLOAD_PROGRESS_INTERVAL_MB = 50.0


def _generate_download_progress(
    url: str,
    folder_path_str: str,
    drpid: int,
    referrer: Optional[str],
):
    """
    Generator that yields progress lines for a single file download.
    Yields: SAVING\\t{basename}\\n, PROGRESS\\t{written}\\t{total}\\n, then DONE\\t{basename}\\t{size}\\t{ext}\\n or ERROR\\t{msg}\\n.
    """
    folder_path = Path(folder_path_str)
    if not folder_path.is_dir():
        yield f"ERROR\tOutput folder not found\n"
        return
    try:
        resp = requests.get(url, stream=True, headers=BROWSER_HEADERS, timeout=(30, 300))
        resp.raise_for_status()
    except requests.RequestException as e:
        yield f"ERROR\t{str(e)[:200]}\n"
        return

    content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
    content_disp = resp.headers.get("Content-Disposition")
    content_length: Optional[int] = None
    try:
        cl = resp.headers.get("Content-Length")
        if cl is not None:
            content_length = int(cl)
    except ValueError:
        pass

    filename = _filename_from_content_disposition(content_disp) or _filename_from_url(url)
    ext = _extension_from_content_type(content_type)
    if filename and "." in filename:
        pass  # keep ext from filename
    else:
        if ext and filename:
            filename = filename + (ext if ext.startswith(".") else "." + ext)
        elif ext:
            filename = "download" + (ext if ext.startswith(".") else "." + ext)
    base = filename
    if "." in base:
        base = base.rsplit(".", 1)[0]
        ext_from_name = "." + filename.rsplit(".", 1)[-1]
        if not ext:
            ext = ext_from_name
    else:
        if not ext:
            ext = ""

    used: Dict[str, int] = {p.name.lower(): 1 for p in folder_path.iterdir() if p.is_file()}
    basename = _unique_download_basename(base, ext, used)
    dest = folder_path / basename

    yield f"SAVING\t{basename}\n"
    chunk_size = 1024 * 1024  # 1 MB
    written = 0
    last_yield_mb = 0.0
    try:
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if not chunk:
                    break
                f.write(chunk)
                written += len(chunk)
                mb = written / (1024 * 1024)
                if (mb - last_yield_mb) >= _DOWNLOAD_PROGRESS_INTERVAL_MB or (content_length and written >= content_length):
                    last_yield_mb = mb
                    total_str = str(content_length) if content_length is not None else ""
                    yield f"PROGRESS\t{written}\t{total_str}\n"
    except OSError as e:
        yield f"ERROR\t{str(e)[:200]}\n"
        return

    ext_display = ext.lstrip(".")
    yield f"DONE\t{basename}\t{written}\t{ext_display}\n"

    _result_by_drpid.setdefault(drpid, {}).setdefault("downloads", []).append({
        "url": url,
        "path": str(dest),
        "size": written,
        "extension": ext_display,
        "filename": basename,
    })
    _scoreboard_add_download(url, referrer, str(dest), written, ext_display, filename=basename)


@app.route("/download-file", methods=["POST"])
def download_file() -> Any:
    """
    Download a non-HTML URL to the project output folder; streams progress (SAVING, PROGRESS, DONE).
    Expects url, drpid, referrer (optional). Uses folder_path from _result_by_drpid.
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

    folder_path = _result_by_drpid.get(drpid, {}).get("folder_path")
    if not folder_path:
        return "No output folder for this project", 400

    def stream() -> Any:
        for line in _generate_download_progress(url, folder_path, drpid, referrer):
            sys.stderr.write(line)
            sys.stderr.flush()
            yield line

    from flask import Response
    return Response(
        stream(),
        mimetype="text/plain; charset=utf-8",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


def _save_metadata_from_request() -> None:
    """Update the project record from request form (metadata + folder stats). Idempotent if no drpid."""
    drpid_str = (request.form.get("drpid") or "").strip()
    folder_path_str = (request.form.get("folder_path") or "").strip()
    if not drpid_str:
        return
    try:
        drpid = int(drpid_str)
    except (ValueError, TypeError):
        return
    _ensure_storage(app)
    from storage import Storage
    values: Dict[str, Any] = {
        "status": "collected",
        "errors": None,
        "title": (request.form.get("metadata_title") or "").strip(),
        "agency": (request.form.get("metadata_agency") or "").strip(),
        "office": (request.form.get("metadata_office") or "").strip(),
        "summary": (request.form.get("metadata_summary") or "").strip(),
        "keywords": (request.form.get("metadata_keywords") or "").strip(),
        "time_start": (request.form.get("metadata_time_start") or "").strip(),
        "time_end": (request.form.get("metadata_time_end") or "").strip(),
        "download_date": (request.form.get("metadata_download_date") or "").strip(),
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
        Logger.warning(f"_save_metadata_from_request: record not found or invalid for DRPID={drpid}: {e}")


@app.route("/save", methods=["POST"])
def save() -> Any:
    """
    Save metadata to DB and optionally save checked scoreboard pages as PDFs.
    Always updates metadata from form. If save_url indices present, streams PDF progress;
    else redirects to index with return_to query so the user stays on the same view.
    """
    _save_metadata_from_request()
    folder_path_str = (request.form.get("folder_path") or "").strip()
    urls_json = (request.form.get("scoreboard_urls_json") or "[]").strip()
    indices = request.form.getlist("save_url")
    return_to = (request.form.get("return_to") or "").strip()
    if return_to and not return_to.startswith("?"):
        return_to = "?" + return_to

    if not folder_path_str or not indices:
        return redirect(url_for("legacy_index") + (return_to or ""))

    try:
        urls = json.loads(urls_json)
    except json.JSONDecodeError:
        return redirect(url_for("legacy_index") + (return_to or ""))

    folder_path = Path(folder_path_str)
    if not folder_path.is_dir():
        return redirect(url_for("legacy_index") + (return_to or ""))

    drpid_val: Optional[int] = None
    try:
        drpid_val = int((request.form.get("drpid") or "").strip())
    except (ValueError, TypeError):
        pass

    def stream() -> Any:
        for line in _generate_save_progress(folder_path, urls, indices):
            sys.stderr.write(line)
            sys.stderr.flush()
            yield line

    from flask import Response
    return Response(
        stream(),
        mimetype="text/plain; charset=utf-8",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )
