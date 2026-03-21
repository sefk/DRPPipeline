"""
Flask API for running pipeline modules from the SPA main page.

Exposes: list modules, run a module (subprocess with streamed log output), stop running module.
Progress is streamed to the client as NDJSON (application/x-ndjson): one JSON object per line,
``{"line": "..."}`` for log text (including trailing newlines) and ``{"ping": true}`` for
keepalives that do not appear in the log. Stderr still receives raw log lines only (no pings),
so the Flask terminal matches the SPA without blank keepalive lines.

Subprocess stdout is read as binary with an incremental UTF-8 decoder so Windows pipes do not
drop or merge lines the way text-mode iteration can.
"""

import codecs
import json
import os
import queue
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Generator, Optional

from flask import Blueprint, Response, current_app, request, stream_with_context

# Project root (parent of interactive_collector)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_MAIN_PY = _PROJECT_ROOT / "main.py"
_STOP_FILE = _PROJECT_ROOT / ".drp_pipeline_stop"

# Current run (so POST /stop can terminate it)
_current_proc: Optional[subprocess.Popen[str]] = None

pipeline_bp = Blueprint("pipeline", __name__, url_prefix="/api/pipeline")


def _ndjson_line(obj: Any) -> str:
    """Single NDJSON record (UTF-8 text, trailing newline)."""
    return json.dumps(obj, ensure_ascii=False) + "\n"


def _normalize_log_line(s: str) -> str:
    """Normalize Windows CRLF from subprocess pipes to single \\n for NDJSON and stderr."""
    return s.replace("\r\n", "\n").replace("\r", "\n")


def _modules() -> dict[str, dict[str, Any]]:
    """Return the MODULES registry from the orchestrator."""
    from orchestration.Orchestrator import MODULES
    return dict(MODULES)


@pipeline_bp.route("/modules", methods=["GET"])
def list_modules() -> Any:
    """
    Return the list of pipeline module names (keys of MODULES).

    Returns:
        JSON: { "modules": ["noop", "sourcing", "collector", ...] }
    """
    # Preserve MODULES order (no noop in list for UI)
    mods = [m for m in _modules().keys() if m != "noop"]
    return {"modules": mods}


def _log(msg: str) -> None:
    """Write to stdout, stderr, and Flask logger so it appears in the Flask terminal."""
    line = msg + "\n"
    sys.stdout.write(line)
    sys.stdout.flush()
    sys.stderr.write(line)
    sys.stderr.flush()
    try:
        current_app.logger.info(msg)
    except Exception:
        pass


@pipeline_bp.route("/run", methods=["POST"])
def run_module() -> Any:
    """
    Run a pipeline module in a subprocess and stream its stdout as the response.

    Request JSON: module (required), num_rows (optional), start_drpid (optional),
    log_level (optional), max_workers (optional).

    Returns:
        Streamed ``application/x-ndjson`` (one JSON object per line). On invalid input, 400 JSON.

    Note: If you don't see [pipeline/run] logs in the terminal, the debug reloader
    may be using a child process whose output isn't shown. Try: flask run --no-reload
    """
    if not request.is_json:
        data: dict[str, Any] = {}
    else:
        data = request.get_json() or {}

    module = (data.get("module") or "").strip()
    _log(f"[pipeline/run] POST received module={module!r}")
    if not module:
        return {"error": "module is required"}, 400

    modules = _modules()
    if module not in modules:
        return {"error": f"Unknown module {module!r}. Valid: {sorted(modules.keys())}"}, 400

    # interactive_collector is not run as subprocess; UI should open collector view instead
    if module == "interactive_collector":
        return {"error": "Use the Interactive Collector button to open the collector UI."}, 400

    num_rows = data.get("num_rows")
    start_drpid = data.get("start_drpid")
    log_level = (data.get("log_level") or "").strip().upper() or None
    max_workers = data.get("max_workers")

    argv = [sys.executable, str(_MAIN_PY), module]
    if num_rows is not None:
        try:
            argv.extend(["--num-rows", str(int(num_rows))])
        except (TypeError, ValueError):
            pass
    if start_drpid is not None:
        try:
            argv.extend(["--start-drpid", str(int(start_drpid))])
        except (TypeError, ValueError):
            pass
    if log_level:
        if log_level in ("DEBUG", "INFO", "WARNING", "ERROR"):
            argv.extend(["--log-level", log_level])
    if max_workers is not None:
        try:
            argv.extend(["--max-workers", str(int(max_workers))])
        except (TypeError, ValueError):
            pass

    def stream() -> Generator[str, None, None]:
        global _current_proc
        proc: Optional[subprocess.Popen[bytes]] = None
        keepalive_interval = 10.0  # Send something every N seconds (proxies / idle TCP)
        # Emit these as the first chunk so they appear in the stream (SPA + terminal)
        start_msg1 = f"[pipeline/run] POST received module={module!r}\n"
        start_msg2 = f"[pipeline/run] starting subprocess: {' '.join(argv)}\n"
        sys.stderr.write(start_msg1)
        sys.stderr.flush()
        try:
            yield _ndjson_line({"line": start_msg1})
        except (BrokenPipeError, ConnectionResetError, OSError):
            return
        sys.stderr.write(start_msg2)
        sys.stderr.flush()
        try:
            yield _ndjson_line({"line": start_msg2})
        except (BrokenPipeError, ConnectionResetError, OSError):
            return
        try:
            try:
                if _STOP_FILE.exists():
                    _STOP_FILE.unlink()
            except OSError:
                pass
            env = os.environ.copy()
            env["DRP_STOP_FILE"] = str(_STOP_FILE)
            env["PYTHONUNBUFFERED"] = "1"
            proc = subprocess.Popen(
                argv,
                cwd=str(_PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=0,
                env=env,
            )
            _current_proc = proc
            assert proc.stdout is not None
            out_queue: "queue.Queue[Optional[str]]" = queue.Queue()
            sentinel: Optional[str] = None

            def reader() -> None:
                decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
                remainder = ""
                try:
                    while True:
                        raw = proc.stdout.read(8192)
                        if raw == b"":
                            break
                        remainder += decoder.decode(raw)
                        while "\n" in remainder:
                            idx = remainder.index("\n")
                            line = _normalize_log_line(remainder[: idx + 1])
                            remainder = remainder[idx + 1 :]
                            out_queue.put(line)
                    remainder += decoder.decode(b"", final=True)
                    if remainder:
                        tail = (
                            remainder
                            if remainder.endswith("\n")
                            else remainder + "\n"
                        )
                        out_queue.put(_normalize_log_line(tail))
                except Exception:
                    pass
                out_queue.put(sentinel)

            t = threading.Thread(target=reader, daemon=True)
            t.start()
            while True:
                try:
                    line = out_queue.get(timeout=keepalive_interval)
                except queue.Empty:
                    try:
                        yield _ndjson_line({"ping": True})
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        break
                    continue
                if line is None:
                    break
                sys.stderr.write(line)
                sys.stderr.flush()
                try:
                    yield _ndjson_line({"line": line})
                except (BrokenPipeError, ConnectionResetError, OSError):
                    break
            proc.wait()
        except Exception as e:
            line = f"\nPipeline run error: {e}\n"
            sys.stderr.write(line)
            sys.stderr.flush()
            try:
                yield _ndjson_line({"line": line})
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
        finally:
            _current_proc = None
            if _STOP_FILE.exists():
                try:
                    _STOP_FILE.unlink()
                except OSError:
                    pass
            if proc is not None and proc.poll() is None:
                proc.terminate()

    return Response(
        stream_with_context(stream()),
        mimetype="application/x-ndjson; charset=utf-8",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


@pipeline_bp.route("/stop", methods=["POST"])
def stop_run() -> Any:
    """
    Request the current pipeline run to stop.

    Writes the stop file (so the orchestrator's inner loop can exit cleanly)
    and terminates the subprocess so work halts promptly.
    """
    global _current_proc
    try:
        _STOP_FILE.touch()
    except OSError:
        pass
    if _current_proc is not None and _current_proc.poll() is None:
        _current_proc.terminate()
    return {"ok": True}
