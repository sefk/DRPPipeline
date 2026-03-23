"""
Observer console for watching a collector training run in real time.

Reads directly from the training database (read-only) and the run log file.
Refreshes every few seconds. Press 'q' + Enter to quit.
Training continues unaffected.

Usage:
    python -m collector_training.observer <run_id> [--db DB_PATH] [--interval SECS]

Example:
    python -m collector_training.observer 7
    python -m collector_training.observer 7 --interval 10
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Thread
from typing import Any, Optional

from tqdm import tqdm

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from collector_training.schema import get_connection, DEFAULT_DB_PATH

LOGS_DIR = PROJECT_ROOT / "collector_training" / "logs"

# ANSI colors
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"
_DIM    = "\033[90m"
_RESET  = "\033[0m"
_WHITE  = "\033[97m"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _elapsed_secs(started_at: Optional[str]) -> float:
    dt = _parse_iso(started_at)
    if dt is None:
        return 0.0
    return max(0.0, (_now_utc() - dt).total_seconds())


def _fmt_duration(secs: float) -> str:
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"


def _score_color(score: float) -> str:
    if score >= 0.8:
        return _GREEN
    if score >= 0.5:
        return _YELLOW
    return _RED


def _status_color(status: str) -> str:
    return {
        "running":   _GREEN,
        "completed": _CYAN,
        "stopped":   _YELLOW,
        "failed":    _RED,
    }.get(status, _RESET)


def _progress_bar(
    n: int,
    total: int,
    elapsed: float = 0.0,
    width: int = 70,
    unit: str = "it",
    desc: str = "",
) -> str:
    """Return a tqdm-formatted progress bar string."""
    if total == 0:
        return "(no data)"
    prefix = f"{desc}: " if desc else ""
    return prefix + tqdm.format_meter(
        n=n, total=total, elapsed=elapsed, ncols=width, unit=unit
    )


def _mini_bar(value: float, width: int = 20) -> str:
    """Compact filled bar for a [0,1] score value."""
    filled = int(round(value * width))
    return "█" * filled + "░" * (width - filled)


# ── Database queries ──────────────────────────────────────────────────────────

def _fetch_run(run_id: int, db_path: Path) -> Optional[dict[str, Any]]:
    con = get_connection(db_path)
    try:
        row = con.execute(
            "SELECT * FROM training_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def _fetch_iterations(run_id: int, db_path: Path) -> list[dict[str, Any]]:
    con = get_connection(db_path)
    try:
        rows = con.execute(
            "SELECT iteration_id, iteration_num, aggregate_score, per_field_scores_json, "
            "model_used, started_at, finished_at, refinement_strategy "
            "FROM iterations WHERE run_id=? ORDER BY iteration_num",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def _fetch_cost(run_id: int, db_path: Path) -> float:
    con = get_connection(db_path)
    try:
        row = con.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) FROM token_usage WHERE run_id=?",
            (run_id,),
        ).fetchone()
        return row[0] if row else 0.0
    finally:
        con.close()


def _fetch_example_counts(run_id: int, db_path: Path) -> dict[str, int]:
    con = get_connection(db_path)
    try:
        total = con.execute(
            "SELECT COUNT(*) FROM training_examples WHERE run_id=?", (run_id,)
        ).fetchone()[0]
        val = con.execute(
            "SELECT COUNT(*) FROM training_examples WHERE run_id=? AND is_validation=1",
            (run_id,),
        ).fetchone()[0]
        large = con.execute(
            "SELECT COUNT(*) FROM training_examples "
            "WHERE run_id=? AND data_size_bytes > 100000000",
            (run_id,),
        ).fetchone()[0]
        return {"total": total, "validation": val, "large": large, "training": total - val}
    finally:
        con.close()


def _fetch_scores_for_iteration(
    iteration_id: int, db_path: Path, limit: int = 10
) -> list[dict[str, Any]]:
    """Return the most recent project_scores rows for a given iteration."""
    con = get_connection(db_path)
    try:
        rows = con.execute(
            "SELECT ps.score, ps.error_message, te.source_url "
            "FROM project_scores ps "
            "JOIN training_examples te ON ps.example_id = te.example_id "
            "WHERE ps.iteration_id=? "
            "ORDER BY ps.score_id DESC LIMIT ?",
            (iteration_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


# ── Log file parsing ──────────────────────────────────────────────────────────

def _read_log(run_id: int) -> list[str]:
    """Read the run log file; return lines (empty list if not found)."""
    path = LOGS_DIR / f"run_{run_id}.log"
    if not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []


def _parse_log_for_current_iteration(
    lines: list[str],
) -> dict[str, Any]:
    """
    Scan the log for the current in-progress iteration.

    Returns a dict:
        iteration_num:  int | None
        examples_done:  int
        iter_started_at: str | None  (log timestamp of Iteration line)
        recent_examples: list[dict]  (last N {score, url, error} entries)
        decisions:       list[str]   (DECIDE lines from current iteration)
    """
    result: dict[str, Any] = {
        "iteration_num":   None,
        "examples_done":   0,
        "iter_started_at": None,
        "recent_examples": [],
        "decisions":       [],
    }

    # Find the last "Iteration N/M" info line
    last_iter_line_idx = -1
    last_iter_num = None
    last_iter_ts = None

    iter_pattern = re.compile(r"Iteration\s+(\d+)/\d+")

    for i, line in enumerate(lines):
        m = iter_pattern.search(line)
        if m and "| INFO" in line:
            last_iter_line_idx = i
            last_iter_num = int(m.group(1))
            # Timestamp is the first token
            last_iter_ts = line.split("|")[0].strip()

    if last_iter_line_idx == -1:
        return result

    result["iteration_num"] = last_iter_num
    result["iter_started_at"] = last_iter_ts

    # Count EXAMPLE lines after the iteration start
    examples = []
    decisions = []
    for line in lines[last_iter_line_idx + 1:]:
        if "| EXAMPLE |" in line:
            # Format: "YYYY-MM-DD HH:MM:SS | EXAMPLE | 0.612  https://..."
            parts = line.split("| EXAMPLE |", 1)
            if len(parts) == 2:
                payload = parts[1].strip()
                score_and_url = payload.split(None, 1)
                try:
                    score = float(score_and_url[0])
                    url = score_and_url[1] if len(score_and_url) > 1 else ""
                    error = None
                    if "  ERROR:" in url:
                        url, error = url.split("  ERROR:", 1)
                        error = error.strip()
                    examples.append({"score": score, "url": url.strip(), "error": error})
                except (ValueError, IndexError):
                    pass
        elif "| DECIDE |" in line:
            parts = line.split("| DECIDE |", 1)
            if len(parts) == 2:
                decisions.append(parts[1].strip())

    result["examples_done"] = len(examples)
    result["recent_examples"] = examples[-10:]
    result["decisions"] = decisions
    return result


def _parse_log_decisions(lines: list[str]) -> list[str]:
    """Return all DECIDE log lines."""
    return [
        line.split("| DECIDE |", 1)[1].strip()
        for line in lines
        if "| DECIDE |" in line
    ]


# ── Rendering ────────────────────────────────────────────────────────────────

def _divider(width: int = 72) -> str:
    return _DIM + "─" * width + _RESET


def render(run_id: int, db_path: Path) -> str:
    """Build the full display string for one refresh cycle."""
    run = _fetch_run(run_id, db_path)
    if run is None:
        return f"Run {run_id} not found in {db_path}\n"

    iterations   = _fetch_iterations(run_id, db_path)
    cost         = _fetch_cost(run_id, db_path)
    counts       = _fetch_example_counts(run_id, db_path)
    log_lines    = _read_log(run_id)
    log_progress = _parse_log_for_current_iteration(log_lines)

    config: dict[str, Any] = {}
    if run.get("config_json"):
        try:
            config = json.loads(run["config_json"])
        except Exception:
            pass

    max_cost      = config.get("max_cost_usd", 10.0)
    max_iters     = config.get("max_iterations", 20)
    num_workers   = config.get("num_eval_workers", 1)
    status        = run.get("status", "?")
    status_color  = _status_color(status)
    run_elapsed   = _elapsed_secs(run.get("started_at"))

    completed_iters = sum(1 for it in iterations if it.get("finished_at"))
    in_progress_it  = next(
        (it for it in reversed(iterations) if it.get("finished_at") is None), None
    )
    best_score  = run.get("best_score") or 0.0
    best_it_num = run.get("best_iteration") or 0

    lines: list[str] = []

    # ── Header ──────────────────────────────────────────────────────────────
    lines.append(_divider())
    lines.append(
        f"{_BOLD}{_WHITE}DRP Collector Training — "
        f"{run.get('collector_name', '?')}  "
        f"[run_id={run_id}  site={run.get('source_site', '?')}]{_RESET}"
    )
    lines.append(
        f"  Started : {(run.get('started_at') or '')[:16].replace('T', ' ')}  "
        f"Elapsed : {_fmt_duration(run_elapsed)}  "
        f"Workers : {num_workers}  "
        f"Status : {status_color}{_BOLD}{status.upper()}{_RESET}"
    )
    if log_lines:
        log_path = LOGS_DIR / f"run_{run_id}.log"
        lines.append(f"  Log     : {_DIM}{log_path}{_RESET}")
    lines.append(_divider())
    lines.append("")

    # ── Run progress bar ────────────────────────────────────────────────────
    lines.append(f"{_BOLD}Run Progress{_RESET}")
    lines.append(
        "  " + _progress_bar(
            n=completed_iters,
            total=max_iters,
            elapsed=run_elapsed,
            width=68,
            unit="iter",
        )
    )
    lines.append("")

    # ── Current iteration progress (from log) ───────────────────────────────
    n_train = counts["training"]
    if in_progress_it is not None:
        iter_num    = in_progress_it["iteration_num"]
        iter_elapsed = _elapsed_secs(in_progress_it.get("started_at"))
        examples_done = log_progress["examples_done"]

        lines.append(f"{_BOLD}Current Iteration  (#{iter_num}){_RESET}")
        model_str = in_progress_it.get("model_used") or "?"
        strat     = in_progress_it.get("refinement_strategy")
        meta_parts = [f"model={model_str}"]
        if strat:
            meta_parts.append(f"strategy={strat!r}")
        lines.append(f"  {_DIM}{' · '.join(meta_parts)}{_RESET}")
        lines.append(
            "  " + _progress_bar(
                n=examples_done,
                total=n_train,
                elapsed=iter_elapsed,
                width=68,
                unit="ex",
            )
        )
        lines.append("")

    # ── Budget bar ──────────────────────────────────────────────────────────
    lines.append(f"{_BOLD}Budget{_RESET}")
    pct = cost / max_cost if max_cost > 0 else 0
    cost_color = _score_color(1.0 - pct)  # green when low, red when high
    # Represent budget as integer cents so tqdm shows sensible numbers
    cost_cents     = int(cost * 100)
    max_cost_cents = int(max_cost * 100)
    bar_line = tqdm.format_meter(
        n=cost_cents, total=max_cost_cents, elapsed=run_elapsed,
        ncols=68, unit="¢",
    )
    lines.append(f"  {cost_color}{bar_line}{_RESET}")
    lines.append(
        f"  {cost_color}${cost:.4f} of ${max_cost:.2f}  "
        f"({pct:.0%} used){_RESET}"
    )
    lines.append("")

    # ── Score history ────────────────────────────────────────────────────────
    lines.append(f"{_BOLD}Score History{_RESET}")
    if not iterations:
        lines.append("  (no iterations yet)")
    else:
        prev_score: Optional[float] = None
        for it in iterations:
            score = it.get("aggregate_score")
            it_num = it["iteration_num"]
            running = it.get("finished_at") is None

            if running:
                bar_str   = _DIM + "░" * 22 + _RESET
                score_str = f"  {_DIM}evaluating…{_RESET}"
                delta_str = ""
            elif score is None:
                bar_str   = _DIM + "░" * 22 + _RESET
                score_str = "  —"
                delta_str = ""
            else:
                color     = _score_color(score)
                bar_str   = color + _mini_bar(score, 22) + _RESET
                score_str = f"  {color}{score:.3f}{_RESET}"
                if prev_score is not None:
                    delta = score - prev_score
                    sign  = "+" if delta >= 0 else ""
                    dcolor = _GREEN if delta > 0 else _RED if delta < 0 else _DIM
                    delta_str = f"  {dcolor}{sign}{delta:.3f}{_RESET}"
                else:
                    delta_str = ""

            tags = []
            if it_num == best_it_num:
                tags.append(f"{_CYAN}best{_RESET}")
            if running:
                tags.append(f"{_GREEN}running{_RESET}")
            tag_str = f"  {_DIM}←{_RESET} " + ", ".join(tags) if tags else ""

            lines.append(
                f"  It {it_num:<3}  {bar_str}{score_str}{delta_str}{tag_str}"
            )
            if score is not None:
                prev_score = score

    lines.append("")

    # ── Per-field breakdown (use best iteration, fallback to latest non-zero) ──
    best_it_obj = next(
        (it for it in iterations if it.get("iteration_num") == best_it_num), None
    )
    latest_complete = best_it_obj or next(
        (
            it for it in reversed(iterations)
            if it.get("aggregate_score") and it["aggregate_score"] > 0
        ),
        None,
    )
    if latest_complete and latest_complete.get("per_field_scores_json"):
        try:
            per_field = json.loads(latest_complete["per_field_scores_json"])
            lines.append(
                f"{_BOLD}Per-Field Scores{_RESET}  "
                f"{_DIM}(iteration {latest_complete['iteration_num']}){_RESET}"
            )
            for field, score in sorted(per_field.items(), key=lambda x: x[1]):
                color   = _score_color(score)
                bar_str = color + _mini_bar(score, 22) + _RESET
                lines.append(
                    f"  {field:<24}  {color}{score:.3f}{_RESET}  {bar_str}"
                )
            lines.append("")
        except Exception:
            pass

    # ── Dataset info ─────────────────────────────────────────────────────────
    lines.append(f"{_BOLD}Dataset{_RESET}")
    lines.append(
        f"  Training : {counts['training']} examples"
        + (f"  ({counts['large']} large, skipped on non-full-fidelity iters)" if counts["large"] else "")
    )
    lines.append(f"  Validation : {counts['validation']} examples (held out)")
    if best_score > 0:
        lines.append(
            f"  Best score : {_CYAN}{best_score:.3f}{_RESET}  "
            f"(iteration {best_it_num})"
        )
    lines.append("")

    # ── Recent examples from log ─────────────────────────────────────────────
    recent = log_progress["recent_examples"]
    if recent:
        iter_label = log_progress.get("iteration_num") or "?"
        lines.append(
            f"{_BOLD}Recent Examples{_RESET}  "
            f"{_DIM}(iter {iter_label}, last {len(recent)}){_RESET}"
        )
        for ex in reversed(recent):
            sc     = ex["score"]
            color  = _score_color(sc)
            url    = ex["url"]
            # Trim URL to fit terminal
            short_url = url.split("data.cms.gov/", 1)[-1] if "data.cms.gov/" in url else url
            short_url = short_url[:60] + "…" if len(short_url) > 60 else short_url
            err_flag = f"  {_RED}[ERR]{_RESET}" if ex.get("error") else ""
            lines.append(f"  {color}{sc:.3f}{_RESET}  {short_url}{err_flag}")
        lines.append("")

    # ── Coordinator decisions ────────────────────────────────────────────────
    all_decisions = _parse_log_decisions(log_lines)
    if all_decisions:
        lines.append(f"{_BOLD}Decisions{_RESET}")
        for d in all_decisions[-6:]:
            lines.append(f"  {_YELLOW}▶{_RESET} {d}")
        lines.append("")

    # ── Footer ──────────────────────────────────────────────────────────────
    lines.append(_divider())
    lines.append(f"{_DIM}[Press q + Enter to quit observer — training continues]{_RESET}")
    lines.append("")

    return "\n".join(lines)


# ── Main loop ────────────────────────────────────────────────────────────────

class TrainingObserver:
    """Read-only terminal observer for a training run."""

    def __init__(
        self,
        run_id: int,
        db_path: Path = DEFAULT_DB_PATH,
        refresh_interval: float = 5.0,
    ) -> None:
        self.run_id = run_id
        self.db_path = db_path
        self.refresh_interval = refresh_interval
        self._stop = Event()

    def run(self) -> None:
        """Main loop: refresh display until run completes or user quits."""
        def _watch_quit() -> None:
            try:
                while not self._stop.is_set():
                    line = sys.stdin.readline()
                    if line.strip().lower() == "q":
                        self._stop.set()
                        break
            except Exception:
                pass

        quit_thread = Thread(target=_watch_quit, daemon=True)
        quit_thread.start()

        while not self._stop.is_set():
            self._refresh()
            run = _fetch_run(self.run_id, self.db_path)
            if run and run.get("status") in ("completed", "stopped", "failed"):
                self._refresh()
                print(f"\nTraining run {self.run_id} finished: {run.get('status')}")
                self._stop.set()
                break
            self._stop.wait(timeout=self.refresh_interval)

    def _refresh(self) -> None:
        os.system("clear")
        print(render(self.run_id, self.db_path), end="")


# ── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Watch a DRP collector training run in real time"
    )
    parser.add_argument("run_id", type=int, help="Training run ID to observe")
    parser.add_argument(
        "--db", default=str(DEFAULT_DB_PATH),
        help=f"Path to training database (default: {DEFAULT_DB_PATH})"
    )
    parser.add_argument(
        "--interval", type=float, default=5.0,
        help="Refresh interval in seconds (default: 5)"
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    observer = TrainingObserver(
        run_id=args.run_id,
        db_path=db_path,
        refresh_interval=args.interval,
    )
    try:
        observer.run()
    except KeyboardInterrupt:
        print("\nObserver stopped (training continues in background)")


if __name__ == "__main__":
    main()
