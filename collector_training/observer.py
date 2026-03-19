"""
Observer console for watching a collector training run in real time.

Reads directly from the training database (read-only).
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
import sys
import time
from pathlib import Path
from threading import Event, Thread
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from collector_training.schema import get_connection, DEFAULT_DB_PATH

# ANSI colors
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"
_DIM    = "\033[90m"
_RESET  = "\033[0m"


# ── Database queries ─────────────────────────────────────────────────────────

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
            "SELECT iteration_num, aggregate_score, per_field_scores_json, "
            "model_used, started_at, finished_at "
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
            "SELECT COUNT(*) FROM training_examples WHERE run_id=? AND data_size_bytes > 100000000",
            (run_id,),
        ).fetchone()[0]
        return {"total": total, "validation": val, "large": large, "training": total - val}
    finally:
        con.close()


# ── Rendering ────────────────────────────────────────────────────────────────

def _bar(value: float, width: int = 20) -> str:
    """Render a horizontal bar for a [0,1] score."""
    filled = int(round(value * width))
    return "█" * filled + "░" * (width - filled)


def _elapsed(started_at: Optional[str]) -> str:
    if not started_at:
        return "?"
    try:
        from datetime import datetime, timezone
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - start
        secs = int(delta.total_seconds())
        if secs < 60:
            return f"{secs}s"
        m, s = divmod(secs, 60)
        if m < 60:
            return f"{m}m {s}s"
        h, m = divmod(m, 60)
        return f"{h}h {m}m"
    except Exception:
        return "?"


def _status_color(status: str) -> str:
    return {
        "running":   _GREEN,
        "completed": _CYAN,
        "stopped":   _YELLOW,
        "failed":    _RED,
    }.get(status, _RESET)


def render(run_id: int, db_path: Path) -> str:
    """Build the full display string for one refresh cycle."""
    run = _fetch_run(run_id, db_path)
    if run is None:
        return f"Run {run_id} not found in {db_path}\n"

    iterations = _fetch_iterations(run_id, db_path)
    cost = _fetch_cost(run_id, db_path)
    counts = _fetch_example_counts(run_id, db_path)

    config: dict[str, Any] = {}
    if run.get("config_json"):
        try:
            config = json.loads(run["config_json"])
        except Exception:
            pass

    max_cost = config.get("max_cost_usd", 10.0)
    status = run.get("status", "?")
    status_color = _status_color(status)

    lines: list[str] = []

    # ── Header ──────────────────────────────────────────────────────────────
    lines.append(
        f"{_BOLD}DRP Collector Training — {run.get('collector_name', '?')}  "
        f"[run_id={run_id}]{_RESET}"
    )
    lines.append(
        f"Started: {(run.get('started_at') or '')[:16].replace('T', ' ')}  "
        f"Elapsed: {_elapsed(run.get('started_at'))}  "
        f"Status: {status_color}{_BOLD}{status.upper()}{_RESET}"
    )
    lines.append("")

    # ── Score trend ──────────────────────────────────────────────────────────
    lines.append(f"{_BOLD}Score trend{_RESET}")
    current_it = len(iterations)
    if not iterations:
        lines.append("  (no iterations yet)")
    else:
        best_score = run.get("best_score") or 0.0
        best_it = run.get("best_iteration") or 0
        for it in iterations:
            score = it.get("aggregate_score")
            if score is None:
                score_str = "  (running)"
                bar_str = ""
            else:
                bar_str = _bar(score)
                score_str = f"  {score:.3f}"
            marker = " ← best" if it["iteration_num"] == best_it else ""
            marker += " ← current" if it["iteration_num"] == current_it else ""
            lines.append(
                f"  It {it['iteration_num']:<3}  {bar_str}{score_str}{_DIM}{marker}{_RESET}"
            )

    lines.append("")

    # ── Per-field breakdown (latest completed iteration) ────────────────────
    latest_complete = next(
        (it for it in reversed(iterations) if it.get("aggregate_score") is not None),
        None,
    )
    if latest_complete and latest_complete.get("per_field_scores_json"):
        try:
            per_field = json.loads(latest_complete["per_field_scores_json"])
            lines.append(
                f"{_BOLD}Per-field (iteration {latest_complete['iteration_num']}){_RESET}"
            )
            for field, score in sorted(per_field.items(), key=lambda x: x[1]):
                color = _GREEN if score >= 0.8 else _YELLOW if score >= 0.5 else _RED
                lines.append(
                    f"  {field:<22}  {color}{score:.2f}  {_bar(score, 20)}{_RESET}"
                )
            lines.append("")
        except Exception:
            pass

    # ── Cost ─────────────────────────────────────────────────────────────────
    pct = cost / max_cost * 100 if max_cost > 0 else 0
    cost_color = _GREEN if pct < 50 else _YELLOW if pct < 75 else _RED
    lines.append(
        f"{_BOLD}Cost{_RESET}  "
        f"{cost_color}${cost:.4f} of ${max_cost:.2f} budget ({pct:.0f}%){_RESET}"
    )

    lines.append(
        f"  Training set:  {counts['training']} examples  "
        f"({counts['large']} large)"
    )
    lines.append(f"  Validation:    {counts['validation']} examples (held out)")
    if run.get("best_score") is not None:
        lines.append(
            f"  Best score:    {run['best_score']:.3f}  "
            f"(iteration {run.get('best_iteration', '?')})"
        )

    lines.append("")
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
        # Background thread watches for 'q' input
        def _watch_quit():
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
                # Show final state and exit
                self._refresh()
                print(f"\nTraining run {self.run_id} finished: {run.get('status')}")
                self._stop.set()
                break
            # Wait, but wake up if stop is set
            self._stop.wait(timeout=self.refresh_interval)

    def _refresh(self) -> None:
        # Clear screen (works on macOS/Linux terminals)
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
