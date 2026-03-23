"""
Observer console for watching a collector training run in real time.

Reads from the training database (read-only) and the run log file.
Refreshes every few seconds. Press 'q' + Enter to quit.
Training continues unaffected.

Usage:
    python -m collector_training.observer <run_id> [--db DB_PATH] [--interval SECS]

Example:
    python -m collector_training.observer 4
    python -m collector_training.observer 4 --interval 3
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

# ANSI
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"
_DIM    = "\033[90m"
_WHITE  = "\033[97m"
_RESET  = "\033[0m"

_FIELD_WEIGHTS = {
    "files": 3.0, "title": 2.0, "summary": 2.0,
    "agency": 1.0, "keywords": 1.0, "time_start": 1.0, "time_end": 1.0,
    "data_types": 1.0, "collection_notes": 1.0, "geographic_coverage": 1.0,
}
_ALL_FIELDS = list(_FIELD_WEIGHTS)


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
    return max(0.0, (_now_utc() - dt).total_seconds()) if dt else 0.0


def _duration_secs(started_at: Optional[str], finished_at: Optional[str]) -> Optional[float]:
    s = _parse_iso(started_at)
    f = _parse_iso(finished_at)
    if s and f:
        return max(0.0, (f - s).total_seconds())
    return None


def _fmt_dur(secs: float) -> str:
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


def _score_color(v: float) -> str:
    return _GREEN if v >= 0.8 else _YELLOW if v >= 0.5 else _RED


def _status_color(s: str) -> str:
    return {"running": _GREEN, "completed": _CYAN,
            "stopped": _YELLOW, "failed": _RED}.get(s, _RESET)


def _mini_bar(v: float, w: int = 22) -> str:
    filled = int(round(v * w))
    return "█" * filled + "░" * (w - filled)


def _pbar(n: int, total: int, elapsed: float = 0.0,
          ncols: int = 68, unit: str = "it") -> str:
    if total == 0:
        return "(no data)"
    return tqdm.format_meter(n=n, total=total, elapsed=elapsed, ncols=ncols, unit=unit)


def _divider(w: int = 72, char: str = "─") -> str:
    return _DIM + char * w + _RESET


def _header2(title: str) -> str:
    return f"{_BOLD}{title}{_RESET}"


# ── Database queries ──────────────────────────────────────────────────────────

def _q(db_path: Path, sql: str, params: tuple = ()) -> list[dict]:
    con = get_connection(db_path)
    try:
        return [dict(r) for r in con.execute(sql, params).fetchall()]
    finally:
        con.close()


def _q1(db_path: Path, sql: str, params: tuple = ()) -> Optional[dict]:
    rows = _q(db_path, sql, params)
    return rows[0] if rows else None


def _fetch_run(run_id: int, db: Path) -> Optional[dict]:
    return _q1(db, "SELECT * FROM training_runs WHERE run_id=?", (run_id,))


def _fetch_iterations(run_id: int, db: Path) -> list[dict]:
    return _q(db,
        "SELECT iteration_id, iteration_num, aggregate_score, per_field_scores_json, "
        "model_used, started_at, finished_at, refinement_strategy "
        "FROM iterations WHERE run_id=? ORDER BY iteration_num", (run_id,))


def _fetch_cost_per_iter(run_id: int, db: Path) -> dict[int, float]:
    rows = _q(db,
        "SELECT iteration_id, SUM(cost_usd) as cost "
        "FROM token_usage WHERE run_id=? GROUP BY iteration_id", (run_id,))
    return {r["iteration_id"]: r["cost"] for r in rows}


def _fetch_total_cost(run_id: int, db: Path) -> float:
    r = _q1(db, "SELECT COALESCE(SUM(cost_usd),0) as c FROM token_usage WHERE run_id=?", (run_id,))
    return r["c"] if r else 0.0


def _fetch_example_counts(run_id: int, db: Path) -> dict[str, int]:
    total = _q1(db, "SELECT COUNT(*) as n FROM training_examples WHERE run_id=?", (run_id,))["n"]
    val   = _q1(db, "SELECT COUNT(*) as n FROM training_examples WHERE run_id=? AND is_validation=1", (run_id,))["n"]
    large = _q1(db, "SELECT COUNT(*) as n FROM training_examples WHERE run_id=? AND data_size_bytes>100000000", (run_id,))["n"]
    return {"total": total, "validation": val, "large": large, "training": total - val}


def _fetch_scored_count(iteration_id: int, db: Path) -> int:
    r = _q1(db, "SELECT COUNT(*) as n FROM project_scores WHERE iteration_id=?", (iteration_id,))
    return r["n"] if r else 0


def _fetch_recent_scores(iteration_id: int, db: Path, limit: int = 12) -> list[dict]:
    return _q(db,
        "SELECT ps.score, ps.error_message, te.source_url "
        "FROM project_scores ps JOIN training_examples te ON ps.example_id=te.example_id "
        "WHERE ps.iteration_id=? ORDER BY ps.score_id DESC LIMIT ?",
        (iteration_id, limit))


# ── Log parsing ───────────────────────────────────────────────────────────────

def _read_log(run_id: int) -> list[str]:
    path = LOGS_DIR / f"run_{run_id}.log"
    if not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []


_ITER_RE   = re.compile(r"Iteration\s+(\d+)/(\d+)")
_EXAMPLE_RE = re.compile(
    r"\|\s*EXAMPLE\s*\|\s*"
    r"(\d+\.\d+)"           # score
    r"\s+(\{[^}]*\})"       # per_field JSON  (no nested braces)
    r"\s+(https?://\S+)"    # url
    r"(?:\s+ERROR:\s*(.+))?" # optional error
)


def _parse_log(lines: list[str]) -> dict[str, Any]:
    """
    Scan log for the current in-progress iteration.
    Returns:
        iter_num          int | None
        iter_total        int | None
        iter_started_ts   str | None
        iter_started_idx  int
        examples          list[{score, per_field, url, error}]
        decisions         list[str]   (all DECIDE lines)
        phase_log_msgs    list[str]   (INFO lines from current iter)
    """
    out: dict[str, Any] = {
        "iter_num": None, "iter_total": None,
        "iter_started_ts": None, "iter_started_idx": -1,
        "examples": [], "decisions": [], "phase_log_msgs": [],
    }

    last_idx = -1
    last_num = None
    last_total = None
    last_ts = None

    for i, line in enumerate(lines):
        if "| INFO    |" in line:
            m = _ITER_RE.search(line)
            if m:
                last_idx   = i
                last_num   = int(m.group(1))
                last_total = int(m.group(2))
                last_ts    = line.split("|")[0].strip()
        if "| DECIDE  |" in line:
            parts = line.split("| DECIDE  |", 1)
            if len(parts) == 2:
                out["decisions"].append(parts[1].strip())
        # also catch slightly different spacing
        elif "| DECIDE |" in line:
            parts = line.split("| DECIDE |", 1)
            if len(parts) == 2:
                out["decisions"].append(parts[1].strip())

    if last_idx == -1:
        return out

    out["iter_num"]          = last_num
    out["iter_total"]        = last_total
    out["iter_started_ts"]   = last_ts
    out["iter_started_idx"]  = last_idx

    examples = []
    phase_msgs = []
    for line in lines[last_idx + 1:]:
        if "| EXAMPLE |" in line:
            m = _EXAMPLE_RE.search(line)
            if m:
                try:
                    pf = json.loads(m.group(2))
                except Exception:
                    pf = {}
                examples.append({
                    "score":     float(m.group(1)),
                    "per_field": pf,
                    "url":       m.group(3),
                    "error":     m.group(4),
                })
        elif "| INFO    |" in line or "| INFO |" in line:
            tag = "| INFO    |" if "| INFO    |" in line else "| INFO |"
            phase_msgs.append(line.split(tag, 1)[1].strip())

    out["examples"]        = examples
    out["phase_log_msgs"]  = phase_msgs
    return out


# ── Render ────────────────────────────────────────────────────────────────────

def render(run_id: int, db_path: Path) -> str:
    run = _fetch_run(run_id, db_path)
    if run is None:
        return f"Run {run_id} not found in {db_path}\n"

    iterations    = _fetch_iterations(run_id, db_path)
    cost_by_iter  = _fetch_cost_per_iter(run_id, db_path)
    total_cost    = _fetch_total_cost(run_id, db_path)
    counts        = _fetch_example_counts(run_id, db_path)
    log_lines     = _read_log(run_id)
    lp            = _parse_log(log_lines)

    cfg: dict[str, Any] = {}
    if run.get("config_json"):
        try:
            cfg = json.loads(run["config_json"])
        except Exception:
            pass

    max_cost      = cfg.get("max_cost_usd", 10.0)
    max_iters     = cfg.get("max_iterations", 20)
    num_workers   = cfg.get("num_eval_workers", 1)
    perfect_thr   = cfg.get("perfect_score_threshold", 0.95)
    status        = run.get("status", "?")
    run_elapsed   = _elapsed_secs(run.get("started_at"))

    completed_iters = sum(1 for it in iterations if it.get("finished_at"))
    in_prog = next((it for it in reversed(iterations) if not it.get("finished_at")), None)
    best_score  = float(run.get("best_score") or 0.0)
    best_it_num = run.get("best_iteration") or 0

    L: list[str] = []

    # ═══ HEADER ═══════════════════════════════════════════════════════════════
    L.append(_divider(72, "═"))
    L.append(
        f"{_BOLD}{_WHITE}  DRP Collector Training — {run.get('collector_name','?')}  "
        f"[run={run_id}  site={run.get('source_site','?')}]{_RESET}"
    )
    L.append(
        f"  Started : {(run.get('started_at') or '')[:16].replace('T',' ')}  "
        f"Elapsed : {_fmt_dur(run_elapsed)}  "
        f"Workers : {num_workers}  "
        f"Status : {_status_color(status)}{_BOLD}{status.upper()}{_RESET}"
    )
    if log_lines:
        L.append(f"  Log     : {_DIM}{LOGS_DIR}/run_{run_id}.log{_RESET}")
    L.append(_divider(72, "═"))
    L.append("")

    # ═══ OVERALL PROGRESS ═════════════════════════════════════════════════════
    L.append(_header2("OVERALL PROGRESS"))
    L.append("")

    # Run iterations
    L.append(f"  {'Iterations':<14} {_pbar(completed_iters, max_iters, run_elapsed, 56, 'iter')}")

    # Budget
    cost_cents = int(total_cost * 100)
    max_cents  = int(max_cost * 100)
    pct_budget = total_cost / max_cost if max_cost > 0 else 0
    bc = _score_color(1.0 - pct_budget)
    L.append(
        f"  {'Budget':<14} {bc}"
        + _pbar(cost_cents, max_cents, run_elapsed, 56, "¢")
        + f"  ${total_cost:.3f}/${max_cost:.2f}{_RESET}"
    )

    # Score toward perfect threshold
    score_pct = min(1.0, best_score / perfect_thr) if perfect_thr > 0 else 0
    sc = _score_color(score_pct)
    L.append(
        f"  {'Best Score':<14} {sc}"
        + _pbar(int(best_score * 1000), int(perfect_thr * 1000), 0, 56, "‰")
        + f"  {best_score:.3f}/{perfect_thr:.2f} target{_RESET}"
    )
    L.append("")

    # ═══ CURRENT ITERATION ════════════════════════════════════════════════════
    if in_prog is not None:
        iter_num     = in_prog["iteration_num"]
        iter_elapsed = _elapsed_secs(in_prog.get("started_at"))
        model_str    = in_prog.get("model_used") or "?"
        strat        = in_prog.get("refinement_strategy")
        n_train      = counts["training"]

        # Phase detection: scored_in_db > 0 means evaluation done, refinement running
        scored_in_db = _fetch_scored_count(in_prog["iteration_id"], db_path)
        log_examples = lp["examples"]
        is_refining  = scored_in_db >= n_train and n_train > 0

        L.append(_divider())
        strat_note = f"  {_DIM}strategy: {strat}{_RESET}" if strat and strat != "plateau_generic" else ""
        L.append(
            f"{_header2(f'CURRENT ITERATION  #{iter_num}')}"
            f"  {_DIM}model={model_str}  elapsed={_fmt_dur(iter_elapsed)}{_RESET}"
            + strat_note
        )
        L.append("")

        if is_refining:
            # ── Refinement phase ────────────────────────────────────────────
            L.append(f"  {_YELLOW}● REFINEMENT IN PROGRESS{_RESET}  "
                     f"model={model_str}  elapsed={_fmt_dur(iter_elapsed)}")
            L.append(f"  All {scored_in_db}/{n_train} examples evaluated — awaiting model response")
            L.append("")

            # Show scores from DB since eval is done
            recent = _fetch_recent_scores(in_prog["iteration_id"], db_path, 12)
            if recent:
                avg = sum(r["score"] for r in recent) / len(recent)
                n_err = sum(1 for r in recent if r.get("error_message"))
                L.append(f"  {_header2('Evaluation Results')}  "
                         f"avg={avg:.3f}  errors={n_err}/{n_train}")
                for r in recent[:8]:
                    sc2   = _score_color(r["score"])
                    short = _short_url(r["source_url"])
                    eflag = f" {_RED}[ERR]{_RESET}" if r.get("error_message") else ""
                    L.append(f"    {sc2}{r['score']:.3f}{_RESET}  {short}{eflag}")
                L.append("")

        else:
            # ── Evaluation phase ─────────────────────────────────────────────
            examples_done = len(log_examples)
            L.append(f"  {_GREEN}● EVALUATION IN PROGRESS{_RESET}")
            L.append(
                "  " + _pbar(examples_done, n_train, iter_elapsed, 66, "ex")
            )

            if examples_done > 0:
                n_err      = sum(1 for e in log_examples if e.get("error"))
                avg_score  = sum(e["score"] for e in log_examples) / examples_done
                score_col  = _score_color(avg_score)
                L.append(
                    f"  Avg score so far : {score_col}{avg_score:.3f}{_RESET}  "
                    f"Errors : {_RED if n_err else _DIM}{n_err}{_RESET}/{examples_done}"
                )
                L.append("")

                # Running per-field averages
                pf_sums: dict[str, list[float]] = {f: [] for f in _ALL_FIELDS}
                for ex in log_examples:
                    for f, v in (ex.get("per_field") or {}).items():
                        if f in pf_sums:
                            pf_sums[f].append(float(v))

                has_pf = any(pf_sums[f] for f in _ALL_FIELDS)
                if has_pf:
                    L.append(f"  {_header2('Running Per-Field Averages')}  "
                             f"{_DIM}({examples_done} examples){_RESET}")
                    ordered = sorted(
                        _ALL_FIELDS,
                        key=lambda f: (sum(pf_sums[f]) / len(pf_sums[f])) if pf_sums[f] else 0
                    )
                    for field in ordered:
                        vals = pf_sums[field]
                        if not vals:
                            continue
                        avg_f = sum(vals) / len(vals)
                        col   = _score_color(avg_f)
                        wt    = _FIELD_WEIGHTS.get(field, 1.0)
                        wt_str = f"w={wt:.0f}" if wt != 1.0 else "   "
                        L.append(
                            f"    {field:<24} {wt_str}  "
                            f"{col}{avg_f:.3f}  {_mini_bar(avg_f, 22)}{_RESET}"
                        )
                    L.append("")

            # Recent examples from log
            recent_log = log_examples[-10:]
            if recent_log:
                L.append(f"  {_header2('Recent Examples')}")
                for ex in reversed(recent_log):
                    sc2   = _score_color(ex["score"])
                    short = _short_url(ex["url"])
                    eflag = f" {_RED}[ERR]{_RESET}" if ex.get("error") else ""
                    L.append(f"    {sc2}{ex['score']:.3f}{_RESET}  {short}{eflag}")
                L.append("")

    # ═══ SCORE HISTORY ════════════════════════════════════════════════════════
    L.append(_divider())
    L.append(_header2("SCORE HISTORY"))
    L.append("")
    if not iterations:
        L.append("  (no iterations yet)")
    else:
        # Table header
        L.append(
            f"  {'It':<4}  {'Score':<7}  {'Δ':<7}  "
            f"{'Model':<18}  {'Cost':<8}  {'Duration':<10}  Bar"
        )
        L.append(f"  {_DIM}{'─'*4}  {'─'*7}  {'─'*7}  {'─'*18}  {'─'*8}  {'─'*10}  {'─'*22}{_RESET}")
        prev_score: Optional[float] = None
        for it in iterations:
            n       = it["iteration_num"]
            score   = it.get("aggregate_score")
            running = not it.get("finished_at")
            model   = (it.get("model_used") or "?").replace("claude-", "").replace("-20251001","")
            cost_it = cost_by_iter.get(it["iteration_id"], 0.0)
            dur     = _duration_secs(it.get("started_at"), it.get("finished_at"))
            dur_str = _fmt_dur(dur) if dur is not None else (_DIM + "running…" + _RESET)

            if running:
                score_str = f"  {_DIM}…{_RESET}"
                delta_str = ""
                bar_str   = _DIM + _mini_bar(0, 22) + _RESET
                col       = _DIM
            elif score is None:
                score_str = "  —"
                delta_str = ""
                bar_str   = ""
                col       = _RESET
            else:
                col       = _score_color(score)
                score_str = f"{col}{score:.3f}{_RESET}"
                bar_str   = col + _mini_bar(score, 22) + _RESET
                if prev_score is not None:
                    d = score - prev_score
                    dc = _GREEN if d > 0.001 else _RED if d < -0.001 else _DIM
                    delta_str = f"{dc}{'+' if d >= 0 else ''}{d:.3f}{_RESET}"
                else:
                    delta_str = ""
                prev_score = score

            tags = []
            if n == best_it_num and score:
                tags.append(f"{_CYAN}best{_RESET}")
            if running:
                tags.append(f"{_GREEN}running{_RESET}")
            tag = f"  ← {', '.join(tags)}" if tags else ""

            cost_str = f"${cost_it:.3f}" if cost_it else "—"
            L.append(
                f"  {n:<4}  {score_str:<7}  {delta_str:<7}  "
                f"{model:<18}  {cost_str:<8}  {dur_str:<10}  {bar_str}{tag}"
            )
    L.append("")

    # ═══ PER-FIELD BREAKDOWN (best iteration) ═════════════════════════════════
    best_it_obj = next((it for it in iterations if it.get("iteration_num") == best_it_num), None)
    ref_it = best_it_obj or next(
        (it for it in reversed(iterations)
         if it.get("aggregate_score") and it["aggregate_score"] > 0), None
    )
    if ref_it and ref_it.get("per_field_scores_json"):
        try:
            pf = json.loads(ref_it["per_field_scores_json"])
            L.append(_divider())
            L.append(
                f"{_header2('PER-FIELD BREAKDOWN')}  "
                f"{_DIM}iteration {ref_it['iteration_num']}  "
                f"(score={ref_it.get('aggregate_score', 0):.3f}){_RESET}"
            )
            L.append("")

            # Two-column comparison: best vs baseline (iter 1)
            baseline_pf: dict[str, float] = {}
            if len(iterations) > 1:
                it1 = next((it for it in iterations if it["iteration_num"] == 1), None)
                if it1 and it1.get("per_field_scores_json"):
                    try:
                        baseline_pf = json.loads(it1["per_field_scores_json"])
                    except Exception:
                        pass

            col_hdr = f"  {'Field':<24}  {'Baseline':>8}  {'Best':>8}  {'Change':>7}  {'Bar (best)'}"
            L.append(col_hdr)
            L.append(f"  {_DIM}{'─'*24}  {'─'*8}  {'─'*8}  {'─'*7}  {'─'*22}{_RESET}")

            ordered = sorted(pf.items(), key=lambda x: x[1])
            for field, best_val in ordered:
                col     = _score_color(best_val)
                bar_str = col + _mini_bar(best_val, 22) + _RESET
                wt      = _FIELD_WEIGHTS.get(field, 1.0)
                wt_str  = f"(w={wt:.0f})" if wt != 1.0 else "      "

                base_val = baseline_pf.get(field)
                if base_val is not None and ref_it["iteration_num"] != 1:
                    delta = best_val - base_val
                    dc = _GREEN if delta > 0.001 else _RED if delta < -0.001 else _DIM
                    chg_str = f"{dc}{'+' if delta >= 0 else ''}{delta:.3f}{_RESET}"
                    base_str = f"{base_val:.3f}"
                else:
                    chg_str  = _DIM + "  —" + _RESET
                    base_str = "—"

                L.append(
                    f"  {field:<24}  {base_str:>8}  "
                    f"{col}{best_val:.3f}{_RESET}  {chg_str:>7}  {bar_str}  {_DIM}{wt_str}{_RESET}"
                )
            L.append("")
        except Exception:
            pass

    # ═══ DATASET ══════════════════════════════════════════════════════════════
    L.append(_divider())
    L.append(_header2("DATASET"))
    L.append(
        f"  Training   : {counts['training']} examples"
        + (f"  ({counts['large']} large — skipped on non-full-fidelity iters)" if counts["large"] else "")
    )
    L.append(f"  Validation : {counts['validation']} examples (held out)")
    L.append("")

    # ═══ COORDINATOR DECISIONS ════════════════════════════════════════════════
    all_decisions = lp["decisions"]
    if all_decisions:
        L.append(_divider())
        L.append(_header2("COORDINATOR DECISIONS"))
        for d in all_decisions[-8:]:
            L.append(f"  {_YELLOW}▶{_RESET} {d}")
        L.append("")

    # ═══ FOOTER ═══════════════════════════════════════════════════════════════
    L.append(_divider(72, "═"))
    L.append(f"{_DIM}  [q + Enter to quit observer — training continues in background]{_RESET}")
    L.append("")

    return "\n".join(L)


def _short_url(url: str, width: int = 60) -> str:
    """Shorten a data.cms.gov URL to just the dataset slug."""
    slug = url.split("data.cms.gov/", 1)[-1] if "data.cms.gov/" in url else url
    return slug[:width] + "…" if len(slug) > width else slug


# ── Main loop ─────────────────────────────────────────────────────────────────

class TrainingObserver:
    def __init__(self, run_id: int, db_path: Path = DEFAULT_DB_PATH,
                 refresh_interval: float = 5.0) -> None:
        self.run_id = run_id
        self.db_path = db_path
        self.refresh_interval = refresh_interval
        self._stop = Event()

    def run(self) -> None:
        def _watch_quit() -> None:
            try:
                while not self._stop.is_set():
                    if sys.stdin.readline().strip().lower() == "q":
                        self._stop.set()
                        break
            except Exception:
                pass

        Thread(target=_watch_quit, daemon=True).start()

        while not self._stop.is_set():
            self._refresh()
            run = _fetch_run(self.run_id, self.db_path)
            if run and run.get("status") in ("completed", "stopped", "failed"):
                self._refresh()
                print(f"\nRun {self.run_id} finished: {run.get('status')}")
                self._stop.set()
                break
            self._stop.wait(timeout=self.refresh_interval)

    def _refresh(self) -> None:
        os.system("clear")
        print(render(self.run_id, self.db_path), end="")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Watch a DRP collector training run")
    parser.add_argument("run_id", type=int)
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--interval", type=float, default=5.0)
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    try:
        TrainingObserver(args.run_id, db_path, args.interval).run()
    except KeyboardInterrupt:
        print("\nObserver stopped (training continues)")


if __name__ == "__main__":
    main()
