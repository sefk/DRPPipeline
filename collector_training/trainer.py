"""
Core training loop: evaluate a collector against training examples,
build a refinement prompt, and call an AI model for improved code.

Phase 2: Single-threaded iteration loop with direct Anthropic API calls.
Phase 3: DSPy integration replaces SimpleRefiner (see dspy_modules.py).
"""
from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from collector_training.schema import get_connection, init_eval_db, DEFAULT_DB_PATH
from collector_training.scorer import (
    best_annotator_score,
    per_field_averages,
    score_project,
    FIELD_WEIGHTS,
)

PROJECT_ROOT = Path(__file__).parent.parent
COLLECTORS_DIR = PROJECT_ROOT / "collectors"
VERSIONS_DIR = PROJECT_ROOT / "collector_training" / "versions"
EVAL_DB_PATH = PROJECT_ROOT / "collector_training_eval.db"


# ── Model pricing (USD per 1M tokens) ──────────────────────────────────────

_MODEL_PRICING: dict[str, tuple[float, float]] = {
    "claude-opus-4-6":           (15.0,  75.0),
    "claude-sonnet-4-6":          (3.0,  15.0),
    "claude-haiku-4-5-20251001":  (0.8,   4.0),
    "gemini-2.5-flash":           (0.15,  0.60),
    "gemini-2.5-pro":             (3.50, 10.50),
}


def _compute_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    prices = _MODEL_PRICING.get(model, (3.0, 15.0))
    return (input_tokens * prices[0] + output_tokens * prices[1]) / 1_000_000


# ── Collector evaluator ─────────────────────────────────────────────────────

class CollectorEvaluator:
    """
    Evaluates a candidate collector against training examples.

    Evaluation strategy:
      1. Write candidate code to the collector's source file.
      2. Create a temporary project in a dedicated eval SQLite DB.
      3. Run `python main.py <module_name> --db-path <eval_db> --num-rows 1
                             --start-drpid <drpid>` as a subprocess.
      4. Read the result back; enumerate files in folder_path.
      5. Restore the original collector code; delete the eval record.
    """

    def __init__(
        self,
        collector_module_name: str,
        collector_file: Path,
        eval_db_path: Optional[Path] = None,
        subprocess_timeout: int = 300,
    ) -> None:
        self.collector_module_name = collector_module_name
        self.collector_file = collector_file
        self.eval_db_path = eval_db_path or EVAL_DB_PATH
        self.subprocess_timeout = subprocess_timeout
        init_eval_db(self.eval_db_path)

    # ── Internal helpers ────────────────────────────────────────────────────

    def _write_candidate(self, code: str) -> str:
        """Replace collector file with candidate code; return original."""
        original = self.collector_file.read_text(encoding="utf-8")
        self.collector_file.write_text(code, encoding="utf-8")
        return original

    def _restore_original(self, original_code: str) -> None:
        self.collector_file.write_text(original_code, encoding="utf-8")

    def _create_eval_record(self, source_url: str) -> int:
        """Insert a sourced project into the eval DB; return DRPID."""
        con = sqlite3.connect(str(self.eval_db_path), timeout=30.0)
        try:
            # Remove any stale record for this URL
            con.execute("DELETE FROM projects WHERE source_url=?", (source_url,))
            cur = con.execute(
                "INSERT INTO projects (source_url, status) VALUES (?,?)",
                (source_url, "sourced"),
            )
            con.commit()
            return cur.lastrowid
        finally:
            con.close()

    def _delete_eval_record(self, drpid: int) -> None:
        con = sqlite3.connect(str(self.eval_db_path), timeout=30.0)
        try:
            con.execute("DELETE FROM projects WHERE DRPID=?", (drpid,))
            con.commit()
        finally:
            con.close()

    def _read_eval_record(self, drpid: int) -> Optional[dict[str, Any]]:
        con = sqlite3.connect(str(self.eval_db_path), timeout=30.0)
        con.row_factory = sqlite3.Row
        try:
            row = con.execute(
                "SELECT * FROM projects WHERE DRPID=?", (drpid,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            con.close()

    def _list_output_files(self, folder_path: Optional[str]) -> list[dict[str, Any]]:
        if not folder_path or not Path(folder_path).exists():
            return []
        return [
            {"name": f.name, "size": f.stat().st_size}
            for f in sorted(Path(folder_path).iterdir())
            if f.is_file()
        ]

    def _run_subprocess(self, drpid: int) -> tuple[int, str, str]:
        """Run the collector subprocess; return (returncode, stdout, stderr)."""
        import os
        cmd = [
            sys.executable, str(PROJECT_ROOT / "main.py"),
            self.collector_module_name,
            "--db-path", str(self.eval_db_path),
            "--num-rows", "1",
            "--start-drpid", str(drpid),
        ]
        env = {**os.environ, "DRP_TRAINING_MODE": "1"}
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.subprocess_timeout,
                cwd=str(PROJECT_ROOT),
                env=env,
            )
            return proc.returncode, proc.stdout, proc.stderr
        except subprocess.TimeoutExpired:
            return -1, "", f"Collector timed out after {self.subprocess_timeout}s"
        except Exception as exc:
            return -1, "", f"Subprocess error: {exc}"

    # ── Public evaluation methods ───────────────────────────────────────────

    def _eval_one_inner(
        self,
        source_url: str,
        ground_truth: dict[str, Any],
        example_id: Optional[int],
    ) -> dict[str, Any]:
        """
        Evaluate the already-written collector against one URL.
        Does NOT touch the collector file — caller manages that.
        Thread-safe: each call uses its own DB record and subprocess.
        """
        drpid = self._create_eval_record(source_url)
        error_message: Optional[str] = None
        collector_output: dict[str, Any] = {}

        try:
            returncode, stdout, stderr = self._run_subprocess(drpid)
            record = self._read_eval_record(drpid)

            if record is None:
                collector_output = {"_crashed": True}
                error_message = "Record disappeared after subprocess run"
            elif record.get("status") == "error" or record.get("errors"):
                collector_output = dict(record)
                collector_output["_crashed"] = True
                error_message = record.get("errors") or "status=error"
            elif returncode != 0 and not record.get("folder_path"):
                collector_output = {"_crashed": True}
                error_message = stderr[:500] if stderr else f"returncode={returncode}"
            else:
                collector_output = dict(record)
                folder_path_str = record.get("folder_path")
                planned_json = (
                    Path(folder_path_str) / "planned_files.json"
                    if folder_path_str else None
                )
                if planned_json and planned_json.exists():
                    with open(planned_json, encoding="utf-8") as fh:
                        collector_output["files"] = json.load(fh)
                else:
                    collector_output["files"] = self._list_output_files(folder_path_str)

            scored = score_project(collector_output, ground_truth)
            return {
                "score":             scored["aggregate"],
                "per_field":         {f: scored[f] for f in FIELD_WEIGHTS},
                "diff":              scored["diff"],
                "collector_output":  collector_output,
                "error_message":     error_message,
                "source_url":        source_url,
                "example_id":        example_id,
            }
        finally:
            self._delete_eval_record(drpid)

    def evaluate_one(
        self,
        source_url: str,
        ground_truth: dict[str, Any],
        candidate_code: str,
    ) -> dict[str, Any]:
        """
        Evaluate candidate collector against one URL.
        Writes and restores the collector file around the evaluation.

        Returns dict with: score, per_field, diff, collector_output, error_message.
        """
        original_code = self._write_candidate(candidate_code)
        try:
            return self._eval_one_inner(source_url, ground_truth, example_id=None)
        finally:
            self._restore_original(original_code)

    def evaluate_all(
        self,
        examples: list[dict[str, Any]],
        candidate_code: str,
        num_workers: int = 1,
        on_example_done: Optional[Callable[[dict[str, Any]], None]] = None,
    ) -> list[dict[str, Any]]:
        """
        Evaluate candidate against all training examples.

        Writes the candidate file ONCE, runs all evaluations (optionally in
        parallel using ThreadPoolExecutor), then restores the original.
        Safe for num_workers > 1 because subprocesses import the file at
        startup — the file is stable for the duration of all subprocesses.
        """
        original_code = self._write_candidate(candidate_code)
        results: list[dict[str, Any]] = []

        def _eval(ex: dict[str, Any]) -> dict[str, Any]:
            gt = ex.get("ground_truth") or json.loads(ex.get("ground_truth_json", "{}"))
            result = self._eval_one_inner(ex["source_url"], gt, ex.get("example_id"))
            if on_example_done is not None:
                on_example_done(result)
            return result

        try:
            if num_workers <= 1:
                for ex in examples:
                    results.append(_eval(ex))
            else:
                with ThreadPoolExecutor(max_workers=num_workers) as pool:
                    futures = {pool.submit(_eval, ex): ex for ex in examples}
                    for fut in as_completed(futures):
                        results.append(fut.result())
        finally:
            self._restore_original(original_code)

        return results


# ── Refinement prompt ───────────────────────────────────────────────────────

def build_refinement_prompt(
    collector_code: str,
    aggregate_score: float,
    worst_cases: list[dict[str, Any]],
    field_breakdown: dict[str, float],
    interface_spec: str,
    refinement_strategy: Optional[str] = None,
) -> str:
    """
    Build the prompt sent to the AI model for collector refinement.
    """
    field_lines = "\n".join(
        f"  {field:<22} {score:.3f}"
        for field, score in sorted(field_breakdown.items(), key=lambda x: x[1])
    )

    worst_text_parts = []
    for i, case in enumerate(worst_cases[:5], 1):
        diff = case.get("diff", {})
        diff_lines = []
        for field, d in diff.items():
            expected = d.get("expected")
            actual = d.get("actual")
            if expected != actual:
                diff_lines.append(f"    {field}:\n      expected: {expected!r}\n      actual:   {actual!r}")
        diff_str = "\n".join(diff_lines) or "    (no diff available)"
        worst_text_parts.append(
            f"  {i}. {case.get('source_url', '?')}  (score: {case.get('score', 0):.3f})\n"
            + diff_str
        )

    worst_text = "\n\n".join(worst_text_parts) or "  (none)"

    strategy_section = ""
    if refinement_strategy:
        strategy_section = f"\nFocus area for this refinement: {refinement_strategy}\n"

    return textwrap.dedent(f"""
    You are improving a DRP Pipeline collector. The collector is a Python class
    that fetches datasets from a specific website and extracts metadata.

    Current aggregate score: {aggregate_score:.3f} / 1.000
    {strategy_section}
    Per-field scores (lower = needs work):
    {field_lines}

    Worst-scoring projects (expected vs actual):
    {worst_text}

    Collector interface spec:
    {interface_spec}

    Current collector source code:
    ```python
    {collector_code}
    ```

    Instructions:
    1. Analyze the diffs above to identify what extraction patterns are failing.
    2. Focus on the lowest-scoring fields, especially 'files' (weight 3.0) and
       'title'/'summary' (weight 2.0 each).
    3. Modify the collector to fix the identified issues.
    4. Return the COMPLETE updated collector Python file — nothing else.
       Do not include ``` markers or any prose before or after the code.
    """).strip()


# ── Simple AI refiner (Phase 2) ─────────────────────────────────────────────

class SimpleRefiner:
    """
    Phase 2 refiner: calls the Anthropic API directly with a structured prompt.
    Replaced by DSPy optimizer in Phase 3 (see dspy_modules.py).
    """

    def __init__(self, model: str = "claude-sonnet-4-6") -> None:
        self.model = model

    def refine(
        self,
        collector_code: str,
        aggregate_score: float,
        worst_cases: list[dict[str, Any]],
        field_breakdown: dict[str, float],
        interface_spec: str,
        refinement_strategy: Optional[str] = None,
    ) -> tuple[str, dict[str, Any]]:
        """
        Call the AI model and return (improved_code, token_usage_dict).
        """
        try:
            import anthropic
        except ImportError:
            raise ImportError(
                "anthropic package required for AI refinement. "
                "Install with: pip install anthropic"
            )

        prompt = build_refinement_prompt(
            collector_code=collector_code,
            aggregate_score=aggregate_score,
            worst_cases=worst_cases,
            field_breakdown=field_breakdown,
            interface_spec=interface_spec,
            refinement_strategy=refinement_strategy,
        )

        client = anthropic.Anthropic()
        message = client.messages.create(
            model=self.model,
            max_tokens=8192,
            messages=[{"role": "user", "content": prompt}],
        )

        improved_code = message.content[0].text.strip()
        # Strip any accidental ``` fencing
        improved_code = re.sub(r"^```(?:python)?\s*\n?", "", improved_code)
        improved_code = re.sub(r"\n?```\s*$", "", improved_code)

        usage = {
            "input_tokens":  message.usage.input_tokens,
            "output_tokens": message.usage.output_tokens,
            "cost_usd":      _compute_cost(
                self.model,
                message.usage.input_tokens,
                message.usage.output_tokens,
            ),
        }
        return improved_code, usage


# ── Single iteration ────────────────────────────────────────────────────────

def run_iteration(
    run_id: int,
    iteration_num: int,
    collector_code: str,
    training_examples: list[dict[str, Any]],
    evaluator: CollectorEvaluator,
    refiner: SimpleRefiner,
    interface_spec: str,
    db_path: Optional[Path] = None,
    refinement_strategy: Optional[str] = None,
    num_workers: int = 1,
    on_example_done: Optional[Callable[[dict[str, Any]], None]] = None,
) -> dict[str, Any]:
    """
    Execute one evaluate → analyze → refine cycle.

    Returns dict with:
      iteration_id, aggregate_score, per_field, worst_cases,
      new_code, token_usage, finished_at
    """
    db_path = db_path or DEFAULT_DB_PATH
    now_iso = datetime.now(timezone.utc).isoformat()

    # Insert iteration record (open)
    con = get_connection(db_path)
    try:
        cur = con.execute(
            "INSERT INTO iterations "
            "(run_id, iteration_num, started_at, collector_code, "
            " refinement_strategy, model_used) "
            "VALUES (?,?,?,?,?,?)",
            (run_id, iteration_num, now_iso, collector_code,
             refinement_strategy, refiner.model),
        )
        iteration_id = cur.lastrowid
        con.commit()
    finally:
        con.close()

    # 1. Evaluate
    results = evaluator.evaluate_all(
        training_examples, collector_code,
        num_workers=num_workers,
        on_example_done=on_example_done,
    )

    aggregate_score = (
        sum(r["score"] for r in results) / len(results) if results else 0.0
    )
    field_breakdown = per_field_averages([r["per_field"] for r in results])

    # 2. Persist per-project scores
    con = get_connection(db_path)
    try:
        for result in results:
            eid = result.get("example_id")
            if eid is None:
                continue
            con.execute(
                "INSERT INTO project_scores "
                "(iteration_id, example_id, score, per_field_json, diff_json, "
                " collector_output_json, error_message) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    iteration_id,
                    eid,
                    result["score"],
                    json.dumps(result.get("per_field")),
                    json.dumps(result.get("diff")),
                    json.dumps(result.get("collector_output")),
                    result.get("error_message"),
                ),
            )
        con.commit()
    finally:
        con.close()

    # 3. Identify worst cases for the refinement prompt
    worst_cases = sorted(results, key=lambda r: r["score"])[:5]

    # 4. Refine
    new_code, token_usage = refiner.refine(
        collector_code=collector_code,
        aggregate_score=aggregate_score,
        worst_cases=worst_cases,
        field_breakdown=field_breakdown,
        interface_spec=interface_spec,
        refinement_strategy=refinement_strategy,
    )

    # 5. Update iteration record with results
    finished_at = datetime.now(timezone.utc).isoformat()
    con = get_connection(db_path)
    try:
        con.execute(
            "UPDATE iterations SET finished_at=?, aggregate_score=?, "
            "per_field_scores_json=? WHERE iteration_id=?",
            (
                finished_at,
                aggregate_score,
                json.dumps(field_breakdown),
                iteration_id,
            ),
        )
        # Record token usage
        con.execute(
            "INSERT INTO token_usage "
            "(iteration_id, run_id, step, model, input_tokens, output_tokens, "
            " cost_usd, timestamp) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                iteration_id,
                run_id,
                "refine",
                refiner.model,
                token_usage["input_tokens"],
                token_usage["output_tokens"],
                token_usage["cost_usd"],
                finished_at,
            ),
        )
        con.commit()
    finally:
        con.close()

    return {
        "iteration_id":     iteration_id,
        "iteration_num":    iteration_num,
        "aggregate_score":  aggregate_score,
        "per_field":        field_breakdown,
        "worst_cases":      worst_cases,
        "new_code":         new_code,
        "token_usage":      token_usage,
        "finished_at":      finished_at,
    }
