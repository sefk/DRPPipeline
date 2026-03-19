"""
Training coordinator: manages the full collector training loop.

Handles stopping conditions, version management, cost tracking, and
deciding which iteration's code to promote as the final collector.
"""
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from collector_training.schema import get_connection, init_db, DEFAULT_DB_PATH
from collector_training.importer import list_examples
from collector_training.trainer import (
    CollectorEvaluator,
    SimpleRefiner,
    VERSIONS_DIR,
)

PROJECT_ROOT = Path(__file__).parent.parent
COLLECTORS_DIR = PROJECT_ROOT / "collectors"


@dataclass
class TrainingConfig:
    """Configuration for a collector training run."""

    # Required
    collector_name: str           # e.g. "CmsGovCollector"
    collector_module_name: str    # e.g. "cms_collector"
    source_site: str              # e.g. "data.cms.gov"

    # Stopping conditions
    max_iterations: int = 20
    max_cost_usd: float = 10.0
    score_plateau_threshold: float = 0.01
    score_plateau_window: int = 3
    perfect_score_threshold: float = 0.95

    # Model configuration
    model_refine: str = "claude-sonnet-4-6"

    # Cost-aware model switching
    cheap_model: str = "claude-haiku-4-5-20251001"
    switch_to_cheap_at_pct: float = 0.50   # Switch after 50% budget used
    warn_at_pct: float = 0.75

    # Evaluation
    num_eval_workers: int = 1
    subprocess_timeout: int = 300

    # Data
    large_dataset_threshold_bytes: int = 100_000_000  # 100 MB
    full_fidelity_every_n_iters: int = 5

    # Extra notes stored in DB
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}


@dataclass
class TrainingResult:
    run_id: int
    best_score: float
    best_iteration: int
    total_cost_usd: float
    stop_reason: str
    best_collector_path: Optional[Path] = None


class TrainingCoordinator:
    """
    Orchestrates the collector training loop.

    Usage:
        config = TrainingConfig(
            collector_name="CmsGovCollector",
            collector_module_name="cms_collector",
            source_site="data.cms.gov",
        )
        coord = TrainingCoordinator(config)
        result = coord.start_and_run(initial_code=open("collectors/CmsGovCollector.py").read())
    """

    def __init__(
        self,
        config: TrainingConfig,
        db_path: Optional[Path] = None,
    ) -> None:
        self.config = config
        self.db_path = db_path or DEFAULT_DB_PATH
        init_db(self.db_path)

        VERSIONS_DIR.mkdir(parents=True, exist_ok=True)

        # Locate the collector source file
        self.collector_file = COLLECTORS_DIR / f"{config.collector_name}.py"
        if not self.collector_file.exists():
            raise FileNotFoundError(
                f"Collector file not found: {self.collector_file}"
            )

        # Load the collector interface spec (lazy — only when first needed)
        self._interface_spec: Optional[str] = None

    # ── Public entry point ──────────────────────────────────────────────────

    def create_run(self, initial_collector_code: Optional[str] = None) -> int:
        """
        Create a new training run record. Returns run_id.
        Uses the collector file on disk if initial_collector_code is not given.
        """
        if initial_collector_code is None:
            initial_collector_code = self.collector_file.read_text(encoding="utf-8")

        now = datetime.now(timezone.utc).isoformat()
        con = get_connection(self.db_path)
        try:
            cur = con.execute(
                "INSERT INTO training_runs "
                "(collector_name, source_site, started_at, status, config_json, notes) "
                "VALUES (?,?,?,?,?,?)",
                (
                    self.config.collector_name,
                    self.config.source_site,
                    now,
                    "running",
                    json.dumps(self.config.to_dict()),
                    self.config.notes,
                ),
            )
            run_id = cur.lastrowid
            con.commit()
        finally:
            con.close()

        # Save the initial (v0) version
        self._save_version(run_id, 0, initial_collector_code)
        return run_id

    def run(self, run_id: int) -> TrainingResult:
        """
        Execute the training loop for an existing run_id.
        Training examples must be imported before calling this.
        """
        training_examples = list_examples(
            run_id, include_validation=False, db_path=self.db_path
        )
        if not training_examples:
            raise ValueError(
                f"No training examples found for run_id={run_id}. "
                "Import training data first (see importer.import_from_*)."
            )

        print(f"Starting training run {run_id}: {len(training_examples)} training examples")

        # Load initial code from version 0
        current_code = self._load_version(run_id, 0)
        scores: list[float] = []
        total_cost = 0.0
        best_score = 0.0
        best_iteration = 0
        stop_reason = "max_iterations"

        evaluator = CollectorEvaluator(
            collector_module_name=self.config.collector_module_name,
            collector_file=self.collector_file,
            subprocess_timeout=self.config.subprocess_timeout,
        )

        for iteration_num in range(1, self.config.max_iterations + 1):
            # Cost-aware model selection
            budget_used_pct = total_cost / self.config.max_cost_usd if self.config.max_cost_usd > 0 else 0
            if budget_used_pct >= self.config.warn_at_pct:
                print(f"  Warning: {budget_used_pct:.0%} of budget used (${total_cost:.2f})")
            model = (
                self.config.cheap_model
                if budget_used_pct >= self.config.switch_to_cheap_at_pct
                else self.config.model_refine
            )
            refiner = SimpleRefiner(model=model)

            # Filter large datasets on non-full-fidelity iterations
            if iteration_num % self.config.full_fidelity_every_n_iters != 0:
                examples = self._filter_large_datasets(training_examples)
            else:
                examples = training_examples

            print(f"  Iteration {iteration_num}/{self.config.max_iterations} "
                  f"({len(examples)} examples, model={model})")

            from collector_training.trainer import run_iteration
            result = run_iteration(
                run_id=run_id,
                iteration_num=iteration_num,
                collector_code=current_code,
                training_examples=examples,
                evaluator=evaluator,
                refiner=refiner,
                interface_spec=self._get_interface_spec(),
                db_path=self.db_path,
                num_workers=self.config.num_eval_workers,
            )

            agg_score = result["aggregate_score"]
            scores.append(agg_score)
            total_cost += result["token_usage"]["cost_usd"]

            print(f"    Score: {agg_score:.3f}  Cost: ${result['token_usage']['cost_usd']:.4f}  "
                  f"Total: ${total_cost:.4f}")

            # Save new version
            new_code = result["new_code"]
            self._save_version(run_id, iteration_num, new_code)

            # Update best
            if agg_score > best_score:
                best_score = agg_score
                best_iteration = iteration_num

            # Update run record
            self._update_run(run_id, best_score, best_iteration, total_cost)

            # Advance to new code for next iteration
            current_code = new_code

            # Check stopping conditions
            should_stop, reason = self._should_stop(
                scores=scores, total_cost=total_cost, agg_score=agg_score
            )
            if should_stop:
                stop_reason = reason
                print(f"  Stopping: {reason}")
                break

        # Finalize: copy best version to canonical file
        best_path = self._finalize(run_id, best_iteration)

        # Mark run complete
        now = datetime.now(timezone.utc).isoformat()
        con = get_connection(self.db_path)
        try:
            con.execute(
                "UPDATE training_runs SET finished_at=?, status=? WHERE run_id=?",
                (now, "completed", run_id),
            )
            con.commit()
        finally:
            con.close()

        return TrainingResult(
            run_id=run_id,
            best_score=best_score,
            best_iteration=best_iteration,
            total_cost_usd=total_cost,
            stop_reason=stop_reason,
            best_collector_path=best_path,
        )

    def start_and_run(self, initial_collector_code: Optional[str] = None) -> TrainingResult:
        """Convenience: create a run and immediately execute it."""
        run_id = self.create_run(initial_collector_code)
        return self.run(run_id)

    # ── Stopping conditions ─────────────────────────────────────────────────

    def _should_stop(
        self,
        scores: list[float],
        total_cost: float,
        agg_score: float,
    ) -> tuple[bool, str]:
        if agg_score >= self.config.perfect_score_threshold:
            return True, f"perfect_score ({agg_score:.3f} >= {self.config.perfect_score_threshold})"

        if total_cost >= self.config.max_cost_usd:
            return True, f"cost_limit (${total_cost:.4f} >= ${self.config.max_cost_usd})"

        w = self.config.score_plateau_window
        if len(scores) >= w + 1:
            recent = scores[-(w + 1):]
            improvement = max(recent[1:]) - recent[0]
            if improvement < self.config.score_plateau_threshold:
                return True, (
                    f"score_plateau (max improvement {improvement:.4f} < "
                    f"{self.config.score_plateau_threshold} over last {w} iterations)"
                )

        return False, ""

    # ── Version management ──────────────────────────────────────────────────

    def _version_path(self, run_id: int, iteration_num: int) -> Path:
        name = f"{self.config.collector_name}_run{run_id}_v{iteration_num}.py"
        return VERSIONS_DIR / name

    def _save_version(self, run_id: int, iteration_num: int, code: str) -> Path:
        path = self._version_path(run_id, iteration_num)
        path.write_text(code, encoding="utf-8")
        return path

    def _load_version(self, run_id: int, iteration_num: int) -> str:
        path = self._version_path(run_id, iteration_num)
        if not path.exists():
            # Fall back to the live collector file
            return self.collector_file.read_text(encoding="utf-8")
        return path.read_text(encoding="utf-8")

    def _finalize(self, run_id: int, best_iteration: int) -> Optional[Path]:
        """Copy the best version back to the canonical collector file."""
        best_path = self._version_path(run_id, best_iteration)
        if not best_path.exists():
            return None
        shutil.copy2(best_path, self.collector_file)
        print(f"  Best version (iteration {best_iteration}) written to {self.collector_file}")
        return self.collector_file

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _update_run(
        self, run_id: int, best_score: float, best_iteration: int, total_cost: float
    ) -> None:
        con = get_connection(self.db_path)
        try:
            con.execute(
                "UPDATE training_runs SET best_score=?, best_iteration=?, "
                "total_cost_usd=? WHERE run_id=?",
                (best_score, best_iteration, total_cost, run_id),
            )
            con.commit()
        finally:
            con.close()

    def _filter_large_datasets(
        self, examples: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Skip large datasets on non-full-fidelity iterations."""
        threshold = self.config.large_dataset_threshold_bytes
        return [
            ex for ex in examples
            if (ex.get("data_size_bytes") or 0) <= threshold
        ]

    def _get_interface_spec(self) -> str:
        if self._interface_spec is None:
            try:
                # Import from the MCP server helper (avoids duplication)
                sys_path_backup = __import__("sys").path[:]
                __import__("sys").path.insert(0, str(PROJECT_ROOT))
                from mcp_collector_dev.server import get_collector_interface
                self._interface_spec = get_collector_interface()
            except Exception:
                self._interface_spec = "(interface spec unavailable)"
        return self._interface_spec


# ── Rollback helper ─────────────────────────────────────────────────────────

def rollback_to_best(run_id: int, db_path: Optional[Path] = None) -> Optional[int]:
    """
    Find and return the iteration_num with the highest aggregate_score for a run.
    Also copies that version back to the canonical collector file.
    """
    db_path = db_path or DEFAULT_DB_PATH
    con = get_connection(db_path)
    try:
        row = con.execute(
            "SELECT iteration_num, aggregate_score, collector_code FROM iterations "
            "WHERE run_id=? AND aggregate_score IS NOT NULL "
            "ORDER BY aggregate_score DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        if not row:
            return None

        # Find collector_name for this run
        run_row = con.execute(
            "SELECT collector_name FROM training_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if not run_row:
            return None

        collector_file = COLLECTORS_DIR / f"{run_row['collector_name']}.py"
        collector_file.write_text(row["collector_code"], encoding="utf-8")
        print(f"Rolled back {collector_file.name} to "
              f"iteration {row['iteration_num']} (score={row['aggregate_score']:.3f})")
        return row["iteration_num"]
    finally:
        con.close()
