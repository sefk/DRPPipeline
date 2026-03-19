"""
DSPy integration for the collector training system.

Phase 1: Typed signatures without optimization (replaces hand-written
         prompt templates with versionable, testable code).
Phase 2: Enable BootstrapFewShot optimization using training data.
Phase 3: Use MIPROv2 and multi-stage pipelines.

See: https://dspy.ai/

Install: pip install dspy-ai
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).parent.parent


def _import_dspy():
    """Import dspy with a helpful error if not installed."""
    try:
        import dspy
        return dspy
    except ImportError:
        raise ImportError(
            "dspy-ai package required for DSPy integration. "
            "Install with: pip install dspy-ai"
        )


# ── DSPy Signatures ─────────────────────────────────────────────────────────

def _make_signatures(dspy):
    """Define DSPy signatures (called lazily so dspy import is optional)."""

    class AnalyzeCollectorGaps(dspy.Signature):
        """
        Analyze the gaps between collector output and human-verified ground truth.
        Identify which extraction patterns are failing and why.
        """
        collector_code: str = dspy.InputField(
            desc="Current Python source of the collector class"
        )
        score_report: str = dspy.InputField(
            desc="Aggregate score and per-field breakdown (field: score)"
        )
        worst_cases: str = dspy.InputField(
            desc="3–5 worst-scoring projects with field-by-field diffs "
                 "(expected vs actual values)"
        )
        analysis: str = dspy.OutputField(
            desc="Structured analysis: which fields fail, why, and what "
                 "code changes would fix them. Be specific about CSS selectors, "
                 "API endpoints, or parsing logic."
        )

    class RefineCollector(dspy.Signature):
        """
        Produce an improved collector based on gap analysis.
        Return the COMPLETE updated Python file — nothing else.
        """
        collector_code: str = dspy.InputField(
            desc="Current Python source of the collector class"
        )
        analysis: str = dspy.InputField(
            desc="Structured analysis of what extraction patterns are failing"
        )
        interface_spec: str = dspy.InputField(
            desc="Collector interface specification (Storage schema, utilities, etc.)"
        )
        improved_code: str = dspy.OutputField(
            desc="Complete updated collector Python file. No ``` markers, no prose."
        )

    return AnalyzeCollectorGaps, RefineCollector


# ── DSPy Module ─────────────────────────────────────────────────────────────

class CollectorTrainer:
    """
    Two-stage DSPy module: gap analysis → code refinement.

    Uses ChainOfThought for both stages, enabling automatic prompt
    optimization via BootstrapFewShot or MIPROv2.
    """

    def __init__(self, model_config: Optional[dict[str, str]] = None) -> None:
        """
        Args:
            model_config: Optional dict with 'analyze' and 'refine' model IDs.
                          Defaults to claude-sonnet-4-6 for both.
        """
        dspy = _import_dspy()
        model_config = model_config or {}

        AnalyzeCollectorGaps, RefineCollector = _make_signatures(dspy)

        analyze_model = model_config.get("analyze", "claude-sonnet-4-6")
        refine_model  = model_config.get("refine",  "claude-sonnet-4-6")

        self._analyze_lm = dspy.LM(f"anthropic/{analyze_model}")
        self._refine_lm  = dspy.LM(f"anthropic/{refine_model}")

        # ChainOfThought enables intermediate reasoning before output
        with dspy.context(lm=self._analyze_lm):
            self.analyze = dspy.ChainOfThought(AnalyzeCollectorGaps)
        with dspy.context(lm=self._refine_lm):
            self.refine_module = dspy.ChainOfThought(RefineCollector)

    def forward(
        self,
        collector_code: str,
        score_report: str,
        worst_cases: str,
        interface_spec: str,
    ) -> tuple[str, str]:
        """
        Run the two-stage pipeline.

        Returns (improved_code, analysis).
        """
        dspy = _import_dspy()

        with dspy.context(lm=self._analyze_lm):
            analysis_result = self.analyze(
                collector_code=collector_code,
                score_report=score_report,
                worst_cases=worst_cases,
            )

        with dspy.context(lm=self._refine_lm):
            refinement_result = self.refine_module(
                collector_code=collector_code,
                analysis=analysis_result.analysis,
                interface_spec=interface_spec,
            )

        return refinement_result.improved_code, analysis_result.analysis

    def __call__(self, *args, **kwargs):
        return self.forward(*args, **kwargs)


# ── DSPy Refiner (Phase 3 replacement for SimpleRefiner) ───────────────────

class DSPyRefiner:
    """
    Phase 3 refiner: uses CollectorTrainer DSPy module instead of raw API calls.

    Drop-in replacement for trainer.SimpleRefiner.
    Supports optional compilation (prompt optimization) via BootstrapFewShot.
    """

    def __init__(
        self,
        model_config: Optional[dict[str, str]] = None,
        compiled_path: Optional[Path] = None,
    ) -> None:
        self.model_config = model_config or {}
        self.model = model_config.get("refine", "claude-sonnet-4-6") if model_config else "claude-sonnet-4-6"
        self._trainer: Optional[CollectorTrainer] = None
        self._compiled_path = compiled_path

    def _get_trainer(self) -> CollectorTrainer:
        if self._trainer is None:
            self._trainer = CollectorTrainer(self.model_config)
            if self._compiled_path and self._compiled_path.exists():
                self._trainer = self._load_compiled(self._trainer)
        return self._trainer

    def _load_compiled(self, trainer: CollectorTrainer) -> CollectorTrainer:
        """Load a compiled (optimized) trainer from disk."""
        dspy = _import_dspy()
        trainer.load(str(self._compiled_path))
        return trainer

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
        Refine collector using the DSPy two-stage pipeline.
        Returns (improved_code, token_usage_dict).
        """
        import json as _json

        score_report = (
            f"Aggregate score: {aggregate_score:.3f}\n\nPer-field scores:\n"
            + "\n".join(f"  {f}: {s:.3f}" for f, s in sorted(field_breakdown.items(), key=lambda x: x[1]))
        )
        if refinement_strategy:
            score_report += f"\n\nFocus: {refinement_strategy}"

        worst_text = _json.dumps(
            [
                {
                    "source_url": c.get("source_url"),
                    "score":      c.get("score"),
                    "diff":       {
                        f: d for f, d in (c.get("diff") or {}).items()
                        if d.get("expected") != d.get("actual")
                    },
                }
                for c in worst_cases[:5]
            ],
            indent=2,
        )

        trainer = self._get_trainer()
        improved_code, _ = trainer(
            collector_code=collector_code,
            score_report=score_report,
            worst_cases=worst_text,
            interface_spec=interface_spec,
        )

        # Token usage not directly available from DSPy; estimate
        token_usage = {
            "input_tokens":  0,
            "output_tokens": 0,
            "cost_usd":      0.0,
        }
        return improved_code, token_usage


# ── Phase 2 optimizer ───────────────────────────────────────────────────────

def training_metric(example: Any, prediction: Any) -> float:
    """
    DSPy metric for optimizer: score the refined collector against the training set.

    Used by BootstrapFewShot / MIPROv2 to optimize prompts.
    The example must have 'training_examples' and 'evaluator' fields.
    """
    try:
        improved_code = prediction.improved_code
        evaluator = example.evaluator
        training_examples = example.training_examples

        results = evaluator.evaluate_all(
            training_examples, improved_code, num_workers=1
        )
        if not results:
            return 0.0
        return sum(r["score"] for r in results) / len(results)
    except Exception:
        return 0.0


def optimize_trainer(
    trainer: CollectorTrainer,
    training_examples_dspy: list[Any],
    num_threads: int = 4,
) -> CollectorTrainer:
    """
    Phase 2: Run BootstrapFewShot optimization.

    training_examples_dspy: list of dspy.Example objects with fields:
        collector_code, score_report, worst_cases, interface_spec,
        evaluator, training_examples (for metric)
    """
    dspy = _import_dspy()
    optimizer = dspy.BootstrapFewShot(
        metric=training_metric,
        max_bootstrapped_demos=3,
        max_labeled_demos=2,
    )
    compiled = optimizer.compile(trainer, trainset=training_examples_dspy)
    return compiled


def build_trainer(
    model_config: Optional[dict[str, str]] = None,
    compiled_path: Optional[Path] = None,
) -> DSPyRefiner:
    """Factory: returns a DSPyRefiner (Phase 3 drop-in for SimpleRefiner)."""
    return DSPyRefiner(model_config=model_config, compiled_path=compiled_path)
