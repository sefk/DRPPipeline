# Collector Training System — Design

An automated system for iteratively improving collectors using human-verified
training data and AI-driven refinement.

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [Workflow Overview](#workflow-overview)
- [Architecture](#architecture)
- [Training Data](#training-data)
- [Scoring System](#scoring-system)
- [Iteration Loop](#iteration-loop)
- [DSPy Integration](#dspy-integration)
- [Multi-Model Support](#multi-model-support)
- [Cost Management](#cost-management)
- [Database Schema](#database-schema)
- [MCP Extensions](#mcp-extensions)
- [Parallel Agents](#parallel-agents)
- [Observer Console](#observer-console)
- [Risks and Future Work](#risks-and-future-work)

---

## Problem Statement

Building a collector requires figuring out where key information lives on a
website — which API endpoints return metadata, which CSS selectors hold the
description, where download links are buried. Today this is done by a developer
manually inspecting the site, writing extraction logic, and testing against a
handful of projects. There is no systematic way to measure quality or iterate
toward better results.

We have a wealth of human-verified training data: projects that volunteers have
already collected manually. This data can drive an automated loop that builds a
collector, scores it against the training set, identifies gaps, and refines the
extraction logic — repeating until quality plateaus.

---

## Workflow Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  1. ENUMERATE        Find all datasets on a source site         │
│                      (manual or automated; stored in Data       │
│                      Inventory spreadsheet)                     │
├─────────────────────────────────────────────────────────────────┤
│  2. HUMAN PASS       Volunteers manually collect a subset       │
│                      (the "DONE" rows in the spreadsheet).      │
│                      This becomes training data.                │
├─────────────────────────────────────────────────────────────────┤
│  3. BOOTSTRAP        Build initial collector using existing     │
│                      drp-collector-dev tools                    │
├─────────────────────────────────────────────────────────────────┤
│  4. EVALUATE         Run collector on training set, score       │
│                      each project, compute aggregate score      │
├─────────────────────────────────────────────────────────────────┤
│  5. REFINE           Feed score + diffs to AI, produce          │
│                      updated collector code                     │
├─────────────────────────────────────────────────────────────────┤
│  6. ITERATE          Repeat 4-5 until score plateaus            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Architecture

```
                    ┌──────────────┐
                    │  Coordinator │
                    │  (Python)    │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              v            v            v
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Agent 1  │ │ Agent 2  │ │ Agent N  │
        │ (refine) │ │ (refine) │ │ (refine) │
        └────┬─────┘ └────┬─────┘ └────┬─────┘
             │             │            │
             v             v            v
        ┌─────────────────────────┐    ┌──────────────────────────┐
        │    Training Database    ├───>│    Observer Console      │
        │  (iterations, scores,   │    │    (read-only live view) │
        │   diffs, cost)          │    │                          │
        └────────────┬────────────┘    │  score trend · agents    │
                     │                 │  cost · field breakdown   │
                     v                 └──────────────────────────┘
        ┌─────────────────────────┐
        │   Training Data Store   │
        │  (human-verified ground │
        │   truth)                │
        └─────────────────────────┘
```

### Design principle: one collector per source

Each data source (CMS, CDC, catalog.data.gov, etc.) gets its own collector.
Source sites have fundamentally different structure, APIs, and metadata
conventions — a universal collector is not practical. The training system
trains one collector at a time against training data from that source.

### Components

**Coordinator** — A Python process that orchestrates the training loop:
- Loads training data and initializes the first collector version
- Dispatches evaluation and refinement work to agents
- Tracks cost, iteration count, and score trajectory
- Decides when to stop (score plateau, cost limit, max iterations)
- Controls parallelism (how many agents run concurrently)

**Agents** — Workers that perform one evaluate-refine cycle:
- Run the current collector against a set of training projects
- Compute per-project and aggregate scores
- Generate a refinement prompt with specific diffs
- Call an AI model to produce updated collector code
- Write results to the training database

**Training Database** — SQLite database tracking all iterations:
- Every collector version, its scores, and what changed
- Token usage and cost per iteration
- Enables visibility into progress and debugging

**Training Data Store** — The ground truth from human collection:
- Imported from the Data Inventory spreadsheet
- Stored locally so evaluation doesn't depend on Google Sheets availability
- Includes all metadata fields and file manifests

**Observer Console** — A live read-only view of training progress:
- Reads directly from the training database; does not interact with agents
- Shows score trend, active agent count, token cost, and per-field breakdown
- Designed for a human to watch a training run without interrupting it
- Future: ability to send guidance or interrupt the run (see Risks and Future
  Work)

---

## Training Data

### Source

This system assumes that two prerequisite steps have already been completed
for a given data source:

1. **Cataloging**: Someone has enumerated all the datasets on the source site
   and recorded them in the [DRP Data Inventory][inventory] spreadsheet (one
   tab per source). For CMS, this is the [CMS - Done][cms-done] tab.
2. **Human first pass**: Volunteers have manually collected a subset of those
   datasets — the rows marked "DONE" in the spreadsheet. Each of these
   represents a project that was carried through the full pipeline by hand,
   producing a DataLumos project we can use as ground truth.

Both of these steps are currently manual. There is an opportunity for future
work to automate them as well — for example, using AI to crawl a source site
and enumerate its datasets, or to triage which datasets are highest priority
for human review. Automating the catalog step would make it feasible to bring
up training for a new data source much faster, without waiting for a volunteer
to manually inventory the site.

[inventory]: https://docs.google.com/spreadsheets/d/1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY/edit
[cms-done]: https://docs.google.com/spreadsheets/d/1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY/edit?gid=864890349#gid=864890349

### What training data contains

For each human-verified project:

| Field | Source | Notes |
|-------|--------|-------|
| `source_url` | Spreadsheet "URL" column | The page the collector must handle |
| `title` | DataLumos project | Extracted from the uploaded project |
| `agency` | DataLumos project | May include sub-offices |
| `summary` | DataLumos project | Description text |
| `keywords` | DataLumos project | Comma-separated tags |
| `time_start`, `time_end` | DataLumos project | Temporal coverage |
| `data_types` | DataLumos project | e.g. "tabular", "geospatial" |
| `files` | DataLumos project | Filenames and sizes |
| `collection_notes` | DataLumos project | Free-form notes |
| `geographic_coverage` | DataLumos project | Location/jurisdiction |

### Import process

1. Fetch the spreadsheet tab as CSV (same approach as `e2e_test.py`)
2. Filter to "DONE" rows
3. For each row, fetch the control DataLumos project (from "Download Location"
   column) using the same scraping logic as `compare_datalumos.py`
4. Store the extracted fields in the training database

This import is a one-time operation per data source, refreshed when new
human-verified rows appear. It should be idempotent — re-running updates
existing records rather than creating duplicates.

### Multiple annotators

Since no single human is authoritative, different volunteers may have made
slightly different choices (e.g., one might include a subtitle in the title,
another might not). When multiple humans have collected the same dataset:

- Store all versions as separate training examples
- During scoring, use the **best match** across annotators (the version
  closest to what the collector produced)
- This prevents penalizing a collector for a reasonable alternative that
  some annotator happened to choose

In practice, most datasets will have a single annotator. The system should
handle both cases.

### Large dataset handling

Every dataset has both metadata and data files. Downloading files on every
iteration is slow and bandwidth-heavy, but file extraction is a key thing
to get right. Training examples are classified by data size at import time:

- **Small datasets** (under a configurable threshold, e.g. 100 MB): Include
  in every training run. The download cost is acceptable and we get full
  fidelity scoring on file extraction.
- **Large datasets**: Treat as exceptional — include only in periodic
  full-fidelity runs (e.g., every 5th iteration or on-demand). For normal
  iterations, score these projects on metadata only and flag the partial
  coverage in the score report.

This keeps the fast iteration loop fast while still catching file extraction
regressions before they ship.

### Validation holdback

A portion of training projects is held out as a validation set that the
refinement loop never sees. The collector is scored against the training set
during iteration, but the validation set is evaluated at the end (and
optionally at milestones) to detect overfitting — a collector that memorizes
patterns specific to the training projects rather than learning
generalizable extraction logic.

Default split: 80% training / 20% validation, assigned randomly at import
time. For small training sets (under 10 projects), reduce the holdback to
1-2 projects rather than skipping it entirely — even a single validation
project provides a useful sanity check.

The validation holdback also serves as a correctness check: a collector that
crashes on unseen URLs is brittle and should score poorly (see "Failure
modes" in Scoring System).

---

## Scoring System

### Design goals

The existing `compare_datalumos.py` uses a three-tier system (GREEN / YELLOW /
RED). For training, we need a continuous score that can serve as a health
function for optimization. The score should:

- Range from 0.0 (complete failure) to 1.0 (perfect match)
- Be decomposable into per-field scores for diagnostic purposes
- Handle partial matches gracefully (e.g., 3 of 5 keywords present)
- Be comparable across iterations to measure progress

### Per-field scoring

Each metadata field is scored independently on [0.0, 1.0]:

| Field | Scoring method |
|-------|---------------|
| `title` | Normalized string similarity (case-insensitive, whitespace-normalized). Use token-level Jaccard + exact substring bonus. |
| `summary` | Sentence-level semantic overlap. Start with token Jaccard; consider embedding similarity later if needed. |
| `agency` | Set comparison (split on delimiters). Each correct org = partial credit. |
| `keywords` | Set comparison with fuzzy matching (stemming or token overlap). Precision and recall both matter. |
| `time_start`, `time_end` | Date proximity. Exact match = 1.0, within same year = 0.8, within 2 years = 0.5, else 0.0. |
| `data_types` | Set comparison. |
| `files` | Filename set comparison (normalized). Also compare total file count — missing files is worse than extra files. |
| `collection_notes` | Token Jaccard, ignoring dates (strip "Downloaded YYYY-MM-DD" patterns). |
| `geographic_coverage` | Normalized string comparison. |

### Aggregate score

Weighted average of per-field scores:

```
score = Σ (weight_i × field_score_i) / Σ weight_i
```

Suggested initial weights (tunable):

| Field | Weight | Rationale |
|-------|--------|-----------|
| `files` | 3.0 | Most important — did we get the data? |
| `title` | 2.0 | Core identity of the dataset |
| `summary` | 2.0 | Key for discoverability |
| `agency` | 1.5 | Important but often straightforward |
| `keywords` | 1.0 | Useful but subjective |
| `time_start` | 1.0 | |
| `time_end` | 1.0 | |
| `data_types` | 0.5 | Usually inferrable from extensions |
| `collection_notes` | 0.5 | Nice to have |
| `geographic_coverage` | 0.5 | Often not applicable |

### Failure modes

Correctness is paramount — a collector that crashes is far worse than one
that extracts incomplete metadata. Some outcomes floor the score regardless
of other fields:

- **Collector crashes** (import error, syntax error, unhandled exception,
  `status=error`) → score = 0.0
- **No files downloaded** (empty `folder_path`) → score capped at 0.2
- **Wrong files downloaded** (no filename overlap with training data) → score
  capped at 0.3

These penalties apply on both training and validation projects. The
validation holdback (see Training Data) ensures that correctness is also
checked against URLs the refinement loop has never seen.

### Per-project vs. aggregate

- **Per-project score**: The weighted average for one project against one
  training example (or best of multiple annotators)
- **Aggregate score**: Mean of per-project scores across all training projects.
  This is the health function we optimize.

---

## Iteration Loop

### One iteration

```python
def iterate(collector_version, training_data, model):
    # 1. Evaluate
    scores = []
    diffs = []
    for project in training_data:
        result = run_collector(collector_version, project.source_url)
        score, diff = evaluate(result, project.ground_truth)
        scores.append(score)
        diffs.append(diff)

    aggregate_score = mean(scores)

    # 2. Analyze failures
    worst = sorted(zip(scores, diffs), key=lambda x: x[0])[:5]

    # 3. Refine
    refinement_prompt = build_refinement_prompt(
        collector_code=collector_version.code,
        aggregate_score=aggregate_score,
        worst_cases=worst,
        field_breakdown=per_field_averages(scores),
    )
    new_code = model.generate(refinement_prompt)

    # 4. Record
    new_version = save_version(new_code, aggregate_score, diffs)
    return new_version, aggregate_score
```

### Stopping conditions

The coordinator stops iterating when any of these hold:

1. **Score plateau**: Aggregate score has not improved by more than 0.01 over
   the last 3 iterations
2. **Cost limit**: Cumulative token cost exceeds a configured budget
3. **Max iterations**: Hard cap (default: 20)
4. **Perfect score**: Aggregate score >= 0.95

### Refinement prompt structure

The refinement prompt given to the AI includes:

1. The current collector source code
2. The aggregate score and per-field breakdown
3. The 3-5 worst-scoring projects with:
   - Source URL
   - Expected values (from training data)
   - Actual values (from collector output)
   - Per-field diffs showing exactly what was wrong
4. The collector interface spec (from `get_collector_interface()`)
5. Instructions: "Modify the collector code to improve extraction. Focus on
   the worst-performing fields. Return the complete updated collector file."

### Version management

Each iteration produces a new collector file. Rather than overwriting:

- Store versions as `collectors/{CollectorName}_v{N}.py`
- The training database tracks which version produced which scores
- The coordinator can roll back to the best-scoring version if a refinement
  makes things worse
- At the end, the best version is copied to the canonical filename

---

## DSPy Integration

[DSPy](https://dspy.ai/) is a framework for programming (not prompting) LLMs.
Instead of hand-tuning prompts, you define typed signatures and let DSPy
optimize the instructions and few-shot examples.

### Why DSPy

1. **Prompt fragility**: Hand-written refinement prompts are brittle. A small
   change in wording can cause large regressions.
2. **Automatic optimization**: DSPy can tune prompts using the training data
   and scoring function we already have.
3. **Reproducibility**: DSPy programs are code, not prose. They're testable,
   versionable, and composable.
4. **Model portability**: A DSPy program can be compiled for different models
   without rewriting prompts.

### Where DSPy fits

DSPy replaces the "refinement prompt" step of the iteration loop. Instead of
a hand-written prompt template, we define DSPy modules:

```python
import dspy

class AnalyzeCollectorGaps(dspy.Signature):
    """Analyze the gaps between collector output and ground truth."""
    collector_code: str = dspy.InputField()
    score_report: str = dspy.InputField()
    worst_cases: str = dspy.InputField()
    analysis: str = dspy.OutputField(desc="structured analysis of what "
                                          "extraction patterns are failing")

class RefineCollector(dspy.Signature):
    """Produce an improved collector based on gap analysis."""
    collector_code: str = dspy.InputField()
    analysis: str = dspy.InputField()
    interface_spec: str = dspy.InputField()
    improved_code: str = dspy.OutputField(desc="complete updated collector "
                                               "Python file")

class CollectorTrainer(dspy.Module):
    def __init__(self):
        self.analyze = dspy.ChainOfThought(AnalyzeCollectorGaps)
        self.refine = dspy.ChainOfThought(RefineCollector)

    def forward(self, collector_code, score_report, worst_cases, interface_spec):
        analysis = self.analyze(
            collector_code=collector_code,
            score_report=score_report,
            worst_cases=worst_cases,
        )
        result = self.refine(
            collector_code=collector_code,
            analysis=analysis.analysis,
            interface_spec=interface_spec,
        )
        return result
```

### DSPy optimization

DSPy's optimizers (e.g., `BootstrapFewShot`, `MIPROv2`) can use our training
data and scoring function as the metric:

```python
def training_metric(example, prediction):
    """Score a refinement attempt using the collector scoring system."""
    # Write prediction.improved_code to a temp file
    # Run it against the training set
    # Return the aggregate score
    return aggregate_score

optimizer = dspy.MIPROv2(metric=training_metric, num_threads=4)
compiled_trainer = optimizer.compile(
    CollectorTrainer(),
    trainset=training_examples,
)
```

This means DSPy can learn:
- What kinds of analysis lead to good refinements
- What few-shot examples help the model produce better code
- What instruction phrasing works best for each model

### Phased adoption

1. **Phase 1**: Use DSPy signatures without optimization — just the structured
   input/output definitions. This replaces hand-written prompt templates with
   typed, versionable code.
2. **Phase 2**: Enable DSPy optimization (BootstrapFewShot) using our training
   data. This auto-tunes the prompts.
3. **Phase 3**: Use advanced optimizers (MIPROv2) and multi-stage pipelines
   for more sophisticated refinement strategies.

---

## Multi-Model Support

### Motivation

- **Cost**: Claude Opus/Sonnet are capable but expensive for iteration-heavy
  workloads. Gemini 2.5 Flash or Claude Haiku may suffice for many steps.
- **Budget**: Relying solely on Claude risks exhausting a personal token budget
  during extended training runs.
- **Diversity**: Different models may find different improvements. Running
  parallel agents on different models and keeping the best result is a valid
  strategy.

### Model roles

Not every step in the loop needs the most capable model:

| Step | Model tier | Rationale |
|------|-----------|-----------|
| Initial collector bootstrap | High (Opus/Sonnet) | Needs deep reasoning about site structure |
| Evaluation (running collector) | None (pure Python) | No LLM needed |
| Scoring | None (pure Python) | No LLM needed |
| Gap analysis | Medium (Sonnet/Flash) | Pattern recognition, not code generation |
| Code refinement | High (Opus/Sonnet) | Needs to write correct Python |
| Simple field extraction fixes | Low (Haiku/Flash) | Small targeted edits |

### Implementation

The coordinator configures a model roster:

```python
MODEL_CONFIG = {
    "bootstrap": {"provider": "anthropic", "model": "claude-sonnet-4-6"},
    "analyze":   {"provider": "google",    "model": "gemini-2.5-flash"},
    "refine":    {"provider": "anthropic", "model": "claude-sonnet-4-6"},
}
```

DSPy natively supports multiple LM backends via `dspy.LM`:

```python
analyzer = dspy.LM("google/gemini-2.5-flash")
refiner = dspy.LM("anthropic/claude-sonnet-4-6")
```

### Adding a new model

To add a model provider:
1. Add the provider's API key to config
2. Register it in `MODEL_CONFIG`
3. DSPy handles the rest (prompt formatting, API calls, retries)

---

## Cost Management

### Token tracking

Every LLM call records:
- Model used
- Input tokens
- Output tokens
- Cost (computed from model pricing)
- Which iteration and step it belongs to

### Budget controls

```python
COST_CONFIG = {
    "max_total_cost_usd": 10.00,       # Hard stop for entire training run
    "max_iteration_cost_usd": 2.00,     # Per-iteration limit
    "warn_at_pct": 0.75,                # Log warning at 75% of budget
    "prefer_cheap_after_pct": 0.50,     # Switch to cheaper models after 50%
}
```

### Cost-aware parallelism

The coordinator adjusts the number of parallel agents based on remaining budget:

- **Ample budget**: Run N agents in parallel, each trying a different
  refinement strategy or model
- **Budget pressure**: Reduce to 1 agent, switch to cheapest viable model
- **Budget exhausted**: Stop, report best result so far

### Cost estimation

Before starting a training run, estimate costs:

```
Per iteration (approximate):
  - Gap analysis:   ~2K input + 1K output tokens
  - Code refinement: ~5K input + 3K output tokens
  - Total: ~11K tokens/iteration

At Gemini 2.5 Flash pricing (~$0.15/M input, ~$0.60/M output):
  ~$0.002/iteration → ~$0.04 for 20 iterations

At Claude Sonnet pricing (~$3/M input, ~$15/M output):
  ~$0.06/iteration → ~$1.20 for 20 iterations
```

The dramatic cost difference makes Flash attractive for most iteration steps.

---

## Database Schema

A new SQLite database for training state (`collector_training.db`), separate
from the pipeline database.

### Tables

```sql
-- Training runs (one per collector being trained)
CREATE TABLE training_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    collector_name TEXT NOT NULL,          -- e.g. "cms_collector"
    source_site TEXT NOT NULL,             -- e.g. "data.cms.gov"
    started_at TEXT NOT NULL,              -- ISO timestamp
    finished_at TEXT,
    status TEXT DEFAULT 'running',         -- running, completed, stopped, failed
    best_score REAL,
    best_iteration INTEGER,
    total_cost_usd REAL DEFAULT 0.0,
    config_json TEXT,                      -- serialized run configuration
    notes TEXT
);

-- Training data (human-verified ground truth)
CREATE TABLE training_examples (
    example_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES training_runs(run_id),
    source_url TEXT NOT NULL,
    annotator TEXT,                        -- who did the manual collection
    control_datalumos_id TEXT,             -- DataLumos workspace ID
    ground_truth_json TEXT NOT NULL,       -- all extracted fields as JSON
    is_validation INTEGER DEFAULT 0,       -- 1 = holdback, 0 = training
    data_size_bytes INTEGER,               -- for large-dataset handling
    imported_at TEXT NOT NULL,
    UNIQUE(run_id, source_url, annotator)
);

-- Iterations within a training run
CREATE TABLE iterations (
    iteration_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES training_runs(run_id),
    iteration_num INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    collector_code TEXT NOT NULL,          -- full Python source
    aggregate_score REAL,
    per_field_scores_json TEXT,            -- {"title": 0.9, "files": 0.7, ...}
    refinement_strategy TEXT,             -- description of what was tried
    model_used TEXT,                       -- which model did the refinement
    parent_iteration INTEGER,             -- which iteration this branched from
    UNIQUE(run_id, iteration_num)
);

-- Per-project scores within an iteration
CREATE TABLE project_scores (
    score_id INTEGER PRIMARY KEY AUTOINCREMENT,
    iteration_id INTEGER NOT NULL REFERENCES iterations(iteration_id),
    example_id INTEGER NOT NULL REFERENCES training_examples(example_id),
    score REAL NOT NULL,
    per_field_json TEXT,                   -- {"title": 0.95, "files": 0.6, ...}
    diff_json TEXT,                        -- field-by-field expected vs actual
    collector_output_json TEXT,            -- what the collector actually produced
    error_message TEXT                     -- if collector crashed
);

-- Token usage tracking
CREATE TABLE token_usage (
    usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
    iteration_id INTEGER REFERENCES iterations(iteration_id),
    run_id INTEGER NOT NULL REFERENCES training_runs(run_id),
    step TEXT NOT NULL,                    -- 'analyze', 'refine', 'bootstrap'
    model TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    cost_usd REAL NOT NULL,
    timestamp TEXT NOT NULL
);

CREATE INDEX idx_iterations_run ON iterations(run_id);
CREATE INDEX idx_scores_iteration ON project_scores(iteration_id);
CREATE INDEX idx_usage_run ON token_usage(run_id);
```

### Visibility

The database supports queries like:

- "Show me the score trajectory for this training run"
- "Which fields improved/regressed between iterations 3 and 4?"
- "What's the total cost so far?"
- "Which training projects are consistently low-scoring?" (hard cases)
- "Compare scores across two parallel agent branches"

---

## MCP Extensions

The existing `drp-collector-dev` MCP server needs new tools to support the
training workflow. These should be added to `mcp_collector_dev/server.py`.

### New tools

| Tool | Description |
|------|-------------|
| `import_training_data` | Fetch DONE rows from spreadsheet, scrape control DataLumos projects, store as training examples. Params: `sheet_name`, `run_id`, `max_rows`. |
| `evaluate_collector` | Run a collector version against all training examples and compute scores. Params: `run_id`, `iteration_num`. Returns aggregate score and worst cases. |
| `get_training_status` | Show current state of a training run: iteration count, best score, score trajectory, cost spent, running agents. |
| `get_iteration_details` | Show per-project scores, diffs, and field breakdown for a specific iteration. |
| `start_training_run` | Initialize a new training run with configuration (model, budget, parallelism). Returns `run_id`. |
| `stop_training_run` | Gracefully stop a running training session. Agents finish current iteration, then stop. |

### Extended existing tools

| Tool | Change |
|------|--------|
| `test_collector_on_project` | Add optional `return_raw=True` param that returns structured output (dict of extracted fields) instead of human-readable text. Needed for scoring. |
| `scaffold_collector` | Add `from_version` param to scaffold from an existing iteration's code instead of the blank template. |

---

## Parallel Agents

### Strategy

Parallelism operates at two levels:

1. **Within an iteration**: Evaluating the collector against N training
   projects is embarrassingly parallel. Each project is independent.
2. **Across refinement strategies**: Multiple agents can try different
   refinement approaches from the same parent iteration and compare results.

### Within-iteration parallelism

Use Python's `ThreadPoolExecutor` (matching the existing Orchestrator pattern):

```python
with ThreadPoolExecutor(max_workers=num_workers) as pool:
    futures = {
        pool.submit(evaluate_one, project): project
        for project in training_projects
    }
    for future in as_completed(futures):
        score, diff = future.result()
        scores.append((futures[future], score, diff))
```

### Cross-strategy parallelism

The coordinator can spawn multiple refinement agents that each take a
different approach:

- **Agent A**: Focus on the worst-scoring field (e.g., "improve file
  extraction")
- **Agent B**: Focus on the worst-scoring project (e.g., "handle this edge
  case")
- **Agent C**: Use a different model (e.g., Gemini Flash vs. Claude Sonnet)

Each agent produces a new collector version. The coordinator evaluates all
of them and keeps the best one as the parent for the next round.

```
Iteration 3 (score: 0.72)
    ├── Agent A: focus on files    → v4a (score: 0.76) ← best, becomes v4
    ├── Agent B: focus on project  → v4b (score: 0.73)
    └── Agent C: use Flash         → v4c (score: 0.71)
Iteration 4 (score: 0.76, from v4a)
    └── ...
```

The `parent_iteration` field in the `iterations` table tracks this branching.

### Claude Code agents

When running within Claude Code, the coordinator can use Claude Code's agent
system to dispatch parallel work:

- Each refinement agent gets its own worktree (via `isolation: "worktree"`)
  to avoid conflicts when writing collector files
- The coordinator agent reads results from the training database
- This maps naturally to the existing Agent tool with `run_in_background`

---

## Implementation Plan

### Phase 1: Foundation

1. Create `collector_training/` directory with:
   - `schema.py` — Database schema and initialization
   - `scorer.py` — Per-field and aggregate scoring functions
   - `importer.py` — Training data import from spreadsheet + DataLumos
2. Port scoring logic from `tests/compare_datalumos.py` into `scorer.py`,
   converting from GREEN/YELLOW/RED to continuous [0.0, 1.0] scores
3. Write unit tests for scoring functions

### Phase 2: Iteration loop

1. Build `trainer.py` — Single-threaded iteration loop:
   - Evaluate current collector → score → analyze gaps → refine → repeat
2. Add `coordinator.py` — Manages stopping conditions, version tracking
3. Integrate with existing `test_collector_on_project` for evaluation
4. Test end-to-end on CMS collector with a small training set (5 projects)

### Phase 3: DSPy integration

1. Define DSPy signatures for analysis and refinement
2. Replace hand-written prompt templates
3. Run DSPy optimization (BootstrapFewShot) on training data
4. Compare optimized vs. unoptimized performance

### Phase 4: Multi-model and cost management

1. Add model configuration and token tracking
2. Implement cost-aware parallelism in coordinator
3. Test with Gemini Flash as the cheap-model option
4. Add MCP tools for monitoring training runs

### Phase 4b: Observer console

1. Build `observer.py` — console interface that tails the training database
2. Auto-refreshes every few seconds while a training run is active
3. Exits cleanly when the run completes

### Phase 5: Parallel agents

1. Add cross-strategy parallelism to coordinator
2. Test with multiple agents on different refinement strategies
3. Tune the coordinator's agent dispatch logic

---

## Observer Console

A training run can take many iterations and several minutes to hours. The
observer console gives a human a live view of what is happening without
requiring them to read log files or query the database directly.

### Interface choice

A **console (terminal) interface** is preferred for v1:

- No server to start, no browser to open — just `python observer.py <run_id>`
- Runs alongside the training process in a second terminal pane
- Works over SSH for remote runs
- Easy to implement with Python's `curses` or simple timed-refresh printing

A web interface remains a future option if richer visualization is needed
(e.g., score charts, diff highlighting), but adds infrastructure complexity
that isn't warranted for v1.

### What the console shows

The display refreshes every few seconds, reading directly from the training
database. Layout (example):

```
DRP Collector Training — cms_collector  [run_id=7]
Started: 2026-03-18 14:02  Elapsed: 12m  Status: RUNNING

Score trend
  It 1   ████████░░░░░░░░░░░░  0.41
  It 2   ██████████████░░░░░░  0.68
  It 3   ███████████████░░░░░  0.73  ← current

Per-field (iteration 3)
  files          0.61  ████████████░░░░░░░░
  title          0.89  █████████████████░░░
  summary        0.75  ███████████████░░░░░
  agency         0.92  ██████████████████░░
  keywords       0.70  ██████████████░░░░░░

Active agents: 2 of 3
  Agent A  refining → focus: files
  Agent B  evaluating (11/14 projects)
  Agent C  idle

Cost  $0.43 of $10.00 budget (4%)
  Training set:  14 projects (2 large, deferred)
  Validation:     4 projects (held out)
  Best score:    0.73 (iteration 3)

[q] quit observer   [training continues in background]
```

### Read-only in v1

The console is deliberately passive — it observes but does not control. The
coordinator and agents are not aware of it. Quitting the console (`q`) has
no effect on the training run.

### Future: interactive control

See Risks and Future Work for the planned enhancement to allow the observer
to send guidance or interrupt the run.

---

## Risks and Future Work

### Risks

- **Source site changes**: The training ground truth (imported from DataLumos)
  is stable — those are outputs of our own effort. However, the *input*
  source sites may change (pages redesigned, URLs moved, datasets removed).
  This is a known risk but not something to solve in v1 — the orchestration
  MCP already handles source-side errors well, tracking exceptions and
  surfacing them for the user to decide what to do.

- **DSPy maturity**: DSPy is evolving rapidly. Pin a specific version in
  `requirements.txt` and be prepared to update as the API stabilizes. If
  DSPy introduces breaking changes, the phased adoption plan (Phase 3 in
  Implementation Plan) gives us a natural fallback — the system works
  without DSPy optimization.

### Future enhancements

- **Automated cataloging**: The system currently assumes a human has already
  enumerated datasets on a source site and dispatched volunteers for a first
  pass (see Training Data > Source). Automating the catalog step — using AI
  to crawl a source site and enumerate its datasets — would make it feasible
  to bring up training for a new data source much faster.

- **Interactive observer**: Extend the observer console (currently read-only)
  to allow a human to send guidance or interrupt the run — for example,
  pausing after N iterations to steer toward a specific fix. This is
  particularly useful when stuck on a plateau, where a human can spot
  structural issues that the AI keeps missing.
