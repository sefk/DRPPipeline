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
- [MCP Interface](#mcp-interface)
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
          ┌───────────────┐
          │  Coordinator  │
          │  (Python)     │
          └───────┬───────┘
                  │
     ┌────────────┼───────────┐
     │            │           │
     v            v           v
┌──────────┐ ┌──────────┐ ┌──────────┐
│ Agent 1  │ │ Agent 2  │ │ Agent N  │
│ (refine) │ │ (refine) │ │ (refine) │
└────┬─────┘ └────┬─────┘ └───┬──────┘
     │            │           │
     v            v           v
  ┌──────────────────────────────┐    ┌────────────────────────┐
  │      Training Database       ├───>│    Observer Console    │
  │     (iterations, scores,     │    │  (read-only live view) │
  │        diffs, cost)          │    │                        │
  └───────────────┬──────────────┘    │  score trend · agents  │
                  │                   │ cost · field breakdown │
                  v                   └────────────────────────┘
     ┌─────────────────────────┐
     │   Training Data Store   │
     │     (human-verified     │
     │      ground truth)      │
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

- **Cost**: Claude Sonnet is capable but expensive for iteration-heavy
  workloads. Gemini 2.5 Flash or Claude Haiku cost 5–20x less per token.
- **Budget**: Relying solely on Sonnet risks exhausting a token budget
  mid-run. Cheaper models can handle later iterations once the collector
  stabilizes.
- **Diversity**: Different models find different improvements. Running
  comparison runs across models is a good way to identify the best
  cost/quality tradeoff for a given collector.

### Supported backends

`SimpleRefiner` routes to the right API based on model name prefix:

| Model name prefix | Backend | Auth |
|-------------------|---------|------|
| `claude-*` | Anthropic API | `ANTHROPIC_API_KEY` |
| `gemini-*` | Google Generative Language API | `GOOGLE_API_KEY` **or** service account (`google-credentials.json` / `GOOGLE_APPLICATION_CREDENTIALS`). Requires Generative Language API enabled in the GCP project. |

### Configured pricing

Token costs are tracked automatically. Known models and their USD/1M token rates:

| Model | Input | Output |
|-------|-------|--------|
| `claude-sonnet-4-6` | $3.00 | $15.00 |
| `claude-haiku-4-5-20251001` | $0.80 | $4.00 |
| `claude-opus-4-6` | $15.00 | $75.00 |
| `gemini-2.5-flash` | $0.15 | $0.60 |
| `gemini-2.5-pro` | $3.50 | $10.50 |

Unknown models fall back to Sonnet pricing as a conservative estimate.

### Choosing a model for a run

Pass `model_refine` to `start_training_run`:

```python
# Default — Sonnet for quality
run_id = coord.create_run()  # uses TrainingConfig default: claude-sonnet-4-6

# Budget run — Haiku (no extra setup needed)
config = TrainingConfig(
    collector_name="CmsGovCollector",
    collector_module_name="cms_collector",
    source_site="data.cms.gov",
    model_refine="claude-haiku-4-5-20251001",
)

# Gemini run — requires GOOGLE_API_KEY
config = TrainingConfig(
    collector_name="CmsGovCollector",
    collector_module_name="cms_collector",
    source_site="data.cms.gov",
    model_refine="gemini-2.5-flash",
)
```

Or via the MCP tool:

```
start_training_run(
    collector_name="CmsGovCollector",
    collector_module_name="cms_collector",
    source_site="data.cms.gov",
    model_refine="gemini-2.5-flash",   # or "claude-haiku-4-5-20251001"
)
```

### Automatic cost-based model switching

The coordinator switches from `model_refine` to `cheap_model` once
`switch_to_cheap_at_pct` of the budget is consumed (default 50%). Both
fields accept any model name from the table above:

```python
config = TrainingConfig(
    ...
    model_refine="claude-sonnet-4-6",        # first half of budget
    cheap_model="claude-haiku-4-5-20251001", # second half
    switch_to_cheap_at_pct=0.50,
)
```

### Adding a new model

1. Add its pricing to `_MODEL_PRICING` in `collector_training/trainer.py`
2. If it uses a new API provider, add a `_call_<provider>` method to
   `SimpleRefiner` and extend the routing logic in `refine()`
3. Set the required API key env var

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

## MCP Interface

The `drp-collector-dev` MCP server (`mcp_collector_dev/server.py`) exposes the
full training workflow as tools. All training operations should go through this
interface rather than running Python scripts directly.

### Training tools

| Tool | Description |
|------|-------------|
| `start_training_run` | Initialize a new training run with configuration (model, budget, max iterations). Returns `run_id`. |
| `import_training_data` | Fetch rows from the Data Inventory spreadsheet and optionally scrape DataLumos for ground truth. Params: `run_id`, `sheet_id`, `sheet_gid`, `status_column`, `done_value`, `scrape_datalumos`. |
| `run_training_loop` | **Launch the training loop in the background.** Spawns `run_training.py --execute` as a detached subprocess; returns immediately with PID and log path. Prerequisites: run must exist and have training examples imported. |
| `evaluate_collector` | Run the current collector version against all training examples and score them. Params: `run_id`, `iteration_num` (0 = current file). Returns aggregate score and per-field breakdown. |
| `get_training_status` | Show current state of a training run: iteration count, best score, score trajectory, cost spent. |
| `get_iteration_details` | Show per-project scores, diffs, and field breakdown for a specific iteration. |
| `stop_training_run` | Gracefully stop a running training session. Sets a DB flag; the coordinator stops at the next iteration boundary. |
| `compare_collection_quality` | Evaluate an uploaded DataLumos project against others from different people. Scrapes public pages, auto-detects the right inventory tab from the source URL domain, and compares metadata completeness and file types. Params: `datalumos_url`, `sheet_id`, `sheet_gid` (auto-detected if omitted), `num_controls`. |

### Typical workflow

```
start_training_run(collector_name, collector_module_name, source_site, model_refine)
  → returns run_id

import_training_data(run_id, sheet_id, sheet_gid,
                     status_column="Data Added (Y/N/IP)", done_value="Y",
                     scrape_datalumos=True)
  → imports and scores examples into the training DB

run_training_loop(run_id)
  → spawns coordinator in background, returns PID + log path

get_training_status(run_id)          # monitor progress
stop_training_run(run_id)            # stop early if needed
```

### `run_training.py` — CLI entry point

`collector_training/run_training.py` provides a CLI equivalent that delegates
to the same MCP interface:

```bash
# Background launch (delegates to run_training_loop, returns immediately):
python collector_training/run_training.py <run_id>

# Direct execution (used internally by run_training_loop's subprocess):
python collector_training/run_training.py <run_id> --execute
```

Prefer the MCP interface over the CLI for all interactive and automated use.
The `--execute` flag is an implementation detail used by the MCP tool's
subprocess; it is not intended for direct use.

---

## Using the Chat Interface

All training operations can be driven through natural language in Claude Code.
The `drp-collector-dev` MCP server is loaded automatically, so you can just
describe what you want.

### Starting a new run

```
Start a new training run for CmsGovCollector using claude-haiku-4-5-20251001.
```

```
Start a training run for CmsGovCollector with gemini-2.5-flash, max 8 iterations
and a $5 budget.
```

Claude will call `start_training_run`, then `import_training_data` to populate
examples, then `run_training_loop` to launch the coordinator as a background
process. The tool returns immediately with a PID and log path; use
`get_training_status` to follow progress.

### Checking status

```
What's the status of training run 10?
```

```
How is the training going?
```

### Stopping and restarting

```
Stop training run 10.
```

```
Let's restart training run 10.
```

Restarting creates a new run (e.g. run 11) with the same config and examples,
then starts the coordinator fresh. Use this after fixing a bug or changing a
model parameter.

### Changing the model backend

```
Start a run using the Haiku model instead of Sonnet.
```

```
Run training with gemini-2.5-flash to compare cost vs quality.
```

For Gemini, the pipeline supports two auth methods (tried in order):

1. **API key** — set `GOOGLE_API_KEY` in your environment:
   ```
   ! export GOOGLE_API_KEY=your-key-here
   ```

2. **Service account** — uses `google-credentials.json` in the project root
   (already provisioned), or the path in `GOOGLE_APPLICATION_CREDENTIALS`.
   Requires the **Generative Language API** to be enabled in the Google Cloud
   project associated with the credentials.

### Promoting a trained version

```
What's the best collector version generated so far?
```

```
Promote run 10 iteration 2 to the production collector.
```

### Parallel comparison runs

To compare models side-by-side, ask Claude to start multiple runs at once:

```
Start two training runs in parallel — one with claude-haiku-4-5-20251001
and one with gemini-2.5-flash — so we can compare cost and quality.
```

Claude will launch both runs in the background and report results for each
when they complete.

### Evaluating collection quality

After a dataset has been uploaded to DataLumos, you can ask how well the
collection compares to others in the inventory:

```
How well did we do collecting and uploading the
"Federally Qualified Health Center All Owners" dataset?
```

```
Evaluate the quality of https://www.datalumos.org/datalumos/project/247122/version/V1/view
```

Claude will call `compare_collection_quality`, which:

1. Scrapes the target project's public DataLumos page (no authentication needed).
2. Looks up the project's source URL in the local pipeline DB to identify its
   data source (e.g. `data.cms.gov`).
3. Auto-detects the matching spreadsheet tab (e.g. "CMS") by downloading the
   inventory spreadsheet's tab list and matching the source domain.
4. Finds 3 completed projects in that tab claimed by **different people** to
   use as controls, and scrapes each one.
5. Compares metadata completeness and file types, and returns a report.

Example output:

```
=== Collection Quality Report ===
  Treatment : DataLumos #247122  "Federally Qualified Health Center All Owners"  (claimed by: sefk)
  (tab auto-detected: "CMS" gid=864890349)
  Controls  : #238570 (Jennifer @WashU), #238228 (mkraley), #238565 (??)

── Metadata Completeness ──
  ✓  Title                            set
  -  Agency                           empty everywhere (skip)
  ✗  Summary                          very short: 268 chars vs controls avg 931
  ✓  Keywords / Subject Terms         set
  ✗  Geographic Coverage              MISSING  (1/3 controls have it)
  ✗  Time Period                      MISSING  (2/3 controls have it)
  ✓  Data Types                       set  (controls all empty)
  ✓  Collection Notes                 set  (controls all empty)

── Files ──
  ✓  Data files (10): FQHC_All_Owners_2023.11.01.csv, ...  (+6 more)
  ~  No documentation files (data dictionaries, guides, READMEs)
  ✓  File count: 10  (controls avg 0.7)

── Summary ──
  Overall: RED  (6 OK, 1 warnings, 3 failures, 1 skipped)

  To improve:
    • Summary  very short: 268 chars vs controls avg 931
    • Geographic Coverage  MISSING  (1/3 controls have it)
    • Time Period  MISSING  (2/3 controls have it)
```

You can also call the tool directly with explicit parameters:

```python
compare_collection_quality(
    datalumos_url="247122",
    sheet_id="1fpNctIesSYc2giu0aHduYLBxVYqlsMMMVhKIPVtY7P0",
    # sheet_gid auto-detected from source URL; or pass explicitly to override
    num_controls=3,
)
```

The tool lives in `mcp_collector_dev/server.py` alongside the training tools.

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

## How to Run

### Prerequisites

1. Install new dependencies:

   ```bash
   pip install anthropic dspy-ai
   # or: pip install -r requirements.txt
   ```

2. Add your Anthropic API key to the environment:

   ```bash
   export ANTHROPIC_API_KEY=sk-ant-...
   ```

3. Make sure `config.json` has `base_output_dir` set to a writable directory.
   The evaluator creates output folders there when running the collector.

---

### Step 1 — Import training data

Training data must be loaded before starting the loop. There are two paths:

**A. From a pre-scraped JSON file (fastest, no browser required)**

Create a JSON file with one object per training example:

```json
[
  {
    "source_url": "https://data.cms.gov/some-dataset",
    "control_datalumos_id": "246966",
    "annotator": "alice",
    "ground_truth": {
      "title": "Medicare Part D Drug Spending",
      "agency": "Centers for Medicare & Medicaid Services",
      "summary": "...",
      "keywords": "medicare, drug spending, part d",
      "time_start": "2013",
      "time_end": "2022",
      "data_types": "tabular",
      "files": [{"name": "Medicare_Part_D_Drug_Spending_2022.csv"}],
      "collection_notes": "Downloaded from CMS open data portal.",
      "geographic_coverage": "United States"
    }
  }
]
```

Then import and run:

```python
from collector_training.schema import init_db
from collector_training.coordinator import TrainingConfig, TrainingCoordinator
from collector_training.importer import import_from_json_file, assign_train_validation_split

init_db()

config = TrainingConfig(
    collector_name="CmsGovCollector",        # Class name (file: collectors/CmsGovCollector.py)
    collector_module_name="cms_collector",   # Registered module name in Orchestrator
    source_site="data.cms.gov",
    max_iterations=20,
    max_cost_usd=5.00,
    model_refine="claude-sonnet-4-6",
)
coord = TrainingCoordinator(config)
run_id = coord.create_run()                 # Saves v0 from the current collector file

import_from_json_file(run_id, "my_training_data.json")
n_train, n_val = assign_train_validation_split(run_id)
print(f"Run {run_id}: {n_train} training, {n_val} validation examples")
```

**B. From Google Sheets (requires the sheet to be world-readable)**

```python
from collector_training.importer import import_from_spreadsheet, assign_train_validation_split

# sheet_id and gid from the spreadsheet URL
count = import_from_spreadsheet(
    run_id=run_id,
    sheet_id="1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY",
    sheet_gid="864890349",
    url_column="URL",
    status_column="Status",
    done_value="DONE",
    download_location_column="Download Location",
    scrape_datalumos=True,    # Requires Playwright + DataLumos login in config.json
)
assign_train_validation_split(run_id)
```

---

### Step 2 — Run the training loop

```python
result = coord.run(run_id)

print(f"Best score:     {result.best_score:.3f}")
print(f"Best iteration: {result.best_iteration}")
print(f"Total cost:     ${result.total_cost_usd:.4f}")
print(f"Stop reason:    {result.stop_reason}")
print(f"Best collector: {result.best_collector_path}")
```

The coordinator writes versioned collector files to
`collector_training/versions/` and copies the best-scoring version back to
`collectors/CmsGovCollector.py` when done.

**One-shot convenience wrapper:**

```python
result = coord.start_and_run()   # create_run() + run() in one call
```

---

### Step 3 — Watch with the observer console

In a second terminal, while training is running:

```bash
python -m collector_training.observer <run_id>
# With options:
python -m collector_training.observer 7 --interval 10 --db collector_training.db
```

Press `q` + Enter to quit the observer. Training continues unaffected.

---

### Step 4 — Inspect results

```python
from collector_training.schema import get_connection

con = get_connection()

# Score trajectory
for row in con.execute(
    "SELECT iteration_num, aggregate_score, model_used "
    "FROM iterations WHERE run_id=? ORDER BY iteration_num", (run_id,)
):
    print(f"  Iter {row['iteration_num']:>2}: {row['aggregate_score']:.3f}  [{row['model_used']}]")

# Total cost
cost = con.execute(
    "SELECT SUM(cost_usd) FROM token_usage WHERE run_id=?", (run_id,)
).fetchone()[0]
print(f"Total cost: ${cost:.4f}")

# Worst-scoring training examples
for row in con.execute("""
    SELECT te.source_url, AVG(ps.score) as avg_score
    FROM project_scores ps
    JOIN training_examples te ON ps.example_id = te.example_id
    JOIN iterations it ON ps.iteration_id = it.iteration_id
    WHERE it.run_id=?
    GROUP BY te.example_id
    ORDER BY avg_score ASC
    LIMIT 5
""", (run_id,)):
    print(f"  {row['avg_score']:.3f}  {row['source_url']}")

con.close()
```

---

### Using the MCP tools (from Claude Code / agent context)

The `drp-collector-dev` MCP server exposes the training workflow as tools that
Claude can call on your behalf. You interact with Claude in natural language;
Claude calls the tools and reports back. Below are representative exchanges.

---

**Starting a training run**

> *You:* Start a training run for the CMS collector. Use a $5 budget.

Claude calls `start_training_run(collector_name="CmsGovCollector",
collector_module_name="cms_collector", source_site="data.cms.gov",
max_cost_usd=5.0)` and replies:

```
Created training run 7 for 'CmsGovCollector'.
Next steps:
  1. import_training_data(run_id=7, ...)
  2. evaluate_collector(run_id=7) — to score the baseline
  3. Run the full training loop via TrainingCoordinator.run(7)
```

---

**Importing training data**

> *You:* Import training data for run 7 from the CMS spreadsheet
> (sheet ID 1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY, gid 864890349).

Claude calls `import_training_data(run_id=7,
sheet_id="1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY",
sheet_gid="864890349")` and replies:

```
Imported 18 training examples for run 7.
Split: 14 training / 4 validation
Note: ground truth fields will be empty unless scrape_datalumos=True
      or you call import_from_json_file() with pre-scraped data.
```

---

**Scoring the baseline**

> *You:* Score the current CMS collector against the training set (run 7,
> iteration 0).

Claude calls `evaluate_collector(run_id=7, iteration_num=0)` and replies:

```
Evaluation — run 7, iteration 0
  Examples:        14
  Aggregate score: 0.412

  Per-field scores:
    files                   0.21  ████░░░░░░░░░░░░░░░░
    time_start              0.44  ████████░░░░░░░░░░░░
    keywords                0.55  ███████████░░░░░░░░░
    summary                 0.67  █████████████░░░░░░░
    title                   0.89  █████████████████░░░

  Worst cases:
    0.18  https://data.cms.gov/medicare-part-d/drug-spending
    0.22  https://data.cms.gov/provider-data/dataset/mj5m-pzi6
    0.31  https://data.cms.gov/...
```

---

**Checking on a running training loop**

> *You:* What's the status of training run 7?

Claude calls `get_training_status(run_id=7)` and replies:

```
Training Run 7: CmsGovCollector (data.cms.gov)
  Status:     RUNNING
  Started:    2026-03-18 14:02
  Examples:   14 training / 4 validation
  Iterations: 3 completed
  Cost:       $0.43 (12 LLM calls)

  Score trajectory:
    Iteration  0: 0.412  [—]
    Iteration  1: 0.581  [claude-sonnet-4-6]
    Iteration  2: 0.634  [claude-sonnet-4-6]
    Iteration  3: 0.701  [claude-sonnet-4-6]  ← best
```

---

**Drilling into a specific iteration**

> *You:* Show me what went wrong in iteration 2 of run 7 — specifically for
> the projects that scored below 0.5.

Claude calls `get_iteration_details(run_id=7, iteration_num=2)` and replies with
per-project scores and field-level diffs, e.g.:

```
Iteration 2 — run 7
  Aggregate score: 0.634
  Model:           claude-sonnet-4-6

  Per-field averages:
    files                   0.41  ████████░░░░░░░░░░░░
    ...

  Per-project scores (14 projects):
    0.28  https://data.cms.gov/medicare-part-d/drug-spending
           files: expected=['Medicare_Part_D_Drug_Spending_2022.csv'] actual=[]
           title: expected='Medicare Part D Drug Spending' actual='Drug Spending'
    ...
```

---

**Stopping the run early**

> *You:* Stop training run 7.

Claude calls `stop_training_run(run_id=7)` and replies:

```
Run 7 marked as stopped. Active iterations will complete normally.
```

---

**MCP tool reference**

The underlying tools and their parameters:

| Tool | Purpose |
|------|---------|
| `start_training_run(collector_name, collector_module_name, source_site, max_iterations, max_cost_usd, model_refine, notes)` | Create a new training run; returns `run_id` |
| `import_training_data(run_id, sheet_id, sheet_gid, url_column, status_column, done_value, download_location_column, max_rows, scrape_datalumos)` | Populate training examples from a spreadsheet |
| `evaluate_collector(run_id, iteration_num)` | Score a collector version against all training examples |
| `get_training_status(run_id)` | Summary: iteration count, score trajectory, cost |
| `get_iteration_details(run_id, iteration_num)` | Per-project scores and field-level diffs for one iteration |
| `stop_training_run(run_id)` | Gracefully stop at next iteration boundary |

---

### Rollback

If training produced a regression, roll back to the best-scoring version:

```python
from collector_training.coordinator import rollback_to_best

best_iter = rollback_to_best(run_id=7)
# Copies collectors/versions/CmsGovCollector_run7_v{best_iter}.py
# back to collectors/CmsGovCollector.py
```

---

### Running tests

```bash
python -m pytest tests/test_scoring.py -v
```

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
