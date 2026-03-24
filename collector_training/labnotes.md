# Collector Training Lab Notes

## Table of Contents
- [2026-03-19 — CmsGovCollector, Training Run 1: Initial Setup and POC](#2026-03-19--cmsgov-collector-training-run-1-initial-setup-and-poc)
- [2026-03-21 — CmsGovCollector, Training Run 2: First Complete E2E](#2026-03-21--cmsgov-collector-training-run-2-first-complete-e2e)
- [2026-03-22 — CmsGovCollector, Training Run 3: Expanded Dataset](#2026-03-22--cmsgov-collector-training-run-3-expanded-dataset)
- [2026-03-23 — CmsGovCollector, Training Runs 4–10: Bug Fixes and First Successful Iteration](#2026-03-23--cmsgov-collector-training-runs-410-bug-fixes-and-first-successful-iteration)

---

## 2026-03-19 — CmsGovCollector, Training Run 1: Initial Setup and POC

### Summary
Training infrastructure set up for `CmsGovCollector` and 15 examples imported, but the training loop was never executed. Run remains in `running` status with 0 iterations. Superseded by Run 2.

### What Was Done
- Created training run 1 via `TrainingCoordinator.create_run()`
- Imported 15 examples from Google Sheets (12 training / 3 validation) with ground truth scraped from DataLumos workspaces
- Saved baseline collector as `CmsGovCollector_run1_v0.py`

### Data Quality Issues Discovered
5 of 15 examples had bad ground truth and were excluded from Run 2:
- **Example 2** (`comprehensive-care-for-joint-replacement-model-metropolitan-statistical-areas`) — files listed were HIFLD geospatial datasets (wrong DataLumos workspace, control ID 241282)
- **Examples 12–15** (Skilled Nursing Facility Cost Report, Medicare Part D/B Spending by Drug, Medicaid Spending by Drug) — completely empty ground truth (no title, agency, files, or dates)

### Artifacts
`collector_training/2026-03-19/` — 1 file:
- `CmsGovCollector_run1_v0.py` — baseline snapshot at time of run creation

---

## 2026-03-21 — CmsGovCollector, Training Run 2: First Complete E2E

### Summary
First completed training run for `CmsGovCollector`. Improved aggregate score from **0.394 → 0.494** (~25% gain) over 5 iterations before plateauing.

### Method
- **Framework:** `TrainingCoordinator` / `SimpleRefiner` (evaluate → analyze → refine loop)
- **Refinement model:** `claude-sonnet-4-6` (all 5 iterations; budget never crossed 50% threshold to switch to Haiku)
- **Evaluation:** Collector subprocess run against real `data.cms.gov` URLs, results scored against ground truth
- **Stopping condition:** Score plateau (max improvement < 0.01 over 3-iteration window)
- **Duration:** ~46 minutes | **Cost:** $0.80

### Training Data
- **Source:** `data.cms.gov` dataset pages
- **Examples:** 10 total (8 training / 2 validation)
- Ground truth scraped from DataLumos workspaces
- Note: 5 examples from run 1 were dropped due to bad ground truth — 1 had HIFLD geospatial files (wrong DataLumos workspace), 4 had completely empty metadata

### Score Trajectory

| Iteration | Aggregate | Notes |
|-----------|-----------|-------|
| 1 (baseline) | 0.394 | Original collector (v0) |
| 2 | 0.490 | +0.096 |
| 3 | **0.494** | **Best — written to collector** |
| 4 | 0.419 | Regression |
| 5 | 0.000 | Crash / breaking change introduced |

### Per-Field Scores (baseline → best)

| Field | Baseline (iter 1) | Best (iter 3) | Change |
|-------|:-----------------:|:-------------:|:------:|
| files | 0.439 | 0.439 | — |
| title | 0.625 | 0.625 | — |
| summary | 0.595 | 0.595 | — |
| agency | 0.000 | 0.625 | **+0.625** |
| data_types | 0.000 | 0.625 | **+0.625** |
| time_end | 0.500 | 0.525 | +0.025 |
| time_start | 0.213 | 0.238 | +0.025 |
| keywords | 0.023 | 0.025 | +0.002 |
| collection_notes | 0.625 | 0.625 | — |
| geographic_coverage | 0.625 | 0.625 | — |

### What Improved
- **`agency`** — went from 0 to 0.625. The original collector hardcoded `"Centers for Medicare & Medicaid Services"` but ground truth expected `"Centers for Medicare and Medicaid Services, United States Department of Health and Human Services"`. Refinement fixed the full agency string.
- **`data_types`** — went from 0 to 0.625. Original left this to file-extension inference; refinement improved the mapping logic.
- Minor gains in `time_start` / `time_end`.

### What Didn't Improve
- **`files`** (weight 3.0 — highest weight) stayed flat at 0.439. This is the hardest field and the biggest remaining opportunity.
- **`keywords`** remained near 0 (0.023 → 0.025). Ground truth uses CMS-specific keyword taxonomy that's hard to infer.
- **`summary`** stayed at 0.595 — already decent, room to improve.

### Artifacts
`collector_training/2026-03-21/` — 6 versioned collector snapshots:
- `CmsGovCollector_run2_v0.py` — baseline (original)
- `CmsGovCollector_run2_v1.py` through `v4.py` — intermediate iterations
- `CmsGovCollector_run2_v5.py` — crashed (score 0.000), not used

The best version (v3) was automatically promoted to `collectors/CmsGovCollector.py`.

### Observations & Next Steps
- The score plateau after 3 iterations suggests the training signal is weak — likely due to the small example set (8 examples) and noisy ground truth for `files`.
- Iteration 5 scoring 0.000 (total crash) is a red flag — the refiner introduced a breaking change. Worth inspecting `CmsGovCollector_run2_v5.py` to see what broke.
- To make further progress: more training examples, better ground truth for `files` (correct file names + hrefs), and possibly a higher `score_plateau_threshold` to run longer.

---

## 2026-03-22 — CmsGovCollector, Training Run 3: Expanded Dataset

### Hypothesis
Run 2 plateaued at 0.494 with only 8 training examples — too thin to give the refiner a reliable signal, especially for `files` (weight 3.0, stuck at 0.439). Expanding to ~30 examples from the Data Inventories sheet should provide a stronger gradient and improve generalization.

### Data Source
Google Sheets: [Data Inventories](https://docs.google.com/spreadsheets/d/1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY/edit?gid=864890349)
- Sheet GID: `864890349`
- Filter: `Data Added (Y/N/IP) = Y` with a DataLumos download link
- ~30 new examples available beyond the 10 used in run 2

### Ground Truth Scraping Refactor
The original `scrape_datalumos=True` path used the authenticated **workspace editing interface** (`/datalumos/workspace?goToPath=...`), which requires being the project owner. Contributors' projects (mkraley, eksm, JB, etc.) are inaccessible this way.

**New approach:** `read_project_public_view()` in `tests/compare_datalumos.py` scrapes the public `/project/{id}/version/V1/view` page via a standalone headless Playwright session — no credentials, works for any project.

DOM pattern on the public view:
- All metadata fields: `div.panel-heading` with `<strong>Label:</strong>` + help `<a>` + text
- Files: `table.table-striped tbody tr` → `td a` for name and href
- `_EXTRACT_PUBLIC_VIEW_JS` uses a generic `fieldMap` approach keyed on lowercased label text

`importer.py`'s `_scrape_datalumos_project` simplified to 3 lines — no Args bootstrapping, no credentials.

### Risks
- **Ground truth quality:** run 1 showed some DataLumos workspaces had wrong or empty data; validate a sample before training
- **Cloudflare:** public view pages pass the JS challenge in a real Playwright browser (headless Chromium with anti-detection flags); plain HTTP fetches are blocked
- **Eval time:** ~9 min/iteration at 8 examples → potentially 30-40 min/iteration at 30+ examples
- **Budget:** refinement prompts grow with more worst-case diffs; cap at `max_iterations=10`

### Plan
1. `start_training_run` — run 3, `max_iterations=10`, `max_cost_usd=10`
2. `import_training_data` — sheet above, `status_column="Data Added (Y/N/IP)"`, `done_value="Y"`, `scrape_datalumos=True`
3. Spot-check `files` ground truth on a sample before running
4. Execute training loop

### Results
*(superseded — dataset was imported into run 4 and subsequent runs)*

---

## 2026-03-23 — CmsGovCollector, Training Runs 4–10: Bug Fixes and First Successful Iteration

### Summary
A long debugging session that uncovered and fixed four distinct bugs blocking
the training loop. Run 10 iteration 2 achieved a score of **0.753** — the best
result yet — and was promoted to production.

### Dataset
68 training / 17 validation examples (expanded from run 3's import). Examples
were carried forward into each restart run via SQL copy.

### Bugs Fixed

#### 1. `run_training.py` — module-level code crashed `_find_module_class`
`run_training.py` had hardcoded `RUN_ID = 2` and executed `int(sys.argv[1])` at
module level. The Orchestrator's `_find_module_class` walks all packages with
`pkgutil.walk_packages`, which imports the module and triggered the `int()`
call with `sys.argv[1] = "cms_collector"` → `ValueError`. Fixed by guarding all
executable code under `if __name__ == "__main__"` and loading config from the
DB for the given run_id.

#### 2. `CmsGovCollector.py` — missing `_update_storage` / `_cleanup_browser`
A previous training run's `_finalize` step had overwritten
`collectors/CmsGovCollector.py` with a version that lacked `_update_storage`
and `_cleanup_browser`. Both methods were called in `run()` but not defined,
causing every evaluation to score 0.000 with `AttributeError`. Restored from
the last git commit and added the LLM-generated improvements from run 6
(`_fetch_slug_with_fallback`, additional description selectors).

#### 3. `max_tokens=8192` — LLM output truncated on every refinement
The collector file is ~6 500 tokens. With `max_tokens=8192` the LLM had barely
enough room to output an unchanged file, let alone add improvements — every
generated version was truncated mid-statement, producing a `SyntaxError`.
Increased to `max_tokens=16000`. Also added:
- Syntax validation in `SimpleRefiner.refine()` — falls back to original code
  rather than saving a broken version
- Syntax check in `_finalize()` — refuses to overwrite the production collector
  with a syntactically invalid file

#### 4. Anthropic API credit exhaustion
Runs 8 and 9 failed mid-run with `BadRequestError: credit balance too low`.
Resolved by topping up the correct account ("Sef's Individual Org",
key `sk-ant-api03-AhSdCDk...`).

### Score Trajectory (runs 4–10)

| Run | Best Score | Best Iter | Notes |
|-----|-----------|-----------|-------|
| 4 | — | — | Stopped: 0 iterations (bug 1) |
| 5 | 0.000 | — | All iterations crashed (bugs 1+2) |
| 6 | 0.666 | 1 | Bug 1 fixed; iters 2–5 crashed (bug 3) |
| 7 | — | — | Stopped pre-run |
| 8 | — | — | Stopped pre-run |
| 9 | 0.666 | 1 | Bugs 1+2 fixed; iters 2–5 crashed (bug 3) |
| 10 | **0.753** | **2** | All bugs fixed; stopped by user after iter 2 |

Run 10 iteration 1 baseline scored ~0.666 aggregate (0.846 on working examples,
0.000 on ~20 examples where the Slug API returns nothing for long paths). The
LLM-refined v2 pushed to **0.753**. Run was stopped before further iterations.

### Production Promotion
`collectors/CmsGovCollector.py` ← `CmsGovCollector_run10_v2.py` (score 0.753).

### Infrastructure Improvements
- `run_training_loop` MCP tool added to `mcp_collector_dev/server.py` — spawns
  the training coordinator as a detached background subprocess and returns
  immediately with PID and log path. Preferred over running `run_training.py`
  directly. Use `get_training_status` / `stop_training_run` to monitor/control.
- `run_training.py` refactored to use the MCP interface: default CLI invocation
  delegates to `run_training_loop`; `--execute` flag is the subprocess entry
  point used internally by the MCP tool.
- `SimpleRefiner` now supports multiple LLM backends: `claude-*` → Anthropic
  API, `gemini-*` → Google Generative Language API
- Gemini auth supports both `GOOGLE_API_KEY` and service account credentials
  (`google-credentials.json` / `GOOGLE_APPLICATION_CREDENTIALS`)
- Training docs updated with multi-backend usage and chat interface examples

### Remaining Issues
- ~20 examples consistently score 0.000 due to "Slug API returned nothing"
  for certain URL paths (long innovation center program paths). These drag down
  the aggregate. Either the fallback logic needs improvement or these examples
  should be excluded from training.
- Run 10 was stopped at iteration 2 — more iterations may improve further.
- Haiku (run 13) and Gemini 2.5 Flash (run 14) comparison runs started in the
  next session. Run 13 has 80/20 examples imported; run 14 needs data copied
  from run 13 before `run_training_loop` can be called.
