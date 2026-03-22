# Collector Training Lab Notes

## Table of Contents
- [2026-03-19 — CmsGovCollector, Training Run 1: Initial Setup and POC](#2026-03-19--cmsgov-collector-training-run-1-initial-setup-and-poc)
- [2026-03-21 — CmsGovCollector, Training Run 2: First Complete E2E](#2026-03-21--cmsgov-collector-training-run-2-first-complete-e2e)

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
