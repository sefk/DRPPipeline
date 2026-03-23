"""
Scoring functions for the collector training system.

Converts collector output vs ground truth into continuous scores [0.0, 1.0].
Ported from tests/compare_datalumos.py (GREEN/YELLOW/RED → continuous).
"""
from __future__ import annotations

import re
from typing import Any, Optional

# ── Field weights ───────────────────────────────────────────────────────────

FIELD_WEIGHTS: dict[str, float] = {
    "files":               3.0,
    "title":               2.0,
    "summary":             2.0,
    "agency":              1.5,
    "keywords":            1.0,
    "time_start":          1.0,
    "time_end":            1.0,
    "data_types":          0.5,
    "collection_notes":    0.5,
    "geographic_coverage": 0.5,
}

# ── Text normalization helpers ──────────────────────────────────────────────

def _norm(s: Any) -> str:
    """Normalize whitespace and lowercase a string."""
    return re.sub(r"\s+", " ", str(s or "").lower()).strip()


def _tokenize(s: Any) -> set[str]:
    """Split normalized string into word tokens."""
    return set(re.findall(r"\w+", _norm(s)))


def _token_jaccard(a: Any, b: Any) -> float:
    """Token-level Jaccard similarity between two strings."""
    ta = _tokenize(a)
    tb = _tokenize(b)
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _set_from(value: Any) -> set[str]:
    """Convert a string or list to a set of normalized non-empty tokens."""
    if not value:
        return set()
    items = value if isinstance(value, list) else [value]
    result = set()
    for item in items:
        for part in re.split(r"[\n,;]+", str(item)):
            p = _norm(part)
            if p:
                result.add(p)
    return result


def _f1(c_set: set, g_set: set) -> float:
    """F1 score between two sets (precision + recall, equal weight)."""
    if not c_set and not g_set:
        return 1.0
    if not g_set:
        return 1.0   # Ground truth empty — treat as N/A
    if not c_set:
        return 0.0
    precision = len(c_set & g_set) / len(c_set)
    recall = len(c_set & g_set) / len(g_set)
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


# ── Per-field scorers ───────────────────────────────────────────────────────

def score_title(collector: Any, ground_truth: Any) -> float:
    """Token-level Jaccard + substring bonus."""
    c = _norm(collector)
    g = _norm(ground_truth)
    if not c and not g:
        return 1.0
    if not c or not g:
        return 0.0
    jaccard = _token_jaccard(c, g)
    bonus = 0.15 if (c in g or g in c) else 0.0
    return min(1.0, jaccard + bonus)


def score_summary(collector: Any, ground_truth: Any) -> float:
    """Token Jaccard similarity for summary text."""
    c = _norm(collector)
    g = _norm(ground_truth)
    if not c and not g:
        return 1.0
    if not g:
        return 1.0   # Ground truth empty — treat as N/A
    if not c:
        return 0.0
    return _token_jaccard(c, g)


def score_agency(collector: Any, ground_truth: Any) -> float:
    """F1 between agency sets (partial credit per matching org)."""
    return _f1(_set_from(collector), _set_from(ground_truth))


def score_keywords(collector: Any, ground_truth: Any) -> float:
    """F1 between keyword sets (precision + recall both matter)."""
    return _f1(_set_from(collector), _set_from(ground_truth))


def _parse_year(date_str: Any) -> Optional[int]:
    """Extract a 4-digit year from a date string, or return None."""
    if not date_str:
        return None
    m = re.search(r"\b(19|20)\d{2}\b", str(date_str))
    return int(m.group()) if m else None


def score_date(collector: Any, ground_truth: Any) -> float:
    """
    Date proximity:  exact=1.0, same year=0.8, ≤2 yrs=0.5, ≤5 yrs=0.2, else=0.0
    """
    c = _norm(collector)
    g = _norm(ground_truth)
    if not c and not g:
        return 1.0
    if not g:
        return 1.0   # Ground truth empty — N/A
    if not c:
        return 0.0
    if c == g:
        return 1.0
    c_year = _parse_year(c)
    g_year = _parse_year(g)
    if c_year is None or g_year is None:
        return 0.5 if (c in g or g in c) else 0.0
    diff = abs(c_year - g_year)
    if diff == 0:
        return 0.8
    if diff <= 2:
        return 0.5
    if diff <= 5:
        return 0.2
    return 0.0


def score_data_types(collector: Any, ground_truth: Any) -> float:
    """F1 between data type sets."""
    return _f1(_set_from(collector), _set_from(ground_truth))


def _extract_filenames(files: Any) -> set[str]:
    """Normalize a files value (list of dicts or strings or comma-sep string) to a set of names."""
    if not files:
        return set()
    # String: treat as comma-separated list of filenames
    if isinstance(files, str):
        return {_norm(n) for n in files.split(",") if _norm(n)}
    # List
    result = set()
    for f in files:
        if isinstance(f, dict):
            name = f.get("name", "")
        else:
            name = str(f)
        n = _norm(name)
        if n:
            result.add(n)
    return result


def score_files(collector: Any, ground_truth: Any) -> float:
    """
    Filename set comparison.  Missing files penalized more than extra files:
    recall weight 0.7, precision weight 0.3.
    """
    c_names = _extract_filenames(collector)
    g_names = _extract_filenames(ground_truth)
    if not c_names and not g_names:
        return 1.0
    if not g_names:
        return 1.0   # No expected files — N/A
    if not c_names:
        return 0.0
    recall = len(c_names & g_names) / len(g_names)
    precision = len(c_names & g_names) / len(c_names)
    return 0.7 * recall + 0.3 * precision


_DL_RE = re.compile(r"\s*\(Downloaded[^)]*\)\s*", re.IGNORECASE)


def score_collection_notes(collector: Any, ground_truth: Any) -> float:
    """Token Jaccard, stripping '(Downloaded YYYY-MM-DD)' patterns."""
    c = _DL_RE.sub("", _norm(collector)).strip()
    g = _DL_RE.sub("", _norm(ground_truth)).strip()
    if not c and not g:
        return 1.0
    if not g:
        return 1.0
    if not c:
        return 0.0
    return _token_jaccard(c, g)


def score_geographic_coverage(collector: Any, ground_truth: Any) -> float:
    """Normalized string comparison with substring bonus."""
    c = _norm(collector)
    g = _norm(ground_truth)
    if not c and not g:
        return 1.0
    if not g:
        return 1.0
    if not c:
        return 0.0
    if c == g:
        return 1.0
    if c in g or g in c:
        return 0.8
    return _token_jaccard(c, g)


# ── Project-level scoring ───────────────────────────────────────────────────

_FIELD_SCORERS = {
    "title":               score_title,
    "summary":             score_summary,
    "agency":              score_agency,
    "keywords":            score_keywords,
    "time_start":          score_date,
    "time_end":            score_date,
    "data_types":          score_data_types,
    "files":               score_files,
    "collection_notes":    score_collection_notes,
    "geographic_coverage": score_geographic_coverage,
}


def score_project(
    collector_output: dict[str, Any],
    ground_truth: dict[str, Any],
) -> dict[str, Any]:
    """
    Score collector output against one ground truth example.

    Returns a dict with per-field scores, 'aggregate', and 'diff'.
    collector_output may include a '_crashed' key or status='error' to indicate
    a total failure.
    """
    # Crashed collector → everything 0
    if collector_output.get("_crashed") or collector_output.get("status") == "error":
        field_scores = {f: 0.0 for f in FIELD_WEIGHTS}
        return {
            **field_scores,
            "aggregate": 0.0,
            "diff": {f: {"expected": ground_truth.get(f), "actual": None}
                     for f in FIELD_WEIGHTS},
        }

    field_scores: dict[str, float] = {}
    diff: dict[str, dict] = {}

    for field, scorer in _FIELD_SCORERS.items():
        c_val = collector_output.get(field)
        g_val = ground_truth.get(field)
        field_scores[field] = scorer(c_val, g_val)
        diff[field] = {"expected": g_val, "actual": c_val}

    # Apply failure-mode floors
    files = collector_output.get("files", [])
    if not files:
        # No files at all (not even planned) — cap aggregate at 0.2
        field_scores["files"] = 0.0
        aggregate = min(0.2, _weighted_average(field_scores))
    elif field_scores["files"] < 0.1:
        # Wrong files — cap aggregate at 0.3
        aggregate = min(0.3, _weighted_average(field_scores))
    else:
        aggregate = _weighted_average(field_scores)

    return {**field_scores, "aggregate": aggregate, "diff": diff}


def _weighted_average(field_scores: dict[str, float]) -> float:
    total_weight = 0.0
    total_score = 0.0
    for field, weight in FIELD_WEIGHTS.items():
        total_score += weight * field_scores.get(field, 0.0)
        total_weight += weight
    return total_score / total_weight if total_weight > 0 else 0.0


def per_field_averages(project_scores: list[dict[str, Any]]) -> dict[str, float]:
    """Average per-field score across multiple scored projects."""
    if not project_scores:
        return {}
    avgs: dict[str, float] = {}
    for field in FIELD_WEIGHTS:
        values = [s[field] for s in project_scores if field in s]
        avgs[field] = sum(values) / len(values) if values else 0.0
    return avgs


def best_annotator_score(
    collector_output: dict[str, Any],
    annotator_examples: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    When multiple annotators collected the same URL, return the best score.
    Prevents penalizing the collector for a reasonable alternative choice.
    """
    if not annotator_examples:
        return {"aggregate": 0.0}
    scored = [score_project(collector_output, gt) for gt in annotator_examples]
    return max(scored, key=lambda s: s["aggregate"])
