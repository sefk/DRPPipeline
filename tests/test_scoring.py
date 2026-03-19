"""
Unit tests for collector_training.scorer.

Tests per-field scoring functions and aggregate project scoring,
including failure-mode floors.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_training.scorer import (
    FIELD_WEIGHTS,
    best_annotator_score,
    per_field_averages,
    score_agency,
    score_collection_notes,
    score_data_types,
    score_date,
    score_files,
    score_geographic_coverage,
    score_keywords,
    score_project,
    score_summary,
    score_title,
)


# ── score_title ──────────────────────────────────────────────────────────────

class TestScoreTitle:
    def test_exact_match(self):
        assert score_title("Medicare Data", "Medicare Data") == pytest.approx(1.0)

    def test_case_insensitive(self):
        s = score_title("medicare data", "Medicare Data")
        assert s >= 0.9

    def test_both_empty(self):
        assert score_title(None, None) == pytest.approx(1.0)
        assert score_title("", "") == pytest.approx(1.0)

    def test_one_empty(self):
        assert score_title(None, "Some Title") == pytest.approx(0.0)
        assert score_title("Some Title", None) == pytest.approx(0.0)

    def test_partial_match_gives_bonus(self):
        # Substring → bonus applied
        s_sub = score_title("Medicare", "Medicare Data 2020")
        s_no  = score_title("Medicare", "Diabetes Statistics")
        assert s_sub > s_no

    def test_completely_different(self):
        s = score_title("Alpha Bravo Charlie", "Delta Echo Foxtrot")
        assert s < 0.2

    def test_result_bounded(self):
        # Should never exceed 1.0
        s = score_title("A", "A")
        assert 0.0 <= s <= 1.0


# ── score_date ───────────────────────────────────────────────────────────────

class TestScoreDate:
    def test_exact_string_match(self):
        assert score_date("2020-01-01", "2020-01-01") == pytest.approx(1.0)

    def test_same_year(self):
        s = score_date("2020-06-01", "2020-01-01")
        assert s == pytest.approx(0.8)

    def test_within_two_years(self):
        s = score_date("2020", "2022")
        assert s == pytest.approx(0.5)

    def test_within_five_years(self):
        s = score_date("2015", "2019")
        assert s == pytest.approx(0.2)

    def test_far_apart(self):
        s = score_date("2000", "2020")
        assert s == pytest.approx(0.0)

    def test_both_empty(self):
        assert score_date(None, None) == pytest.approx(1.0)

    def test_ground_truth_empty(self):
        # N/A — no penalty
        assert score_date("2020", None) == pytest.approx(1.0)

    def test_collector_empty(self):
        assert score_date(None, "2020") == pytest.approx(0.0)


# ── score_files ──────────────────────────────────────────────────────────────

class TestScoreFiles:
    def _files(self, names):
        return [{"name": n} for n in names]

    def test_exact_match(self):
        f = self._files(["data.csv", "readme.txt"])
        assert score_files(f, f) == pytest.approx(1.0)

    def test_both_empty(self):
        assert score_files([], []) == pytest.approx(1.0)
        assert score_files(None, None) == pytest.approx(1.0)

    def test_no_collector_files(self):
        g = self._files(["data.csv"])
        assert score_files([], g) == pytest.approx(0.0)
        assert score_files(None, g) == pytest.approx(0.0)

    def test_missing_some_files(self):
        c = self._files(["data.csv"])
        g = self._files(["data.csv", "readme.txt", "schema.json"])
        s = score_files(c, g)
        assert 0.0 < s < 1.0

    def test_extra_files_not_penalized_heavily(self):
        # A collector that gets ALL required files + extras should score better
        # than one that misses required files. Recall (weight 0.7) dominates.
        g = self._files(["data.csv", "readme.txt"])
        c_all_plus_extra = self._files(["data.csv", "readme.txt", "bonus.pdf"])
        c_missing_one    = self._files(["data.csv"])
        s_extra   = score_files(c_all_plus_extra, g)
        s_missing = score_files(c_missing_one, g)
        assert s_extra > s_missing

    def test_case_insensitive(self):
        c = self._files(["DATA.CSV"])
        g = self._files(["data.csv"])
        assert score_files(c, g) == pytest.approx(1.0)


# ── score_keywords ───────────────────────────────────────────────────────────

class TestScoreKeywords:
    def test_exact_match(self):
        assert score_keywords("alpha, beta, gamma", "alpha, beta, gamma") >= 0.99

    def test_partial_overlap(self):
        s = score_keywords("alpha, beta", "alpha, beta, gamma, delta")
        assert 0.0 < s < 1.0

    def test_no_overlap(self):
        s = score_keywords("foo, bar", "baz, qux")
        assert s == pytest.approx(0.0)

    def test_both_empty(self):
        assert score_keywords(None, None) == pytest.approx(1.0)

    def test_ground_truth_empty(self):
        assert score_keywords("foo", None) == pytest.approx(1.0)


# ── score_collection_notes ───────────────────────────────────────────────────

class TestScoreCollectionNotes:
    def test_strips_download_date(self):
        c = "Collected from CMS website. (Downloaded 2024-03-01)"
        g = "Collected from CMS website. (Downloaded 2023-01-15)"
        s = score_collection_notes(c, g)
        assert s >= 0.9

    def test_different_notes(self):
        s = score_collection_notes("alpha beta gamma", "delta epsilon zeta")
        assert s < 0.2


# ── score_project ────────────────────────────────────────────────────────────

class TestScoreProject:
    def _gt(self, **kwargs):
        base = {
            "title": "Test Dataset",
            "summary": "A test dataset for unit tests.",
            "agency": "Test Agency",
            "keywords": "test, data",
            "time_start": "2020",
            "time_end": "2022",
            "data_types": "tabular",
            "files": [{"name": "data.csv"}],
            "collection_notes": "Manually collected.",
            "geographic_coverage": "United States",
        }
        base.update(kwargs)
        return base

    def _output(self, **kwargs):
        base = {
            "title": "Test Dataset",
            "summary": "A test dataset for unit tests.",
            "agency": "Test Agency",
            "keywords": "test, data",
            "time_start": "2020",
            "time_end": "2022",
            "data_types": "tabular",
            "files": [{"name": "data.csv"}],
            "folder_path": "/tmp/DRP000001",
            "collection_notes": "Manually collected.",
            "geographic_coverage": "United States",
            "status": "collected",
        }
        base.update(kwargs)
        return base

    def test_perfect_match(self):
        result = score_project(self._output(), self._gt())
        assert result["aggregate"] >= 0.9

    def test_crashed_collector(self):
        result = score_project({"_crashed": True}, self._gt())
        assert result["aggregate"] == pytest.approx(0.0)
        for field in FIELD_WEIGHTS:
            assert result[field] == pytest.approx(0.0)

    def test_error_status(self):
        result = score_project({"status": "error"}, self._gt())
        assert result["aggregate"] == pytest.approx(0.0)

    def test_no_files_floor(self):
        # Missing folder_path → cap at 0.2
        output = self._output()
        del output["folder_path"]
        output.pop("files", None)
        result = score_project(output, self._gt())
        assert result["aggregate"] <= 0.2

    def test_wrong_files_floor(self):
        # files score near 0 → cap at 0.3
        output = self._output(files=[{"name": "completely_wrong.xlsx"}])
        result = score_project(output, self._gt(files=[{"name": "data.csv"}]))
        assert result["aggregate"] <= 0.3

    def test_result_has_all_fields(self):
        result = score_project(self._output(), self._gt())
        assert "aggregate" in result
        assert "diff" in result
        for field in FIELD_WEIGHTS:
            assert field in result

    def test_diff_structure(self):
        output = self._output(title="Wrong Title")
        result = score_project(output, self._gt())
        assert result["diff"]["title"]["expected"] == "Test Dataset"
        assert result["diff"]["title"]["actual"] == "Wrong Title"


# ── per_field_averages ───────────────────────────────────────────────────────

class TestPerFieldAverages:
    def test_basic(self):
        scores = [
            {"title": 0.8, "files": 0.6, "summary": 1.0},
            {"title": 0.4, "files": 0.8, "summary": 0.5},
        ]
        avgs = per_field_averages(scores)
        assert avgs["title"] == pytest.approx(0.6)
        assert avgs["files"] == pytest.approx(0.7)
        assert avgs["summary"] == pytest.approx(0.75)

    def test_empty_list(self):
        assert per_field_averages([]) == {}


# ── best_annotator_score ─────────────────────────────────────────────────────

class TestBestAnnotatorScore:
    def test_picks_best(self):
        collector = {
            "title": "Dataset A",
            "files": [{"name": "data.csv"}],
            "folder_path": "/tmp/x",
        }
        annotators = [
            {"title": "Completely Different", "files": [{"name": "other.csv"}]},
            {"title": "Dataset A",            "files": [{"name": "data.csv"}]},
        ]
        result = best_annotator_score(collector, annotators)
        assert result["aggregate"] >= 0.5

    def test_empty_annotators(self):
        result = best_annotator_score({}, [])
        assert result["aggregate"] == pytest.approx(0.0)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
