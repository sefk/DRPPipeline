"""
Database schema and initialization for the collector training system.

Uses a separate SQLite database (collector_training.db) from the main pipeline DB.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "collector_training.db"

_TRAINING_SCHEMA = """
CREATE TABLE IF NOT EXISTS training_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    collector_name TEXT NOT NULL,
    source_site TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT DEFAULT 'running',
    best_score REAL,
    best_iteration INTEGER,
    total_cost_usd REAL DEFAULT 0.0,
    config_json TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS training_examples (
    example_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES training_runs(run_id),
    source_url TEXT NOT NULL,
    annotator TEXT,
    control_datalumos_id TEXT,
    ground_truth_json TEXT NOT NULL,
    is_validation INTEGER DEFAULT 0,
    data_size_bytes INTEGER,
    imported_at TEXT NOT NULL,
    UNIQUE(run_id, source_url, annotator)
);

CREATE TABLE IF NOT EXISTS iterations (
    iteration_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES training_runs(run_id),
    iteration_num INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    collector_code TEXT NOT NULL,
    aggregate_score REAL,
    per_field_scores_json TEXT,
    refinement_strategy TEXT,
    model_used TEXT,
    parent_iteration INTEGER,
    UNIQUE(run_id, iteration_num)
);

CREATE TABLE IF NOT EXISTS project_scores (
    score_id INTEGER PRIMARY KEY AUTOINCREMENT,
    iteration_id INTEGER NOT NULL REFERENCES iterations(iteration_id),
    example_id INTEGER NOT NULL REFERENCES training_examples(example_id),
    score REAL NOT NULL,
    per_field_json TEXT,
    diff_json TEXT,
    collector_output_json TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS token_usage (
    usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
    iteration_id INTEGER REFERENCES iterations(iteration_id),
    run_id INTEGER NOT NULL REFERENCES training_runs(run_id),
    step TEXT NOT NULL,
    model TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    cost_usd REAL NOT NULL,
    timestamp TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_iterations_run ON iterations(run_id);
CREATE INDEX IF NOT EXISTS idx_scores_iteration ON project_scores(iteration_id);
CREATE INDEX IF NOT EXISTS idx_usage_run ON token_usage(run_id);
"""

# Evaluation DB uses the same schema as the main pipeline DB (projects table)
_EVAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    DRPID INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT,
    status_notes TEXT,
    warnings TEXT,
    errors TEXT,
    datalumos_id TEXT UNIQUE,
    source_url TEXT NOT NULL UNIQUE,
    folder_path TEXT,
    title TEXT,
    agency TEXT,
    office TEXT,
    summary TEXT,
    keywords TEXT,
    time_start TEXT,
    time_end TEXT,
    data_types TEXT,
    extensions TEXT,
    download_date TEXT,
    collection_notes TEXT,
    file_size TEXT,
    published_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_source_url ON projects(source_url);
CREATE INDEX IF NOT EXISTS idx_status ON projects(status);
"""


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Open and return a WAL-mode connection to the training database."""
    path = db_path or DEFAULT_DB_PATH
    con = sqlite3.connect(str(path), check_same_thread=False, timeout=30.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def init_db(db_path: Optional[Path] = None) -> None:
    """Create training database tables if they don't exist."""
    con = get_connection(db_path)
    con.executescript(_TRAINING_SCHEMA)
    con.commit()
    con.close()


def init_eval_db(db_path: Path) -> None:
    """Create a pipeline-schema evaluation database for training runs."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path), timeout=30.0)
    con.executescript(_EVAL_SCHEMA)
    con.commit()
    con.close()
