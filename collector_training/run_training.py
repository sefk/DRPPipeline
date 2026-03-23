"""Run the collector training loop for a given run_id."""
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from collector_training.coordinator import TrainingConfig, TrainingCoordinator
from collector_training.schema import DEFAULT_DB_PATH, get_connection, init_db

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m collector_training.run_training <run_id>")
        sys.exit(1)

    RUN_ID = int(sys.argv[1])

    init_db()

    con = get_connection(DEFAULT_DB_PATH)
    try:
        row = con.execute(
            "SELECT config_json FROM training_runs WHERE run_id=?", (RUN_ID,)
        ).fetchone()
    finally:
        con.close()

    if row is None:
        print(f"Error: training run {RUN_ID} not found")
        sys.exit(1)

    config_dict = json.loads(row[0])
    config = TrainingConfig(**{k: v for k, v in config_dict.items() if k in TrainingConfig.__dataclass_fields__})

    coord = TrainingCoordinator(config)

    print(f"Running training loop for run_id={RUN_ID}...")
    result = coord.run(RUN_ID)

    print(f"\n=== Training Complete ===")
    print(f"  Run ID:         {result.run_id}")
    print(f"  Best score:     {result.best_score:.3f}")
    print(f"  Best iteration: {result.best_iteration}")
    print(f"  Total cost:     ${result.total_cost_usd:.4f}")
    print(f"  Stop reason:    {result.stop_reason}")
    if result.best_collector_path:
        print(f"  Output:         {result.best_collector_path}")
