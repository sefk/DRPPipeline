"""
Collector training loop executor and CLI entry point.

Two operating modes:

  --execute   Subprocess mode: run TrainingCoordinator.run() synchronously.
              Used by run_training_loop (MCP tool) as its background subprocess.

  (default)   CLI mode: delegate to run_training_loop, which launches the
              training loop in the background and returns immediately.
              Equivalent to calling run_training_loop via Claude / MCP.

Usage:
    python collector_training/run_training.py <run_id>             # background launch
    python collector_training/run_training.py <run_id> --execute   # direct execution
"""
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from collector_training.coordinator import TrainingConfig, TrainingCoordinator
from collector_training.schema import DEFAULT_DB_PATH, get_connection, init_db


def _execute(run_id: int) -> int:
    """Run training synchronously. Returns 0 on success, 1 on error."""
    init_db()
    con = get_connection(DEFAULT_DB_PATH)
    try:
        row = con.execute(
            "SELECT config_json FROM training_runs WHERE run_id=?", (run_id,)
        ).fetchone()
    finally:
        con.close()

    if row is None:
        print(f"Error: training run {run_id} not found", file=sys.stderr)
        return 1

    config_dict = json.loads(row["config_json"])
    config = TrainingConfig(**{
        k: v for k, v in config_dict.items()
        if k in TrainingConfig.__dataclass_fields__
    })
    coord = TrainingCoordinator(config)
    result = coord.run(run_id)

    print(f"\n=== Run {run_id} Complete ===")
    print(f"  Best score:  {result.best_score:.3f} (iter {result.best_iteration})")
    print(f"  Total cost:  ${result.total_cost_usd:.4f}")
    print(f"  Stop reason: {result.stop_reason}")
    if result.best_collector_path:
        print(f"  Output:      {result.best_collector_path}")
    return 0


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python collector_training/run_training.py <run_id> [--execute]")
        sys.exit(1)

    run_id = int(sys.argv[1])

    if "--execute" in sys.argv:
        sys.exit(_execute(run_id))
    else:
        # Delegate to the MCP interface: launches in background, returns immediately.
        from mcp_collector_dev.server import run_training_loop
        print(run_training_loop(run_id))


if __name__ == "__main__":
    main()
