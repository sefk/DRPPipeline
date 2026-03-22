"""Run the CmsGovCollector training loop for a given run_id."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from collector_training.coordinator import TrainingConfig, TrainingCoordinator
from collector_training.schema import init_db

RUN_ID = 2

init_db()

config = TrainingConfig(
    collector_name="CmsGovCollector",
    collector_module_name="cms_collector",
    source_site="data.cms.gov",
    max_iterations=20,
    max_cost_usd=10.0,
    model_refine="claude-sonnet-4-6",
    notes="Second training run — 10 clean examples",
)

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
