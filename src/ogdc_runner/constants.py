# .txt? To make sure it's clear the file can't be executed directly
from __future__ import annotations

# .yaml and .yml?
RECIPE_CONFIG_FILENAME = "meta.yml"

# Workflow Execution Limits
# Maximum number of tasks that can execute simultaneously in parallel workflows.
# This prevents resource exhaustion while allowing the workflow orchestrator to
# automatically schedule queued tasks as active tasks complete.
MAX_PARALLEL_LIMIT = 5
