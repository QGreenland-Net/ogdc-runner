# .txt? To make sure it's clear the file can't be executed directly
from __future__ import annotations

# .yaml and .yml?
RECIPE_CONFIG_FILENAME = "meta.yml"

# Maximum number of parallel tasks that can be executed simultaneously
# This limit prevents resource exhaustion in the workflow orchestrator.
# This value is used to set the 'parallelism' parameter at the workflow level,
# which controls how many tasks can run concurrently. Remaining tasks are
# automatically scheduled as active tasks complete.

MAX_PARALLEL_LIMIT = 5
