# Scripts Directory

This directory contains shell scripts used by OGDC Runner for workflow
orchestration.

## Scripts

### partition_processing.sh

Processes file partitions in parallel execution workflows. This script is used
by the {class}`ogdc_runner.parallel.ParallelExecutionOrchestrator` to handle
file processing in Argo workflows.

### list_pvc_inputs.sh

Recursively enumerates files from one or more mounted input PVC paths and writes
partition JSON for Argo `withParam` fan-out.

### stage_pvc_inputs.sh

Recursively links files from mounted input PVC paths into a workflow input
directory so sequential shell recipes can continue reading from `/input_dir`.
