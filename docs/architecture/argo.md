# Argo Workflows

[OGDC recipes](../recipes.md) are executed via one or more
[argo-workflows](https://argo-workflows.readthedocs.io/). The
[OGDC Service API](./service.md) is responsible for translating user recipes
into argo workflows that are submitted and executed by Argo.

This document includes details of how Argo is setup and used by the OGDC.

## argo-workflows server configuration

The configuration for argo-workflows is defined in the
[ogdc-helm](https://github.com/qgreenland-net/ogdc-helm) repository. Refer to
the configurations there to see the specifics of how argo is setup for
deployment.

## Argo Python API (Hera)

The OGDC uses the [hera](https://hera.readthedocs.io/en/stable/) python package
to interact with the argo-workflows API. Boilerplate setup and configuration of
argo via hera can be found in {mod}`ogdc_runner.argo`.

## Workflow artifacts

Argo
[artifacts](https://argo-workflows.readthedocs.io/en/latest/walk-through/artifacts/)
are configured to use an S3-compatible
[artifact repository](https://argo-workflows.readthedocs.io/en/latest/configure-artifact-repository/).

Artifacts are used to store intermediate workflow outputs and the final output
of recipes with a `temporary` output type.

Artifacts will be automatically garbage collected on workflow deletion. See the
{ref}`workflow-persistence` section below for details on automatic workflow
deletion.

(workflow-persistence)=

## Workflow persistence

Successful Argo workflows are retained for 1 day. Workflows associated with
recipes with the `temporary` output type are retained for 7 days to allow for
sufficient time to retrieve final outputs.

Successful workflows with the `ogdc/persist-workflow-in-archive: true` label
will be archived in the OGDC's postgresql database for long-term storage. These
archived workflows can be used for metrics and data provenance purposes.

Non-successful workflows are not automatically cleaned up or archived. They are
retained for inspection/debugging and should be cleaned up manually once the
issue leading to failure is resolved.

To ensure consistent behavior of OGDC-submitted argo workflows (e.g., setting
the archival label and TTL for successful workflows), the
{class}`ogdc_runner.argo.OgdcWorkflow` context manager has been defined to wrap
the behavior of [hera's](https://hera.readthedocs.io/en/stable/)
[Workflow](https://hera.readthedocs.io/en/stable/api/workflows/workflow_classes/workflow/#hera.workflows.workflow.Workflow).
