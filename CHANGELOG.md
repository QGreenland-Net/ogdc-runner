# v0.2.0

- Add support for both `local` (via rancher-desktop) and `dev` environments
  (remote cluster) via the `ENVIRONMENT` envvar. `ENVIRONMENT=local` will cause
  a locally-built `ogdc-runner` image to be used for executing argo workflows.
  `ENVIRONMENT=dev` will tell argo to use the most recent `latest`-tagged
  `ghcr.io/qgreenland-net/ogdc-runner` image.
- Bugfix: allow viz workflow recipes to use an `id` other than `viz-workflow`.
- `meta.yaml`: Add `workflow` config that defines workflow-specifc configuration
  options (e.g., `shell` workflows have the `sh_file` config option).
- Resolve bug that prevented viz-workflow `config.json` file from being used if
  the `ogdc-runner` was passed a remote (GitHub) recipe directory (#101).
- Move validation of all recipe configuration (including sidecar files like
  `sh_file` for `ShellWorkflow` and `config_file` for `VizWorkflow` to Pydantic
  models. This requires that some models (`Shell` and `Viz`-`Workflow`) be
  provided the `recipe_directory` context on instantiation.

# v0.1.0

- This initial release provides a basic API/CLI for submitting `shell` and
  `viz_workflow` OGDC recipes. This release is intended for internal
  QGreenland-Net team use only.
