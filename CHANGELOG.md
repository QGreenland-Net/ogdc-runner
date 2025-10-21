# v0.2.0

- Add support for both `local` (via rancher-desktop) and `dev` environments
  (remote cluster) via the `ENVIRONMENT` envvar. `ENVIRONMENT=local` will cause
  a locally-built `ogdc-runner` image to be used for executing argo workflows.
  `ENVIRONMENT=dev` will tell argo to use the most recent `latest`-tagged
  `ghcr.io/qgreenland-net/ogdc-runner` image.

# v0.1.0

- This initial release provides a basic API/CLI for submitting `shell` and
  `viz_workflow` OGDC recipes. This release is intended for internal
  QGreenland-Net team use only.
