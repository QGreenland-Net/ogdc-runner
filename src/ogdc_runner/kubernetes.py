from __future__ import annotations

from kubernetes import client

client_for_resource: dict[(str, str), any] = {
    ("Job", "batch/v1"): client.BatchV1,
    ("ConfigMap", "v1"): None,
}


def k8s_apply(
    *,
    yaml_objects: list[dict],
):
    """Declaratively apply a configuration to a kubernetes cluster.

    i.e. `kubectl apply -f my.yaml`. This capability is missing from the kubernetes
    Python bindings.

    The bindings do feature a `create_from_yaml` feature, but that is the imperative
    style that we'd like to avoid. If we use `kubernetes.utils.create_from_yaml`, for
    example, then we would need to handle deletion and re-creation, or patching of the
    object on subsequent runs.
    """

    for yaml_object in yaml_objects:
        kind = yaml_object["kind"]
        api = yaml_object["apiVersion"]

        api = client_for_resource[(kind, api)]

        breakpoint()
        api.apply(yaml_object)
