# Architecture

```{mermaid}
graph LR

%% Definitions
subgraph ADC_K8S[ADC k8s]
  subgraph OGDC[OGDC]
    OGDC_RUNNER_SERVICE[ogdc-runner service]
    ARGO[Argo Workflows]
  end
end

subgraph OGDC_RUNNER_CLI[ogdc-runner CLI]
end

%% Relationships
OGDC_RUNNER_CLI -->|"submit|status"| OGDC_RUNNER_SERVICE

OGDC_RUNNER_SERVICE -->|"create/query Argo workflow(s) required to execute recipe instructions"|ARGO

%% Style
style OGDC_RUNNER_SERVICE stroke:#ff6666,stroke-width:2px;
style OGDC_RUNNER_CLI stroke:#ff6666,stroke-width:2px;
```

[Please view our main documentation site for context](https://qgreenland-net.github.io).

This component:

- Defines and documents the recipe API(s). See [OGDC Recipes](./recipes.md).
- Defines a CLI published as the `ogdc-runner` pypi Python package that accepts
  a recipe as input and submits it to the service layer.
- Defines a service layer published as the `ghcr.io/qgreenland-net/ogdc-runner`
  docker image that translates submitted OGDC recipes into Argo Workflows for
  execution.

## Aspirational

Original aspirational architecture diagram of the `ogdc-runner` as it relates to
the rest of the OGDC.

```{warning}
This diagram does not reflect the actual, current implementation. This remains for reference only.
```

```{mermaid}
graph LR

%% Definitions
subgraph ADC_K8S[ADC k8s]
  subgraph GHA_SELFHOSTED[GHA self-hosted runner]
    OGDC_RUNNER[ogdc-runner]
  end
  OGDC[OGDC]
end

subgraph RECIPE_REPO[Recipe repo]
  GHA[GitHub Actions]
  RECIPE[Recipe]
  SECRET[Secret token]
end



%% Relationships
OGDC_RUNNER -->|submit| OGDC
GHA_SELFHOSTED -->|status| GHA
GHA -->|trigger| GHA_SELFHOSTED

SECRET -->|read| GHA
RECIPE -->|on change| GHA


%% Style
style OGDC_RUNNER stroke:#ff6666,stroke-width:2px;
```
