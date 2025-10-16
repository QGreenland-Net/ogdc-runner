# Architecture

Aspirational architecture diagram of the `ogdc-runner` as it relates to the rest
of the OGDC.

```{warning}
This diagram does not reflect the actual, current implementation. It needs to be updated!
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

[Please view our main documentation site for context](https://qgreenland-net.github.io).

This component:

- defines and documents the recipe API(s)
- accepts a recipe as input and submits it to the OGDC for execution
