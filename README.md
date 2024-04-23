# ogdc-runner

```mermaid
graph LR

%% Definitions
subgraph ADC_K8S[ADC Kubernetes]
  subgraph GHA_SELFHOSTED[GHA self-hosted runner]
    OGDC_RUNNER[ogdc-runner]
  end
  OGDC[Open Geospatial Data Cloud]
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

* defines and documents the recipe API(s)
* accepts a recipe as input and submits it to the OGDC for execution

```bash
ogdc-runner /path/to/ogdc-recipes/my-recipe
```

Or:

```bash
ogdc-runner https://github.com/QGreenland-Net/ogdc-recipes/ my-recipe
```


## Implementation notes

* `ogdc-runner` could be a Python program
* The trigger/status interface could be handled (transparently?) by a GitHub Actions
  self-hosted runner.
