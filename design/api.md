# API Design

This document is to outline the API design for version 1.0.0 of `ogdc-workflows`. The goal of the design is to follow RESTful best practices, including the use of resource-based URIs where endpoints are nouns, and standard HTTP methods (GET, POST, PUT, PATCH, DELETE) represent actions to perform on those nouns.

## Key Definitions

Before getting to the actual API design, key resources and parameters are identified and defined, whether they become endpoints or not. These defnitions can also be used to refine the `meta.yaml` schema.

- **workflow**: a schedulable job that is specified by a specific recipe
- **recipe**: a specific configuration and parameterization of one or more executable templates, desined to be executed as a workflow
- **template**: a re-usable, standalone definition of a task, including configuration, executable, inputs, and outputs
    - **template-type**: one of shell, visualization, or future templates we might add and support with `ogdc-runner` as needed

## API Endpoints

### 1. Workflows (`/workflows`)

- **`POST /workflows`**
    - **Description:** Submits a new workflow for execution. (Replaces `/submit`).
    - **Request Body:** `workflow_path` (str), `overwrite` (bool)
    - **Response:** `message` (str), `recipe_workflow_name` (str)
- **`GET /workflows`**
    - **Description:** Retrieves a list of workflows submitted by the user.
    - **Request Body:** limit, offset
- **`GET /workflows/{recipe_workflow_name}`**
    - **Description:** Retrieves the current status and high-level details of a specific workflow. (Replaces `/status`)
    - **Response:** `recipe_workflow_name` (str), `status` (str), `timestamp`, `config/recipe`
- **`DELETE /workflows/{recipe_workflow_name}`**
    - **Description:** Cleanly cancels a pending or running workflow.
- **`GET /workflows/{recipe_workflow_name}/recipe`**
    - **Description:** Retrieves the exact recipe used to execute the workflow (e.g., `meta.yaml` + `recipe.sh`).
- **`GET /workflows/{recipe_workflow_name}/output`**
    - **Description:** Retrieves access to the workflow's output. (Replaces `/output`)
    - **Response:** `data_url` (str)

### 2. Recipes (`/recipes`)

* **`GET /recipes`**
    - **Description:** Retrieves a list of all available geospatial processing recipes.
    - **Request Body:** limit, offset
* **`GET /recipes/{recipe_name}`**
    - **Description:** Retrieves the metadata, schema, and requirements for a specific recipe.
* **`GET /recipes/{recipe_name}/workflows`**
    - **Description:** Retrieves a history of workflows that have been executed using this specific recipe.
    - **Request Body:** limit, offset

### 3. System

* **`GET /queue`**
    - **Description:** Retrieves a list of pending workflows currently waiting in the Argo queue.
* **`GET /version`**
    - **Description:** Retrieves the current OGDC runner version.

### 4. Authentication (`/auth` or root)

- **`GET /login`**
    - **Description** Initiate the OIDC login flow.
- **`GET /authorize`**
    - **Description** OIDC authorization callback endpoint.
- **`POST /refresh`**
    - **Description** Re-validate the user session and return a new access token using the refresh token.

## Recipe Configuration

With the above in mind, it would be beneficial to also be consistent within the recipe schema.  

Example for recipe-type `shell`

```
name: "Greenland Ice Sheet sea-level contribution"
version: 1.0.0

recipe:
  type: "shell"
  command: "bash -c 'for nc_file in /input_dir/*.nc; do base_name=$(basename "$nc_file" .nc); echo "Processing $nc_file..."; gdal_translate -of GTiff -co COMPRESS=LZW "$nc_file" "/output_dir/${base_name}_epsg3413.tif"; done && echo "Complete! Created $(ls /output_dir/*.tif 2>/dev/null | wc -l) files"'"

inputs:
  datasets:
    - type: "dataone"
      dataset_identifier: "resource_map_doi:10.18739/A29G5GD39"
      filename: "percent_gris_*.nc" 

output:
  type: "temporary"
```

Example for recipe-type `visualization`

```
name: "ADC Visualization workflow"

recipe:
  type: "visualization"

input:
  datasets:
    - type: "url" 
      value: "https://demo.arcticdata.io/tiles/OGDC/QGnet/viz-workflow/ice_basins.gpkg"
  parameters:
    config_file: "config.json"

output:
  type: "local"
```