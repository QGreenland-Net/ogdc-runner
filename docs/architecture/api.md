# OGDC API v1 Design Guide

This document is to outline the API design for version 1.0.0 of
`ogdc-workflows`. The goal of the design is to follow RESTful best practices,
including the use of resource-based URIs where endpoints are nouns, and standard
HTTP methods (GET, POST, PUT, PATCH, DELETE) represent actions to perform on
those nouns.

This API is a work in progress.

## Key Definitions

- **workflow**: a schedulable job that is specified by a specific recipe
- **recipe**: a specific configuration and parameterization of one or more
  executable templates, designed to be executed as a workflow
- **template**: a reusable, standalone definition of a task, including
  configuration, executable, inputs, and outputs
  - **template-type**: one of shell, visualization, or future templates we might
    add and support with `ogdc-runner` as needed

## API Endpoints

### 1. Workflows (`/workflows`)

#### Implemented:

- **`POST /workflows`**
  - **Description:** Submits a new workflow for execution. (Replaces `/submit`).
  - **Request Body:** `recipe_path` (str), `overwrite` (bool)
  - **Response:** `message` (str), `recipe_workflow_name` (str)
- **`GET /workflows/{recipe_workflow_name}`**
  - **Description:** Retrieves the current status and high-level details of a
    specific workflow. (Replaces `/status`)
  - **Response:** `recipe_workflow_name` (str), `status` (str), `timestamp`,
    `config/recipe`
- **`GET /workflows/{recipe_workflow_name}/output`**
  - **Description:** Retrieves access to the workflow's output. (Replaces
    `/output`)
  - **Response:** `data_url` (str)

#### Planned:

- **`GET /workflows`**
  - **Description:** Retrieves a list of workflows submitted by the user.
  - **Request Body:** limit, offset
- **`DELETE /workflows/{recipe_workflow_name}`**
  - **Description:** Cleanly cancels a pending or running workflow.
- **`GET /workflows/{recipe_workflow_name}/recipe`**
  - **Description:** Retrieves the exact recipe used to execute the workflow
    (e.g., `meta.yaml` + `recipe.sh`).

### 2. Recipes (`/recipes`)

#### Planned:

- **`GET /recipes`**
  - **Description:** Retrieves a list of all available geospatial processing
    recipes.
  - **Request Body:** limit, offset
- **`GET /recipes/{recipe_name}`**
  - **Description:** Retrieves the metadata, schema, and requirements for a
    specific recipe.
- **`GET /recipes/{recipe_name}/workflows`**
  - **Description:** Retrieves a history of workflows that have been executed
    using this specific recipe.
  - **Request Body:** limit, offset

### 3. System

#### Implemented:

- **`GET /version`**
  - **Description:** Retrieves the current OGDC runner version.

#### Planned:

- **`GET /queue`**
  - **Description:** Retrieves a list of pending workflows currently waiting in
    the Argo queue.

### 4. Authentication (`/auth` or root)

#### Implemented:

- **`GET /login`**
  - **Description** Initiate the OIDC login flow.
- **`GET /authorize`**
  - **Description** OIDC authorization callback endpoint.
- **`POST /refresh`**
  - **Description** Re-validate the user session and return a new access token
    using the refresh token.

## Potential Recipe Configuration

With the above service API in mind, it would be beneficial to also be consistent
within the recipe schema. See
[Issue #172](https://github.com/QGreenland-Net/ogdc-runner/issues/172) for ideas
on how we could update the recipe configuration to match the service API
definitions.

The current recipe configuration is documented in [OGDC Recipes](../recipes.md).
