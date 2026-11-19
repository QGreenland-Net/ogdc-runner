"""Module containing FastAPI routes requiring an access token."""

from __future__ import annotations

import datetime as dt
import os
import shutil
from typing import cast

import pydantic
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from fastapi.security import HTTPBearer
from loguru import logger

from ogdc_runner.api import submit_ogdc_recipe
from ogdc_runner.argo import get_recipe_workflows_for_order
from ogdc_runner.exceptions import OgdcOutputDataRetrievalError
from ogdc_runner.publish import get_temporary_output_data_url
from ogdc_runner.recipe import stage_ogdc_recipe
from ogdc_runner.service.auth import auth_client
from ogdc_runner.service.db import SessionDependency, get_db_session
from ogdc_runner.service.orders import (
    OrderId,
    OrderStatus,
    create_order,
    get_order_status,
    update_order_status,
)

bearer_schema = HTTPBearer(auto_error=False)

SCOPE_ADMIN = os.getenv("OGDC_SCOPE_ADMIN", "ogdc:admin")

router = APIRouter(
    prefix="/workflows",
    tags=["workflows"],
    # Require that all routes in this module be authenticated via an access
    # token with appropriate scopes
    dependencies=[
        Depends(bearer_schema),
        Depends(auth_client.require_scope(SCOPE_ADMIN)),
    ],
)


class WorkflowSubmitRequest(pydantic.BaseModel):
    recipe_path: str
    overwrite: bool = False


class WorkflowSubmitResponse(pydantic.BaseModel):
    message: str
    order_id: str


def _submit_recipe_and_clean(
    recipe_path: str,
    overwrite: bool,
    order_id: OrderId,
) -> None:
    """Submits the given recipe for execution and then cleans up the staged recipe."""
    staged_recipe_dir = stage_ogdc_recipe(recipe_path)
    try:
        submit_ogdc_recipe(
            recipe_dir=staged_recipe_dir,
            overwrite=overwrite,
            order_id=order_id,
        )
        # TODO: this update could technically happen after "success" is achieved
        # in submission above, although unlikely. Should guard against
        # unexpected status transitions.
        with get_db_session() as session:
            update_order_status(
                order_id=order_id,
                order_status=OrderStatus.RUNNING,
                session=session,
            )
    except Exception as e:
        # Get the order record and update it with "Failed" status.
        with get_db_session() as session:
            update_order_status(
                order_id=order_id,
                order_status=OrderStatus.FAILED,
                session=session,
            )
        raise e
    finally:
        # Always cleanup the staged recipe directory
        shutil.rmtree(staged_recipe_dir)


@router.post(
    "",
    response_model=WorkflowSubmitResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_workflow(
    payload: WorkflowSubmitRequest,
    background_tasks: BackgroundTasks,
    session: SessionDependency,
) -> WorkflowSubmitResponse:
    """Submit a recipe to OGDC for execution.

    Requires a valid auth token and scope.
    """
    try:
        order_id = create_order(session=session)
        # Background task to run the recipe submission logic.
        background_tasks.add_task(
            _submit_recipe_and_clean,
            recipe_path=payload.recipe_path,
            overwrite=payload.overwrite,
            order_id=order_id,
        )

        # return a generic success message immediately
        return WorkflowSubmitResponse(
            message="Recipe submission accepted.",
            order_id=order_id,
        )
    except Exception as e:
        # TODO: try to update the db here if the id was added??
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initiate submission: {e}.",
        ) from e


class WorkflowDetailResponse(pydantic.BaseModel):
    order_id: str
    status: OrderStatus
    timestamp: dt.datetime = pydantic.Field(default_factory=dt.datetime.now)


@router.get(
    "/{order_id}",
    response_model=WorkflowDetailResponse,
)
def get_workflow(
    order_id: str,
    session: SessionDependency,
) -> WorkflowDetailResponse:
    """Check an OGDC workflow's status."""
    status = get_order_status(session=session, order_id=order_id)
    return WorkflowDetailResponse(
        order_id=order_id,
        status=status,
    )


class WorkflowOutputResponse(pydantic.BaseModel):
    data_url: str


@router.get(
    "/{order_id}/output",
    response_model=WorkflowOutputResponse,
)
def get_workflow_output(order_id: str) -> WorkflowOutputResponse:
    """Get a signed `/storage` download link for the outputs of the given workflow.

    The link is minted by `ogdc_runner.storage_links.build_signed_url` and
    expires after `ogdc_runner.storage_links.LINK_TTL_SECONDS` seconds (2 hours).

    TODO: support other output types. This assumes a temporary output stored on
    Argo's artifact s3 storage.
    """
    try:
        order_id = cast(OrderId, order_id)
        recipe_workflows = get_recipe_workflows_for_order(order_id)
        if (
            recipe_workflows
            and len(recipe_workflows) == 1
            and recipe_workflows[0].metadata.name
        ):
            recipe_workflow_name = recipe_workflows[0].metadata.name
            s3_location = get_temporary_output_data_url(
                workflow_name=recipe_workflow_name
            )
        else:
            err_msg = f"Could not find matching workflow output for {order_id=}: {recipe_workflows=}"
            logger.error(err_msg)
            raise OgdcOutputDataRetrievalError(err_msg)
    except OgdcOutputDataRetrievalError as e:
        raise HTTPException(
            status_code=404,
            detail=f"The requested output for {order_id} was not found: {e}.",
        ) from e

    return WorkflowOutputResponse(data_url=s3_location)
