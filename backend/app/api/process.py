from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.tables import txn_staging
from ..db.session import get_session
from ..idempotency import claim_idempotency, get_idempotent_response, store_idempotent_response
from ..settings import settings
from ..temporal.client import get_temporal_client
from ..temporal.workflows import StagingTransactionWorkflow


router = APIRouter(prefix="/staging-transactions", tags=["staging-transactions"])


def _parse_numeric_id(raw: str, *, field: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid_{field}") from exc
    if value <= 0:
        raise HTTPException(status_code=422, detail=f"invalid_{field}")
    return value


async def _start_staging_workflow(*, staging_id: str):
    client = await get_temporal_client()
    workflow_id = f"staging-{staging_id}"
    handle = await client.start_workflow(
        StagingTransactionWorkflow.run,
        staging_id,
        id=workflow_id,
        task_queue=settings.temporal_task_queue,
    )
    return workflow_id, handle.run_id


@router.post("/{staging_id}/process")
async def process_staging_transaction(
    staging_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
):
    sid_int = _parse_numeric_id(staging_id, field="staging_id")
    sid = str(sid_int)
    scope = f"api:process_staging:{sid}"
    if idempotency_key:
        cached = await get_idempotent_response(session, scope=scope, key=idempotency_key)
        if cached:
            return cached
        claimed = await claim_idempotency(session, scope=scope, key=idempotency_key, request_payload={"staging_id": sid})
        if not claimed:
            cached = await get_idempotent_response(session, scope=scope, key=idempotency_key)
            if cached:
                return cached

    try:
        workflow_id, run_id = await _start_staging_workflow(staging_id=sid)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"temporal_start_failed:{type(e).__name__}")

    resp = {"workflow_id": workflow_id, "run_id": run_id}
    if idempotency_key:
        await store_idempotent_response(session, scope=scope, key=idempotency_key, response_payload=resp)
        await session.commit()
    return resp


@router.post("/deals/{block_staging_id}/process")
async def process_deal_allocations(
    block_staging_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
):
    block_sid = _parse_numeric_id(block_staging_id, field="block_staging_id")
    block_row = (
        await session.execute(
            select(txn_staging.c.id, txn_staging.c.level, txn_staging.c.deal_block_id).where(txn_staging.c.id == block_sid)
        )
    ).first()
    if not block_row:
        raise HTTPException(status_code=404, detail="block_staging_not_found")
    if block_row[1] != "block":
        raise HTTPException(status_code=409, detail="not_block_staging")
    if block_row[2] is None:
        raise HTTPException(status_code=409, detail="block_deal_id_missing")

    normalized_block_sid = str(block_sid)
    scope = f"api:process_deal:{normalized_block_sid}"
    if idempotency_key:
        cached = await get_idempotent_response(session, scope=scope, key=idempotency_key)
        if cached:
            return cached
        claimed = await claim_idempotency(
            session,
            scope=scope,
            key=idempotency_key,
            request_payload={"block_staging_id": normalized_block_sid},
        )
        if not claimed:
            cached = await get_idempotent_response(session, scope=scope, key=idempotency_key)
            if cached:
                return cached

    allocation_rows = (
        await session.execute(
            select(txn_staging.c.id)
            .where(
                txn_staging.c.deal_block_id == block_row[2],
                txn_staging.c.level == "allocation",
                txn_staging.c.status == "entry",
                txn_staging.c.lifecycle == "active",
            )
            .order_by(txn_staging.c.created_at.asc())
        )
    ).all()
    if not allocation_rows:
        raise HTTPException(status_code=409, detail="allocation_staging_not_found")

    started: list[dict[str, str | None]] = []
    for row in allocation_rows:
        sid = str(row[0])
        try:
            workflow_id, run_id = await _start_staging_workflow(staging_id=sid)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"temporal_start_failed:{type(e).__name__}")
        started.append({"staging_id": sid, "workflow_id": workflow_id, "run_id": run_id})

    resp = {
        "block_staging_id": normalized_block_sid,
        "deal_block_id": str(block_row[2]),
        "started": started,
    }
    if idempotency_key:
        await store_idempotent_response(session, scope=scope, key=idempotency_key, response_payload=resp)
        await session.commit()
    return resp
