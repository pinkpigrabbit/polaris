from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.session import get_session
from ..db.tables import abor_nav_result, abor_nav_run, portfolio
from ..idempotency import claim_idempotency, get_idempotent_response, store_idempotent_response
from ..nav.service import compute_ibor_nav, persist_ibor_nav_run
from ..redis_cache import redis_cache
from ..settings import settings
from ..temporal.client import get_temporal_client
from ..temporal.workflows import AborNavWorkflow

from temporalio.common import WorkflowIDReusePolicy


def _dstr(v) -> str:
    s = format(v, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


router = APIRouter(prefix="/nav", tags=["nav"])


def _parse_numeric_id(raw: str, *, field: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid_{field}") from exc
    if value <= 0:
        raise HTTPException(status_code=422, detail=f"invalid_{field}")
    return value


class AborRunRequest(BaseModel):
    asof_date: date


@router.get("/ibor/{portfolio_id}")
async def get_ibor_nav(
    portfolio_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    pid = _parse_numeric_id(portfolio_id, field="portfolio_id")
    rc = (await session.execute(select(portfolio.c.report_currency).where(portfolio.c.id == pid))).scalar_one_or_none()
    if not rc:
        raise HTTPException(status_code=404, detail="portfolio_not_found")

    payload = await compute_ibor_nav(session, portfolio_id=pid, report_currency=str(rc))
    await redis_cache.set_ibor_nav(portfolio_id=str(pid), payload=payload)
    return payload


@router.post("/ibor/{portfolio_id}/snapshot")
async def snapshot_ibor_nav(
    portfolio_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
):
    pid = _parse_numeric_id(portfolio_id, field="portfolio_id")
    rc = (await session.execute(select(portfolio.c.report_currency).where(portfolio.c.id == pid))).scalar_one_or_none()
    if not rc:
        raise HTTPException(status_code=404, detail="portfolio_not_found")

    scope = f"api:ibor_snapshot:{pid}"
    if idempotency_key:
        cached = await get_idempotent_response(session, scope=scope, key=idempotency_key)
        if cached:
            return cached
        claimed = await claim_idempotency(session, scope=scope, key=idempotency_key, request_payload={"portfolio_id": str(pid)})
        if not claimed:
            cached = await get_idempotent_response(session, scope=scope, key=idempotency_key)
            if cached:
                return cached

    asof_ts = datetime.now(tz=timezone.utc)
    payload = await compute_ibor_nav(session, portfolio_id=pid, report_currency=str(rc), asof_ts=asof_ts)
    run_id = await persist_ibor_nav_run(
        session,
        run_type="snapshot",
        portfolio_id=pid,
        asof_ts=asof_ts,
        asof_date=asof_ts.date(),
        report_currency=str(rc),
        through_acct_transaction_id=None,
        nav_payload=payload,
        idempotency_scope=scope,
        idempotency_key=idempotency_key,
    )
    await session.commit()

    resp = {"nav_run_id": str(run_id)}
    if idempotency_key:
        await store_idempotent_response(session, scope=scope, key=idempotency_key, response_payload=resp)
        await session.commit()
    return resp


@router.post("/abor/{portfolio_id}/run")
async def run_abor_nav(
    portfolio_id: str,
    body: AborRunRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    pid_int = _parse_numeric_id(portfolio_id, field="portfolio_id")
    pid = str(pid_int)
    # Validate portfolio exists
    exists = (await session.execute(select(portfolio.c.id).where(portfolio.c.id == pid_int))).scalar_one_or_none()
    if not exists:
        raise HTTPException(status_code=404, detail="portfolio_not_found")

    asof_date = body.asof_date.isoformat()
    client = await get_temporal_client()
    workflow_id = f"abor-nav-{pid}-{asof_date}"
    try:
        handle = await client.start_workflow(
            AborNavWorkflow.run,
            args=[pid, asof_date],
            id=workflow_id,
            task_queue=settings.temporal_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"temporal_start_failed:{type(e).__name__}")

    return {"workflow_id": workflow_id, "run_id": handle.first_execution_run_id}


@router.get("/abor/{portfolio_id}/result")
async def get_abor_nav_result(
    portfolio_id: str,
    asof_date: date,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    pid = _parse_numeric_id(portfolio_id, field="portfolio_id")
    run_row = (
        await session.execute(
            select(abor_nav_run.c.id)
            .where(
                abor_nav_run.c.portfolio_id == pid,
                abor_nav_run.c.run_type == "eod",
                abor_nav_run.c.asof_date == asof_date,
                abor_nav_run.c.status == "completed",
            )
            .order_by(abor_nav_run.c.completed_at.desc())
            .limit(1)
        )
    ).first()
    if not run_row:
        raise HTTPException(status_code=404, detail="nav_not_found")

    nav_rc = (
        await session.execute(select(abor_nav_result.c.nav_rc).where(abor_nav_result.c.abor_nav_run_id == run_row[0]))
    ).scalar_one()
    return {"nav_run_id": str(run_row[0]), "nav_rc": _dstr(nav_rc)}
