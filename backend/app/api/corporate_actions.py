from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.session import get_session
from ..db.tables import ca_election, ca_event
from ..settings import settings
from ..temporal.client import get_temporal_client
from ..temporal.workflows import CorporateActionWorkflow

from temporalio.common import WorkflowIDReusePolicy


router = APIRouter(prefix="/corporate-actions", tags=["corporate-actions"])


def _parse_numeric_id(raw: str, *, field: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid_{field}") from exc
    if value <= 0:
        raise HTTPException(status_code=422, detail=f"invalid_{field}")
    return value


class CreateCaEventRequest(BaseModel):
    ca_type: Literal["cash_dividend", "stock_split"]
    instrument_id: str
    ex_date: date
    record_date: date | None = None
    pay_date: date | None = None
    currency: str | None = Field(default=None, min_length=3, max_length=3)
    cash_amount_per_share: Decimal | None = None
    split_numerator: Decimal | None = None
    split_denominator: Decimal | None = None
    require_election: bool = False


class CreateElectionRequest(BaseModel):
    portfolio_id: str
    choice: Literal["accept", "decline"]


@router.post("")
async def create_ca_event(
    body: CreateCaEventRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    now = datetime.now(tz=timezone.utc)
    eid = await session.execute(
        ca_event.insert()
        .values(
            ca_type=body.ca_type,
            instrument_id=_parse_numeric_id(body.instrument_id, field="instrument_id"),
            ex_date=body.ex_date,
            record_date=body.record_date,
            pay_date=body.pay_date,
            currency=body.currency,
            cash_amount_per_share=body.cash_amount_per_share,
            split_numerator=body.split_numerator,
            split_denominator=body.split_denominator,
            status="announced",
            require_election=body.require_election,
            lifecycle="active",
            created_at=now,
            updated_at=now,
        )
        .returning(ca_event.c.id)
    )
    event_id = eid.scalar_one()
    await session.commit()
    return {"ca_event_id": str(event_id), "status": "announced"}


@router.get("/{ca_event_id}")
async def get_ca_event(ca_event_id: str, session: Annotated[AsyncSession, Depends(get_session)]):
    eid = _parse_numeric_id(ca_event_id, field="ca_event_id")
    ev = (await session.execute(select(ca_event).where(ca_event.c.id == eid))).mappings().first()
    if not ev:
        raise HTTPException(status_code=404, detail="not_found")
    return {
        "ca_event_id": ca_event_id,
        "ca_type": ev["ca_type"],
        "instrument_id": str(ev["instrument_id"]),
        "ex_date": ev["ex_date"].isoformat(),
        "pay_date": ev["pay_date"].isoformat() if ev["pay_date"] else None,
        "currency": ev["currency"],
        "status": ev["status"],
        "require_election": bool(ev["require_election"]),
    }


@router.post("/{ca_event_id}/elections")
async def create_ca_election(
    ca_event_id: str,
    body: CreateElectionRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
    actor: Annotated[str | None, Header(alias="X-Actor")] = None,
):
    eid = _parse_numeric_id(ca_event_id, field="ca_event_id")
    now = datetime.now(tz=timezone.utc)
    stmt = (
        insert(ca_election)
        .values(
            ca_event_id=eid,
            portfolio_id=_parse_numeric_id(body.portfolio_id, field="portfolio_id"),
            choice=body.choice,
            actor=actor,
            elected_at=now,
        )
        .on_conflict_do_update(
            index_elements=[ca_election.c.ca_event_id, ca_election.c.portfolio_id],
            set_={"choice": body.choice, "actor": actor, "elected_at": now},
        )
    )
    await session.execute(stmt)
    await session.commit()
    return {"ca_event_id": ca_event_id, "portfolio_id": body.portfolio_id, "choice": body.choice}


@router.post("/{ca_event_id}/process")
async def process_ca_event(ca_event_id: str, session: Annotated[AsyncSession, Depends(get_session)]):
    eid_int = _parse_numeric_id(ca_event_id, field="ca_event_id")
    eid = str(eid_int)
    exists = (await session.execute(select(ca_event.c.id).where(ca_event.c.id == eid_int))).scalar_one_or_none()
    if not exists:
        raise HTTPException(status_code=404, detail="not_found")

    client = await get_temporal_client()
    workflow_id = f"ca-{eid}"
    try:
        handle = await client.start_workflow(
            CorporateActionWorkflow.run,
            eid,
            id=workflow_id,
            task_queue=settings.temporal_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"temporal_start_failed:{type(e).__name__}")
    return {"workflow_id": workflow_id, "run_id": handle.run_id}
