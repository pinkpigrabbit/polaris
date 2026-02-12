from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .db.tables import pending_trade


@dataclass(frozen=True)
class TemporalContext:
    workflow_id: str | None = None
    run_id: str | None = None
    activity_id: str | None = None


class InvalidTransition(Exception):
    pass


async def read_staging_status(session: AsyncSession, staging_id) -> tuple[str, str]:
    stmt = select(pending_trade.c.status, pending_trade.c.lifecycle).where(pending_trade.c.id == staging_id)
    res = await session.execute(stmt)
    row = res.first()
    if not row:
        raise KeyError("staging_not_found")
    return row[0], row[1]


async def advance_status(
    session: AsyncSession,
    *,
    staging_id,
    from_status: str,
    to_status: str,
    triggered_by: str,
    idempotency_scope: str | None,
    idempotency_key: str | None,
    temporal: TemporalContext,
) -> None:
    stmt = (
        update(pending_trade)
        .where(
            and_(
                pending_trade.c.id == staging_id,
                pending_trade.c.status == from_status,
                pending_trade.c.lifecycle == "active",
            )
        )
        .values(status=to_status, entry_version=pending_trade.c.entry_version + 1)
        .returning(pending_trade.c.id)
    )
    res = await session.execute(stmt)
    updated = res.first()
    if not updated:
        cur_status, cur_lifecycle = await read_staging_status(session, staging_id)
        if cur_lifecycle != "active":
            raise InvalidTransition(f"lifecycle_not_active:{cur_lifecycle}")
        if cur_status == to_status:
            return
        raise InvalidTransition(f"status_mismatch:{cur_status} (expected {from_status})")

    _ = (triggered_by, idempotency_scope, idempotency_key, temporal)
