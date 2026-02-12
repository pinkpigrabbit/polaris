from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .db.tables import idempotency_record


def _hash_payload(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


async def get_idempotent_response(session: AsyncSession, *, scope: str, key: str) -> Any | None:
    stmt = select(idempotency_record.c.response).where(
        idempotency_record.c.scope == scope,
        idempotency_record.c.key == key,
    )
    res = await session.execute(stmt)
    row = res.first()
    return row[0] if row else None


async def claim_idempotency(
    session: AsyncSession,
    *,
    scope: str,
    key: str,
    request_payload: Any | None,
) -> bool:
    req_hash = _hash_payload(request_payload) if request_payload is not None else None
    stmt = (
        insert(idempotency_record)
        .values(scope=scope, key=key, request_hash=req_hash)
        .on_conflict_do_nothing(index_elements=[idempotency_record.c.scope, idempotency_record.c.key])
    )
    res = await session.execute(stmt)
    return (res.rowcount or 0) > 0


async def store_idempotent_response(
    session: AsyncSession,
    *,
    scope: str,
    key: str,
    response_payload: Any,
) -> None:
    stmt = (
        insert(idempotency_record)
        .values(scope=scope, key=key, response=response_payload)
        .on_conflict_do_update(
            index_elements=[idempotency_record.c.scope, idempotency_record.c.key],
            set_={"response": response_payload},
        )
    )
    await session.execute(stmt)
