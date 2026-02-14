from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.session import get_session
from ..db.tables import (
    instrument,
    instrument_equity,
    instrument_fixedincome,
    instrument_futures,
    instrument_fx,
    instrument_identifier,
    instrument_swap,
    instrument_type_id_rule,
    security_id_type,
    security_type_rule,
)
from .schemas import (
    CreateInstrumentRequest,
    InstrumentResponse,
    UpdateSecurityIdRequest,
)


router = APIRouter(prefix="/instruments", tags=["instruments"])


def _parse_numeric_id(raw: str, *, field: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid_{field}") from exc
    if value <= 0:
        raise HTTPException(status_code=422, detail=f"invalid_{field}")
    return value


def _normalize_currency(raw: str) -> str:
    return raw.strip().upper()


def _normalize_identifier_type(raw: str) -> str:
    return raw.strip().upper()


def _normalize_instrument_type(raw: str) -> str:
    return raw.strip().lower()


def _subtype_name_for_instrument_type(instrument_type: str) -> str | None:
    if instrument_type in {"stock", "equity"}:
        return "equity"
    if instrument_type == "futures":
        return "futures"
    if instrument_type == "fx":
        return "fx"
    if instrument_type == "swap":
        return "swap"
    if instrument_type in {"fixedincome", "bond"}:
        return "fixedincome"
    return None


async def _build_response(session: AsyncSession, *, instrument_id: int) -> InstrumentResponse:
    base = (
        await session.execute(
            select(
                instrument.c.id,
                instrument.c.instrument_type,
                instrument.c.security_id,
                instrument.c.currency,
                instrument.c.full_name,
                instrument.c.short_name,
                instrument.c.security_type,
                instrument_type_id_rule.c.default_id_type_code,
                security_type_rule.c.default_settlement_days,
            )
            .join(
                instrument_type_id_rule,
                instrument_type_id_rule.c.instrument_type == instrument.c.instrument_type,
            )
            .join(
                security_type_rule,
                (security_type_rule.c.security_type == instrument.c.security_type)
                & (security_type_rule.c.currency == instrument.c.currency),
            )
            .where(instrument.c.id == instrument_id)
        )
    ).first()
    if not base:
        raise HTTPException(status_code=404, detail="instrument_not_found")

    identifier_rows = (
        await session.execute(
            select(
                instrument_identifier.c.id_type_code,
                instrument_identifier.c.id_value,
                instrument_identifier.c.is_primary,
            )
            .where(instrument_identifier.c.instrument_id == instrument_id)
            .order_by(instrument_identifier.c.id.asc())
        )
    ).all()

    payload: dict[str, Any] = {
        "instrument_id": str(base[0]),
        "instrument_type": str(base[1]),
        "currency": str(base[3]),
        "full_name": str(base[4]),
        "short_name": str(base[5]),
        "security_type": str(base[6]),
        "security_id": str(base[2]),
        "default_id_type_code": str(base[7]),
        "default_settlement_days": int(base[8]),
        "identifiers": [
            {
                "id_type_code": str(r[0]),
                "id_value": str(r[1]),
                "is_primary": bool(r[2]),
            }
            for r in identifier_rows
        ],
    }

    subtype_name = _subtype_name_for_instrument_type(str(base[1]))
    if subtype_name == "equity":
        row = (
            await session.execute(select(instrument_equity).where(instrument_equity.c.instrument_id == instrument_id))
        ).mappings().first()
        payload["equity"] = None if not row else {
            "shares_outstanding": row["shares_outstanding"],
            "free_float_shares": row["free_float_shares"],
            "share_class": row["share_class"],
            "voting_rights": row["voting_rights"],
            "par_value": row["par_value"],
        }
    elif subtype_name == "futures":
        row = (
            await session.execute(select(instrument_futures).where(instrument_futures.c.instrument_id == instrument_id))
        ).mappings().first()
        payload["futures"] = None if not row else {
            "underlying_instrument_id": None if row["underlying_instrument_id"] is None else str(row["underlying_instrument_id"]),
            "contract_size": row["contract_size"],
            "multiplier": row["multiplier"],
            "tick_size": row["tick_size"],
            "maturity_date": row["maturity_date"],
            "settlement_type": row["settlement_type"],
        }
    elif subtype_name == "fx":
        row = (await session.execute(select(instrument_fx).where(instrument_fx.c.instrument_id == instrument_id))).mappings().first()
        payload["fx"] = None if not row else {
            "base_currency": row["base_currency"],
            "quote_currency": row["quote_currency"],
            "settlement_date": row["settlement_date"],
            "tenor": row["tenor"],
        }
    elif subtype_name == "swap":
        row = (
            await session.execute(select(instrument_swap).where(instrument_swap.c.instrument_id == instrument_id))
        ).mappings().first()
        payload["swap"] = None if not row else {
            "underlying_instrument_id": None if row["underlying_instrument_id"] is None else str(row["underlying_instrument_id"]),
            "effective_date": row["effective_date"],
            "maturity_date": row["maturity_date"],
            "notional_currency": row["notional_currency"],
            "notional_amount": row["notional_amount"],
            "fixed_rate": row["fixed_rate"],
            "floating_rate_index": row["floating_rate_index"],
            "payment_frequency": row["payment_frequency"],
        }
    elif subtype_name == "fixedincome":
        row = (
            await session.execute(select(instrument_fixedincome).where(instrument_fixedincome.c.instrument_id == instrument_id))
        ).mappings().first()
        payload["fixedincome"] = None if not row else {
            "issue_date": row["issue_date"],
            "maturity_date": row["maturity_date"],
            "coupon_rate": row["coupon_rate"],
            "coupon_frequency": row["coupon_frequency"],
            "day_count_convention": row["day_count_convention"],
            "face_value": row["face_value"],
            "issuer_name": row["issuer_name"],
        }

    return InstrumentResponse.model_validate(payload)


@router.post("", response_model=InstrumentResponse)
async def create_instrument(
    body: CreateInstrumentRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    instrument_type = _normalize_instrument_type(body.instrument_type)
    currency = _normalize_currency(body.currency)
    security_type = body.security_type.strip()

    default_id_type = (
        await session.execute(
            select(instrument_type_id_rule.c.default_id_type_code).where(
                instrument_type_id_rule.c.instrument_type == instrument_type
            )
        )
    ).scalar_one_or_none()
    if not default_id_type:
        raise HTTPException(status_code=400, detail="instrument_type_rule_not_found")
    default_id_type = _normalize_identifier_type(str(default_id_type))

    security_type_rule_row = (
        await session.execute(
            select(security_type_rule.c.default_settlement_days).where(
                (security_type_rule.c.security_type == security_type) & (security_type_rule.c.currency == currency)
            )
        )
    ).scalar_one_or_none()
    if security_type_rule_row is None:
        raise HTTPException(status_code=400, detail="security_type_rule_not_found")

    identifiers = [
        (
            _normalize_identifier_type(item.id_type_code),
            item.id_value.strip(),
            bool(item.is_primary),
        )
        for item in body.identifiers
    ]

    seen_types: set[str] = set()
    primary_count = 0
    default_id_value: str | None = None
    for id_type_code, id_value, is_primary in identifiers:
        if id_type_code in seen_types:
            raise HTTPException(status_code=400, detail="duplicate_identifier_type")
        seen_types.add(id_type_code)
        if is_primary:
            primary_count += 1
        if id_type_code == default_id_type:
            default_id_value = id_value

    normalized_identifiers = identifiers
    if default_id_value is None:
        raise HTTPException(status_code=400, detail="default_identifier_missing")
    if primary_count > 1:
        raise HTTPException(status_code=400, detail="multiple_primary_identifiers")
    if primary_count == 0:
        normalized_identifiers = [
            (id_type_code, id_value, id_type_code == default_id_type)
            for id_type_code, id_value, _ in identifiers
        ]

    existing_id_types = {
        str(row[0])
        for row in (
            await session.execute(select(security_id_type.c.code).where(security_id_type.c.code.in_(list(seen_types))))
        ).all()
    }
    if existing_id_types != seen_types:
        raise HTTPException(status_code=400, detail="unknown_identifier_type")

    security_id = body.security_id.strip() if body.security_id else default_id_value
    now = datetime.now(tz=timezone.utc)
    instrument_insert = await session.execute(
        instrument.insert()
        .values(
            instrument_type=instrument_type,
            security_id=security_id,
            name=body.short_name,
            full_name=body.full_name.strip(),
            short_name=body.short_name.strip(),
            security_type=security_type,
            currency=currency,
            lifecycle="active",
            created_at=now,
            updated_at=now,
        )
        .returning(instrument.c.id)
    )
    instrument_id = int(instrument_insert.scalar_one())

    for id_type_code, id_value, is_primary in normalized_identifiers:
        await session.execute(
            instrument_identifier.insert().values(
                instrument_id=instrument_id,
                id_type_code=id_type_code,
                id_value=id_value,
                is_primary=is_primary,
                created_at=now,
                updated_at=now,
            )
        )

    subtype_name = _subtype_name_for_instrument_type(instrument_type)
    if subtype_name == "equity":
        if body.equity is None:
            raise HTTPException(status_code=400, detail="equity_attributes_required")
        subtype_payload = body.equity.model_dump(exclude_none=True)
        await session.execute(instrument_equity.insert().values(instrument_id=instrument_id, **subtype_payload))
    elif subtype_name == "futures":
        if body.futures is None:
            raise HTTPException(status_code=400, detail="futures_attributes_required")
        subtype_payload = body.futures.model_dump(exclude_none=True)
        if "underlying_instrument_id" in subtype_payload:
            subtype_payload["underlying_instrument_id"] = _parse_numeric_id(
                subtype_payload["underlying_instrument_id"],
                field="underlying_instrument_id",
            )
        await session.execute(instrument_futures.insert().values(instrument_id=instrument_id, **subtype_payload))
    elif subtype_name == "fx":
        if body.fx is None:
            raise HTTPException(status_code=400, detail="fx_attributes_required")
        subtype_payload = body.fx.model_dump(exclude_none=True)
        subtype_payload["base_currency"] = _normalize_currency(subtype_payload["base_currency"])
        subtype_payload["quote_currency"] = _normalize_currency(subtype_payload["quote_currency"])
        await session.execute(instrument_fx.insert().values(instrument_id=instrument_id, **subtype_payload))
    elif subtype_name == "swap":
        if body.swap is None:
            raise HTTPException(status_code=400, detail="swap_attributes_required")
        subtype_payload = body.swap.model_dump(exclude_none=True)
        if "underlying_instrument_id" in subtype_payload:
            subtype_payload["underlying_instrument_id"] = _parse_numeric_id(
                subtype_payload["underlying_instrument_id"],
                field="underlying_instrument_id",
            )
        if "notional_currency" in subtype_payload:
            subtype_payload["notional_currency"] = _normalize_currency(subtype_payload["notional_currency"])
        await session.execute(instrument_swap.insert().values(instrument_id=instrument_id, **subtype_payload))
    elif subtype_name == "fixedincome":
        if body.fixedincome is None:
            raise HTTPException(status_code=400, detail="fixedincome_attributes_required")
        subtype_payload = body.fixedincome.model_dump(exclude_none=True)
        await session.execute(instrument_fixedincome.insert().values(instrument_id=instrument_id, **subtype_payload))

    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail="instrument_conflict") from exc

    return await _build_response(session, instrument_id=instrument_id)


@router.get("/{instrument_id}", response_model=InstrumentResponse)
async def get_instrument(
    instrument_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    iid = _parse_numeric_id(instrument_id, field="instrument_id")
    return await _build_response(session, instrument_id=iid)


@router.patch("/{instrument_id}/security-id", response_model=InstrumentResponse)
async def update_security_id(
    instrument_id: str,
    body: UpdateSecurityIdRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    iid = _parse_numeric_id(instrument_id, field="instrument_id")
    updated = await session.execute(
        update(instrument)
        .where(instrument.c.id == iid)
        .values(security_id=body.security_id.strip(), updated_at=datetime.now(tz=timezone.utc))
        .returning(instrument.c.id)
    )
    if not updated.first():
        raise HTTPException(status_code=404, detail="instrument_not_found")

    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail="security_id_conflict") from exc

    return await _build_response(session, instrument_id=iid)
