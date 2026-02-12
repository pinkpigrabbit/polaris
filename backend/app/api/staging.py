from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.session import get_session
from ..db.tables import instrument, pending_trade, pending_trade_change, portfolio
from ..idempotency import claim_idempotency, get_idempotent_response, store_idempotent_response
from .schemas import (
    CreateDealStagingRequest,
    CreateDealStagingResponse,
    CreateStagingRequest,
    DealAdjustmentResponse,
    DealAllocationStagingResponse,
    ModifyDealRequest,
    StagingResponse,
    UpdateStagingRequest,
)


router = APIRouter(prefix="/staging-transactions", tags=["staging-transactions"])

# Compatibility aliases during table rename migration.
txn_staging = pending_trade
txn_staging_change = pending_trade_change

_MONEY_SCALE = Decimal("0.01")
_TRANSACTION_SIGN_BY_TYPE = {
    "BUY": Decimal("1"),
    "SELL": Decimal("-1"),
    "BuyEquity": Decimal("1"),
    "SellEquity": Decimal("-1"),
}


def _parse_numeric_id(raw: str, *, field: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid_{field}") from exc
    if value <= 0:
        raise HTTPException(status_code=422, detail=f"invalid_{field}")
    return value


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(_MONEY_SCALE, rounding=ROUND_HALF_UP)


def _dstr(value: Decimal) -> str:
    s = format(value, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _compute_block_and_allocation_amounts(*, abs_quantities: list[Decimal], price: Decimal) -> tuple[Decimal, list[Decimal], int | None]:
    if not abs_quantities:
        return Decimal("0"), [], None

    total_abs_qty = sum(abs_quantities, Decimal("0"))
    block_amount_qc = _round_money(total_abs_qty * price)
    allocation_raw_amounts = [qty * price for qty in abs_quantities]
    allocation_amounts_qc = [_round_money(v) for v in allocation_raw_amounts]
    residual_qc = block_amount_qc - sum(allocation_amounts_qc, Decimal("0"))

    rounding_adjustment_index: int | None = None
    if residual_qc != 0:
        rounding_adjustment_index = max(range(len(allocation_raw_amounts)), key=lambda idx: abs(allocation_raw_amounts[idx]))
        allocation_amounts_qc[rounding_adjustment_index] = allocation_amounts_qc[rounding_adjustment_index] + residual_qc

    return block_amount_qc, allocation_amounts_qc, rounding_adjustment_index


async def _create_deal_adjustment_stagings(
    session: AsyncSession,
    *,
    deal_block_id: int,
    instrument_id: int,
    trade_date,
    settle_date,
    quote_currency: str,
    price: Decimal,
    target_qty_by_portfolio: dict[int, Decimal],
    mark_deleted: bool,
    force_reversal_replacement: bool = False,
) -> DealAdjustmentResponse:
    now = datetime.now(tz=timezone.utc)
    current_rows = (
        await session.execute(
            text(
                """
                SELECT portfolio_id, COALESCE(SUM(quantity), 0) AS quantity
                FROM deal_allocation
                WHERE block_id = :block_id
                  AND lifecycle = 'active'
                GROUP BY portfolio_id
                """
            ),
            {"block_id": deal_block_id},
        )
    ).all()
    current_qty_by_portfolio = {int(r[0]): Decimal(r[1]) for r in current_rows}

    all_portfolios = set(current_qty_by_portfolio.keys()) | set(target_qty_by_portfolio.keys())
    delta_by_portfolio: dict[int, Decimal] = {}
    for pid in all_portfolios:
        delta = target_qty_by_portfolio.get(pid, Decimal("0")) - current_qty_by_portfolio.get(pid, Decimal("0"))
        if delta != 0:
            delta_by_portfolio[pid] = delta

    if mark_deleted or force_reversal_replacement:
        await session.execute(
            text(
                """
                UPDATE deal_allocation
                SET lifecycle = 'deleted', updated_at = :updated_at
                WHERE block_id = :block_id
                  AND lifecycle = 'active'
                """
            ),
            {"block_id": deal_block_id, "updated_at": now},
        )

    allocation_plan: list[tuple[int, Decimal, str | None]] = []
    if force_reversal_replacement and not mark_deleted:
        for pid in sorted(current_qty_by_portfolio.keys(), key=lambda v: str(v)):
            qty = current_qty_by_portfolio[pid]
            if qty != 0:
                allocation_plan.append((pid, -qty, "modify_reversal"))
        for pid in sorted(target_qty_by_portfolio.keys(), key=lambda v: str(v)):
            qty = target_qty_by_portfolio[pid]
            if qty != 0:
                allocation_plan.append((pid, qty, "modify_replacement"))
    else:
        for pid in sorted(delta_by_portfolio.keys(), key=lambda v: str(v)):
            source_system = "delete_reversal" if mark_deleted else None
            allocation_plan.append((pid, delta_by_portfolio[pid], source_system))

    abs_quantities = [abs(item[1]) for item in allocation_plan]
    block_amount_qc, allocation_amounts_qc, rounding_adjustment_index = _compute_block_and_allocation_amounts(
        abs_quantities=abs_quantities,
        price=price,
    )
    block_delta_quantity = sum((item[1] for item in allocation_plan), Decimal("0"))

    ordered_portfolios = [item[0] for item in allocation_plan]

    report_currency_by_portfolio = {
        row[0]: str(row[1])
        for row in (
            await session.execute(select(portfolio.c.id, portfolio.c.report_currency).where(portfolio.c.id.in_(ordered_portfolios)))
        ).all()
    }

    block_staging_insert = await session.execute(
        txn_staging.insert()
        .values(
            level="block",
            deal_block_id=deal_block_id,
            deal_allocation_id=None,
            portfolio_id=None,
            instrument_id=instrument_id,
            trade_date=trade_date,
            settle_date=settle_date,
            quantity=block_delta_quantity,
            price=price,
            quote_currency=quote_currency,
            report_currency=quote_currency,
            qc_gross_amount=block_amount_qc,
            rc_gross_amount=block_amount_qc,
            status="entry",
            lifecycle="active",
            entry_version=1,
            created_at=now,
            updated_at=now,
        )
        .returning(txn_staging.c.id)
    )
    block_staging_id = block_staging_insert.scalar_one()

    allocation_stagings: list[DealAllocationStagingResponse] = []
    allocation_lifecycle = "deleted" if mark_deleted else "active"
    for idx, (pid, delta_qty, source_system) in enumerate(allocation_plan):
        row_lifecycle = "deleted" if source_system == "modify_reversal" else allocation_lifecycle
        alloc_insert = await session.execute(
            text(
                """
                INSERT INTO deal_allocation(
                  block_id,
                  portfolio_id,
                  quantity,
                  price,
                  is_rounding_adjustment,
                  lifecycle,
                  created_at,
                  updated_at
                )
                VALUES (
                  :block_id,
                  :portfolio_id,
                  :quantity,
                  :price,
                  :is_rounding_adjustment,
                  :lifecycle,
                  :created_at,
                  :updated_at
                )
                RETURNING id
                """
            ),
            {
                "block_id": deal_block_id,
                "portfolio_id": str(pid),
                "quantity": delta_qty,
                "price": price,
                "is_rounding_adjustment": rounding_adjustment_index == idx,
                "lifecycle": row_lifecycle,
                "created_at": now,
                "updated_at": now,
            },
        )
        deal_allocation_id = alloc_insert.scalar_one()

        report_currency = report_currency_by_portfolio.get(pid)
        if report_currency is None:
            raise HTTPException(status_code=404, detail="portfolio_not_found")
        allocation_amount = allocation_amounts_qc[idx]
        alloc_staging_insert = await session.execute(
            txn_staging.insert()
            .values(
                level="allocation",
                deal_block_id=deal_block_id,
                deal_allocation_id=deal_allocation_id,
                portfolio_id=pid,
                instrument_id=instrument_id,
                trade_date=trade_date,
                settle_date=settle_date,
                quantity=delta_qty,
                price=price,
                quote_currency=quote_currency,
                report_currency=report_currency,
                qc_gross_amount=allocation_amount,
                rc_gross_amount=allocation_amount if quote_currency == report_currency else None,
                status="entry",
                lifecycle="active",
                source_system=source_system,
                entry_version=1,
                created_at=now,
                updated_at=now,
            )
            .returning(txn_staging.c.id)
        )
        allocation_staging_id = alloc_staging_insert.scalar_one()
        allocation_stagings.append(
            DealAllocationStagingResponse(
                portfolio_id=str(pid),
                quantity=_dstr(delta_qty),
                amount_qc=_dstr(allocation_amount),
                staging_id=str(allocation_staging_id),
            )
        )

    target_block_quantity = sum(target_qty_by_portfolio.values(), Decimal("0"))
    await session.execute(
        text(
            """
            UPDATE deal_block
            SET quantity = :quantity,
                lifecycle = :lifecycle,
                updated_at = :updated_at
            WHERE id = :block_id
            """
        ),
        {
            "quantity": Decimal("0") if mark_deleted else target_block_quantity,
            "lifecycle": "deleted" if mark_deleted else "active",
            "updated_at": now,
            "block_id": deal_block_id,
        },
    )

    await session.commit()
    return DealAdjustmentResponse(
        block_staging_id=str(block_staging_id),
        deal_block_id=str(deal_block_id),
        block_delta_quantity=_dstr(block_delta_quantity),
        block_amount_qc=_dstr(block_amount_qc),
        allocation_stagings=allocation_stagings,
    )


@router.post("", response_model=StagingResponse)
async def create_staging_transaction(
    body: CreateStagingRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
):
    scope = "api:create_staging"
    if idempotency_key:
        cached = await get_idempotent_response(session, scope=scope, key=idempotency_key)
        if cached:
            return cached
        claimed = await claim_idempotency(session, scope=scope, key=idempotency_key, request_payload=body.model_dump())
        if not claimed:
            cached = await get_idempotent_response(session, scope=scope, key=idempotency_key)
            if cached:
                return cached

    now = datetime.now(tz=timezone.utc)
    insert_stmt = txn_staging.insert().values(
        level=body.level,
        portfolio_id=_parse_numeric_id(body.portfolio_id, field="portfolio_id") if body.portfolio_id else None,
        instrument_id=_parse_numeric_id(body.instrument_id, field="instrument_id"),
        trade_date=body.trade_date,
        settle_date=body.settle_date,
        quantity=body.quantity,
        price=body.price,
        quote_currency=body.quote_currency,
        report_currency=body.report_currency,
        status="entry",
        lifecycle="active",
        entry_version=1,
        created_at=now,
        updated_at=now,
    ).returning(txn_staging.c.id, txn_staging.c.status, txn_staging.c.lifecycle, txn_staging.c.entry_version)

    res = await session.execute(insert_stmt)
    row = res.first()
    await session.commit()
    if not row:
        raise HTTPException(status_code=500, detail="insert_failed")

    resp = StagingResponse(id=str(row[0]), status=row[1], lifecycle=row[2], entry_version=row[3]).model_dump()
    if idempotency_key:
        await store_idempotent_response(session, scope=scope, key=idempotency_key, response_payload=resp)
        await session.commit()
    return resp


@router.post("/deals", response_model=CreateDealStagingResponse)
async def create_deal_staging_transactions(
    body: CreateDealStagingRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    instrument_exists = (
        await session.execute(
            select(instrument.c.id).where(instrument.c.id == _parse_numeric_id(body.instrument_id, field="instrument_id"))
        )
    ).scalar_one_or_none()
    if not instrument_exists:
        raise HTTPException(status_code=404, detail="instrument_not_found")

    portfolio_ids = [_parse_numeric_id(item.portfolio_id, field="portfolio_id") for item in body.allocations]
    unique_portfolio_ids = list(set(portfolio_ids))
    existing_portfolios = {
        row[0]
        for row in (
            await session.execute(select(portfolio.c.id).where(portfolio.c.id.in_(unique_portfolio_ids)))
        ).all()
    }
    if len(existing_portfolios) != len(unique_portfolio_ids):
        raise HTTPException(status_code=404, detail="portfolio_not_found")

    total_qty_raw = Decimal(body.quantity)
    if total_qty_raw == 0:
        raise HTTPException(status_code=400, detail="invalid_total_quantity")
    total_qty_abs = abs(total_qty_raw)

    alloc_qty_abs: list[Decimal] = []
    for item in body.allocations:
        alloc_qty = abs(Decimal(item.quantity))
        if alloc_qty == 0:
            raise HTTPException(status_code=400, detail="invalid_allocation_quantity")
        alloc_qty_abs.append(alloc_qty)

    alloc_total_qty_abs = sum(alloc_qty_abs, Decimal("0"))
    if alloc_total_qty_abs != total_qty_abs:
        raise HTTPException(status_code=400, detail="allocation_quantity_mismatch")

    price = Decimal(body.price)
    if price <= 0:
        raise HTTPException(status_code=400, detail="invalid_price")

    transaction_sign = _TRANSACTION_SIGN_BY_TYPE[body.transaction_type]
    signed_total_qty = total_qty_abs * transaction_sign

    block_amount_qc = _round_money(total_qty_abs * price)
    allocation_raw_amounts = [alloc_qty * price for alloc_qty in alloc_qty_abs]
    allocation_amounts_qc = [_round_money(v) for v in allocation_raw_amounts]
    residual_qc = block_amount_qc - sum(allocation_amounts_qc, Decimal("0"))

    rounding_adjustment_index: int | None = None
    if residual_qc != 0:
        rounding_adjustment_index = max(range(len(allocation_raw_amounts)), key=lambda idx: abs(allocation_raw_amounts[idx]))
        allocation_amounts_qc[rounding_adjustment_index] = allocation_amounts_qc[rounding_adjustment_index] + residual_qc

    now = datetime.now(tz=timezone.utc)

    block_insert = await session.execute(
        text(
            """
            INSERT INTO deal_block(
              external_ref,
              instrument_id,
              trade_date,
              settle_date,
              trade_currency,
              quantity,
              price,
              lifecycle,
              created_at,
              updated_at
            )
            VALUES (
              :external_ref,
              :instrument_id,
              :trade_date,
              :settle_date,
              :trade_currency,
              :quantity,
              :price,
              'active',
              :created_at,
              :updated_at
            )
            RETURNING id
            """
        ),
        {
            "external_ref": body.external_ref,
            "instrument_id": body.instrument_id,
            "trade_date": body.trade_date,
            "settle_date": body.settle_date,
            "trade_currency": body.quote_currency,
            "quantity": signed_total_qty,
            "price": price,
            "created_at": now,
            "updated_at": now,
        },
    )
    deal_block_id = block_insert.scalar_one()

    block_staging_insert = await session.execute(
        txn_staging.insert()
        .values(
            level="block",
            deal_block_id=deal_block_id,
            deal_allocation_id=None,
            portfolio_id=None,
            instrument_id=_parse_numeric_id(body.instrument_id, field="instrument_id"),
            trade_date=body.trade_date,
            settle_date=body.settle_date,
            quantity=signed_total_qty,
            price=price,
            quote_currency=body.quote_currency,
            report_currency=body.report_currency,
            qc_gross_amount=block_amount_qc,
            rc_gross_amount=block_amount_qc if body.quote_currency == body.report_currency else None,
            status="entry",
            lifecycle="active",
            entry_version=1,
            created_at=now,
            updated_at=now,
        )
        .returning(txn_staging.c.id)
    )
    block_staging_id = block_staging_insert.scalar_one()

    allocation_stagings: list[DealAllocationStagingResponse] = []
    for idx, alloc in enumerate(body.allocations):
        signed_alloc_qty = alloc_qty_abs[idx] * transaction_sign
        alloc_insert = await session.execute(
            text(
                """
                INSERT INTO deal_allocation(
                  block_id,
                  portfolio_id,
                  quantity,
                  price,
                  is_rounding_adjustment,
                  lifecycle,
                  created_at,
                  updated_at
                )
                VALUES (
                  :block_id,
                  :portfolio_id,
                  :quantity,
                  :price,
                  :is_rounding_adjustment,
                  'active',
                  :created_at,
                  :updated_at
                )
                RETURNING id
                """
            ),
            {
                "block_id": deal_block_id,
                "portfolio_id": alloc.portfolio_id,
                "quantity": signed_alloc_qty,
                "price": price,
                "is_rounding_adjustment": rounding_adjustment_index == idx,
                "created_at": now,
                "updated_at": now,
            },
        )
        deal_allocation_id = alloc_insert.scalar_one()

        alloc_staging_insert = await session.execute(
            txn_staging.insert()
            .values(
                level="allocation",
                deal_block_id=deal_block_id,
                deal_allocation_id=deal_allocation_id,
                portfolio_id=_parse_numeric_id(alloc.portfolio_id, field="portfolio_id"),
                instrument_id=_parse_numeric_id(body.instrument_id, field="instrument_id"),
                trade_date=body.trade_date,
                settle_date=body.settle_date,
                quantity=signed_alloc_qty,
                price=price,
                quote_currency=body.quote_currency,
                report_currency=body.report_currency,
                qc_gross_amount=allocation_amounts_qc[idx],
                rc_gross_amount=allocation_amounts_qc[idx] if body.quote_currency == body.report_currency else None,
                status="entry",
                lifecycle="active",
                entry_version=1,
                created_at=now,
                updated_at=now,
            )
            .returning(txn_staging.c.id)
        )
        allocation_staging_id = alloc_staging_insert.scalar_one()
        allocation_stagings.append(
            DealAllocationStagingResponse(
                portfolio_id=alloc.portfolio_id,
                quantity=_dstr(signed_alloc_qty),
                amount_qc=_dstr(allocation_amounts_qc[idx]),
                staging_id=str(allocation_staging_id),
            )
        )

    await session.commit()

    return CreateDealStagingResponse(
        block_staging_id=str(block_staging_id),
        deal_block_id=str(deal_block_id),
        block_amount_qc=_dstr(block_amount_qc),
        allocation_stagings=allocation_stagings,
    )


@router.patch("/deals/{deal_block_id}", response_model=DealAdjustmentResponse)
async def modify_deal_staging_transactions(
    deal_block_id: str,
    body: ModifyDealRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    block_id = _parse_numeric_id(deal_block_id, field="deal_block_id")
    block_row = (
        await session.execute(
            text(
                """
                SELECT id, instrument_id, trade_date, settle_date, trade_currency, quantity, price, lifecycle
                FROM deal_block
                WHERE id = :block_id
                """
            ),
            {"block_id": block_id},
        )
    ).first()
    if not block_row:
        raise HTTPException(status_code=404, detail="deal_block_not_found")
    if block_row[7] != "active":
        raise HTTPException(status_code=409, detail="deal_block_not_active")

    total_qty_raw = Decimal(body.quantity)
    if total_qty_raw == 0:
        raise HTTPException(status_code=400, detail="invalid_total_quantity")
    total_qty_abs = abs(total_qty_raw)
    sign = Decimal("1") if Decimal(block_row[5]) >= 0 else Decimal("-1")

    target_qty_by_portfolio: dict[int, Decimal] = {}
    for alloc in body.allocations:
        pid = _parse_numeric_id(alloc.portfolio_id, field="portfolio_id")
        alloc_qty_abs = abs(Decimal(alloc.quantity))
        if alloc_qty_abs == 0:
            raise HTTPException(status_code=400, detail="invalid_allocation_quantity")
        target_qty_by_portfolio[pid] = target_qty_by_portfolio.get(pid, Decimal("0")) + (alloc_qty_abs * sign)

    if sum((abs(v) for v in target_qty_by_portfolio.values()), Decimal("0")) != total_qty_abs:
        raise HTTPException(status_code=400, detail="allocation_quantity_mismatch")

    existing_portfolios = {
        row[0]
        for row in (
            await session.execute(select(portfolio.c.id).where(portfolio.c.id.in_(list(target_qty_by_portfolio.keys()))))
        ).all()
    }
    if len(existing_portfolios) != len(target_qty_by_portfolio):
        raise HTTPException(status_code=404, detail="portfolio_not_found")

    resp = await _create_deal_adjustment_stagings(
        session,
        deal_block_id=block_id,
        instrument_id=block_row[1],
        trade_date=block_row[2],
        settle_date=block_row[3],
        quote_currency=str(block_row[4]),
        price=Decimal(block_row[6]),
        target_qty_by_portfolio=target_qty_by_portfolio,
        mark_deleted=False,
        force_reversal_replacement=True,
    )
    return resp


@router.delete("/deals/{deal_block_id}", response_model=DealAdjustmentResponse)
async def delete_deal_staging_transactions(
    deal_block_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    block_id = _parse_numeric_id(deal_block_id, field="deal_block_id")
    block_row = (
        await session.execute(
            text(
                """
                SELECT id, instrument_id, trade_date, settle_date, trade_currency, quantity, price, lifecycle
                FROM deal_block
                WHERE id = :block_id
                """
            ),
            {"block_id": block_id},
        )
    ).first()
    if not block_row:
        raise HTTPException(status_code=404, detail="deal_block_not_found")
    if block_row[7] != "active":
        raise HTTPException(status_code=409, detail="deal_block_not_active")

    resp = await _create_deal_adjustment_stagings(
        session,
        deal_block_id=block_id,
        instrument_id=block_row[1],
        trade_date=block_row[2],
        settle_date=block_row[3],
        quote_currency=str(block_row[4]),
        price=Decimal(block_row[6]),
        target_qty_by_portfolio={},
        mark_deleted=True,
        force_reversal_replacement=False,
    )
    return resp


@router.get("/{staging_id}", response_model=StagingResponse)
async def get_staging_transaction(staging_id: str, session: Annotated[AsyncSession, Depends(get_session)]):
    sid = _parse_numeric_id(staging_id, field="staging_id")
    stmt = select(
        txn_staging.c.id,
        txn_staging.c.status,
        txn_staging.c.lifecycle,
        txn_staging.c.entry_version,
    ).where(txn_staging.c.id == sid)
    res = await session.execute(stmt)
    row = res.first()
    if not row:
        raise HTTPException(status_code=404, detail="not_found")
    return StagingResponse(id=str(row[0]), status=row[1], lifecycle=row[2], entry_version=row[3]).model_dump()


@router.patch("/{staging_id}", response_model=StagingResponse)
async def update_staging_transaction(
    staging_id: str,
    body: UpdateStagingRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
    actor: Annotated[str | None, Header(alias="X-Actor")] = None,
    change_reason: Annotated[str | None, Header(alias="X-Change-Reason")] = None,
):
    sid = _parse_numeric_id(staging_id, field="staging_id")
    existing_stmt = select(txn_staging).where(txn_staging.c.id == sid)
    existing_res = await session.execute(existing_stmt)
    existing = existing_res.mappings().first()
    if not existing:
        raise HTTPException(status_code=404, detail="not_found")
    if existing["lifecycle"] != "active":
        raise HTTPException(status_code=409, detail="not_active")
    if existing["status"] != "entry":
        raise HTTPException(status_code=409, detail="not_editable")

    updates = body.model_dump(exclude_unset=True)
    if not updates:
        return StagingResponse(
            id=str(existing["id"]),
            status=existing["status"],
            lifecycle=existing["lifecycle"],
            entry_version=existing["entry_version"],
        ).model_dump()

    now = datetime.now(tz=timezone.utc)
    upd_stmt = (
        update(txn_staging)
        .where(and_(txn_staging.c.id == sid, txn_staging.c.status == "entry", txn_staging.c.lifecycle == "active"))
        .values(**updates, updated_at=now, entry_version=txn_staging.c.entry_version + 1)
        .returning(txn_staging.c.id, txn_staging.c.status, txn_staging.c.lifecycle, txn_staging.c.entry_version)
    )
    upd_res = await session.execute(upd_stmt)
    row = upd_res.first()
    if not row:
        raise HTTPException(status_code=409, detail="concurrent_update")

    await session.execute(
        txn_staging_change.insert().values(
            staging_id=sid,
            changed_at=now,
            actor=actor,
            change_reason=change_reason,
            old_row=jsonable_encoder(dict(existing)),
            new_row=jsonable_encoder({**dict(existing), **updates, "entry_version": row[3]}),
        )
    )
    await session.commit()
    return StagingResponse(id=str(row[0]), status=row[1], lifecycle=row[2], entry_version=row[3]).model_dump()
