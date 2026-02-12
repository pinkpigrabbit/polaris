from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.tables import (
    fx_rate,
    instrument,
    ibor_nav_line_item,
    ibor_nav_result,
    ibor_nav_run,
    market_price,
    abor_nav_line_item,
    abor_nav_result,
    abor_nav_run,
    position_current,
    position_snapshot_eod,
)


@dataclass(frozen=True)
class NavLine:
    instrument_id: int
    quantity: Decimal
    price: Decimal | None
    price_currency: str | None
    fx_rate_to_rc: Decimal | None
    market_value_rc: Decimal


def _dstr(d: Decimal) -> str:
    # Canonical decimal string: no scientific notation, no trailing zeros.
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _asof_ts_for_eod(asof_date: date) -> datetime:
    return datetime.combine(asof_date, time(23, 59, 59), tzinfo=timezone.utc)


async def _get_prices_distinct_on(
    session: AsyncSession,
    *,
    instrument_ids: list[int],
    asof_ts: datetime,
    asof_date: date | None,
    eod_only: bool,
) -> dict[int, tuple[Decimal, str]]:
    if not instrument_ids:
        return {}

    if asof_date is None:
        sql = text(
            """
            SELECT DISTINCT ON (instrument_id) instrument_id, price, currency
            FROM market_price
            WHERE instrument_id = ANY(:ids)
              AND asof_ts <= :asof_ts
            ORDER BY instrument_id, asof_ts DESC
            """
        )
        rows = (await session.execute(sql, {"ids": instrument_ids, "asof_ts": asof_ts})).all()
    else:
        sql = text(
            """
            SELECT DISTINCT ON (instrument_id) instrument_id, price, currency
            FROM market_price
            WHERE instrument_id = ANY(:ids)
              AND asof_date = :asof_date
              AND (:eod_only = FALSE OR is_eod = TRUE)
            ORDER BY instrument_id, asof_ts DESC
            """
        )
        rows = (await session.execute(sql, {"ids": instrument_ids, "asof_date": asof_date, "eod_only": eod_only})).all()

    out: dict[int, tuple[Decimal, str]] = {}
    for iid, price, ccy in rows:
        out[iid] = (Decimal(price), str(ccy))
    return out


async def _get_prices_distinct_on_with_meta(
    session: AsyncSession,
    *,
    instrument_ids: list[int],
    asof_date: date,
    eod_only: bool,
) -> dict[int, tuple[Decimal, str, datetime, str | None]]:
    if not instrument_ids:
        return {}

    sql = text(
        """
        SELECT DISTINCT ON (instrument_id) instrument_id, price, currency, asof_ts, source_id
        FROM market_price
        WHERE instrument_id = ANY(:ids)
          AND asof_date = :asof_date
          AND (:eod_only = FALSE OR is_eod = TRUE)
        ORDER BY instrument_id, asof_ts DESC
        """
    )
    rows = (await session.execute(sql, {"ids": instrument_ids, "asof_date": asof_date, "eod_only": eod_only})).all()
    out: dict[int, tuple[Decimal, str, datetime, str | None]] = {}
    for iid, price, ccy, ts, source_id in rows:
        out[iid] = (Decimal(price), str(ccy), ts, str(source_id) if source_id else None)
    return out


async def _get_fx_rate(
    session: AsyncSession,
    *,
    base_currency: str,
    quote_currency: str,
    asof_ts: datetime,
    eod_only: bool,
) -> Decimal:
    if base_currency == quote_currency:
        return Decimal("1")

    sql = text(
        """
        SELECT rate
        FROM fx_rate
        WHERE base_currency = :base
          AND quote_currency = :quote
          AND asof_ts <= :asof_ts
          AND (:eod_only = FALSE OR is_eod = TRUE)
        ORDER BY asof_ts DESC
        LIMIT 1
        """
    )
    res = await session.execute(
        sql,
        {"base": base_currency, "quote": quote_currency, "asof_ts": asof_ts, "eod_only": eod_only},
    )
    row = res.first()
    if not row:
        raise RuntimeError(f"fx_rate_missing:{base_currency}->{quote_currency}")
    return Decimal(row[0])


async def _get_fx_rate_with_meta(
    session: AsyncSession,
    *,
    base_currency: str,
    quote_currency: str,
    asof_ts: datetime,
    eod_only: bool,
) -> tuple[Decimal, datetime | None, str | None]:
    if base_currency == quote_currency:
        return Decimal("1"), None, None

    sql = text(
        """
        SELECT rate, asof_ts, source_id
        FROM fx_rate
        WHERE base_currency = :base
          AND quote_currency = :quote
          AND asof_ts <= :asof_ts
          AND (:eod_only = FALSE OR is_eod = TRUE)
        ORDER BY asof_ts DESC
        LIMIT 1
        """
    )
    res = await session.execute(
        sql,
        {"base": base_currency, "quote": quote_currency, "asof_ts": asof_ts, "eod_only": eod_only},
    )
    row = res.first()
    if not row:
        raise RuntimeError(f"fx_rate_missing:{base_currency}->{quote_currency}")
    return Decimal(row[0]), row[1], row[2]


async def compute_ibor_nav(
    session: AsyncSession,
    *,
    portfolio_id: int,
    report_currency: str,
    asof_ts: datetime | None = None,
) -> dict[str, Any]:
    asof_ts = asof_ts or datetime.now(tz=timezone.utc)

    pos_rows = (
        await session.execute(
            select(position_current.c.instrument_id, position_current.c.quantity).where(
                position_current.c.portfolio_id == portfolio_id
            )
        )
    ).all()
    instrument_ids = [r[0] for r in pos_rows]

    instr_types = {
        r[0]: r[1]
        for r in (
            await session.execute(
                select(instrument.c.id, instrument.c.instrument_type).where(instrument.c.id.in_(instrument_ids))
            )
        ).all()
    }

    prices = await _get_prices_distinct_on(session, instrument_ids=instrument_ids, asof_ts=asof_ts, asof_date=None, eod_only=False)

    lines: list[NavLine] = []
    nav_total = Decimal("0")
    for iid, qty in pos_rows:
        qty_d = Decimal(qty)
        itype = instr_types.get(iid)
        if itype == "cash":
            mv_rc = qty_d
            lines.append(
                NavLine(
                    instrument_id=iid,
                    quantity=qty_d,
                    price=Decimal("1"),
                    price_currency=report_currency,
                    fx_rate_to_rc=Decimal("1"),
                    market_value_rc=mv_rc,
                )
            )
            nav_total += mv_rc
            continue

        if iid not in prices:
            raise RuntimeError(f"price_missing:{iid}")
        price, price_ccy = prices[iid]
        fxr = await _get_fx_rate(session, base_currency=price_ccy, quote_currency=report_currency, asof_ts=asof_ts, eod_only=False)
        mv_rc = qty_d * price * fxr
        lines.append(
            NavLine(
                instrument_id=iid,
                quantity=qty_d,
                price=price,
                price_currency=price_ccy,
                fx_rate_to_rc=fxr,
                market_value_rc=mv_rc,
            )
        )
        nav_total += mv_rc

    return {
        "valuation_basis": "IBOR",
        "run_type": "realtime",
        "portfolio_id": str(portfolio_id),
        "asof_ts": asof_ts.isoformat(),
        "report_currency": report_currency,
        "nav_rc": _dstr(nav_total),
        "line_items": [
            {
                "instrument_id": str(l.instrument_id),
                "quantity": _dstr(l.quantity),
                "price": _dstr(l.price) if l.price is not None else None,
                "price_currency": l.price_currency,
                "fx_rate_to_rc": _dstr(l.fx_rate_to_rc) if l.fx_rate_to_rc is not None else None,
                "market_value_rc": _dstr(l.market_value_rc),
            }
            for l in lines
        ],
    }


async def compute_abor_nav(
    session: AsyncSession,
    *,
    portfolio_id: int,
    report_currency: str,
    asof_date: date,
) -> dict[str, Any]:
    asof_ts = _asof_ts_for_eod(asof_date)

    pos_rows = (
        await session.execute(
            select(position_snapshot_eod.c.instrument_id, position_snapshot_eod.c.quantity).where(
                position_snapshot_eod.c.portfolio_id == portfolio_id,
                position_snapshot_eod.c.asof_date == asof_date,
            )
        )
    ).all()
    instrument_ids = [r[0] for r in pos_rows]

    instr_types = {
        r[0]: r[1]
        for r in (
            await session.execute(
                select(instrument.c.id, instrument.c.instrument_type).where(instrument.c.id.in_(instrument_ids))
            )
        ).all()
    }

    prices = await _get_prices_distinct_on_with_meta(
        session,
        instrument_ids=instrument_ids,
        asof_date=asof_date,
        eod_only=True,
    )

    line_items: list[dict[str, Any]] = []
    nav_total = Decimal("0")
    for iid, qty in pos_rows:
        qty_d = Decimal(qty)
        itype = instr_types.get(iid)
        if itype == "cash":
            mv_rc = qty_d
            line_items.append(
                {
                    "instrument_id": str(iid),
                    "quantity": _dstr(qty_d),
                    "price": "1",
                    "price_currency": report_currency,
                    "price_asof_ts": None,
                    "price_source_id": None,
                    "fx_rate_to_rc": "1",
                    "fx_rate_asof_ts": None,
                    "fx_rate_source_id": None,
                    "market_value_rc": _dstr(mv_rc),
                }
            )
            nav_total += mv_rc
            continue

        if iid not in prices:
            raise RuntimeError(f"eod_price_missing:{iid}:{asof_date.isoformat()}")
        price, price_ccy, price_ts, price_source_id = prices[iid]
        fxr, fx_ts, fx_source_id = await _get_fx_rate_with_meta(
            session,
            base_currency=price_ccy,
            quote_currency=report_currency,
            asof_ts=asof_ts,
            eod_only=True,
        )
        mv_rc = qty_d * price * fxr
        # ABOR은 가격/FX의 기준시각/소스를 추적 가능해야 한다.
        line_items.append(
            {
                "instrument_id": str(iid),
                "quantity": _dstr(qty_d),
                "price": _dstr(price),
                "price_currency": price_ccy,
                "price_asof_ts": price_ts.isoformat() if price_ts else None,
                "price_source_id": str(price_source_id) if price_source_id else None,
                "fx_rate_to_rc": _dstr(fxr),
                "fx_rate_asof_ts": fx_ts.isoformat() if fx_ts else None,
                "fx_rate_source_id": str(fx_source_id) if fx_source_id else None,
                "market_value_rc": _dstr(mv_rc),
            }
        )
        nav_total += mv_rc

    return {
        "valuation_basis": "ABOR",
        "run_type": "eod",
        "portfolio_id": str(portfolio_id),
        "asof_date": asof_date.isoformat(),
        "asof_ts": asof_ts.isoformat(),
        "report_currency": report_currency,
        "nav_rc": _dstr(nav_total),
        "line_items": line_items,
    }


async def persist_ibor_nav_run(
    session: AsyncSession,
    *,
    run_type: str,
    portfolio_id: int,
    asof_ts: datetime,
    asof_date: date,
    report_currency: str,
    through_acct_transaction_id: int | None,
    nav_payload: dict[str, Any],
    idempotency_scope: str | None,
    idempotency_key: str | None,
) -> int:
    now = datetime.now(tz=timezone.utc)
    nav_run_stmt = (
        insert(ibor_nav_run)
        .values(
            run_type=run_type,
            portfolio_id=portfolio_id,
            asof_ts=asof_ts,
            asof_date=asof_date,
            report_currency=report_currency,
            through_acct_transaction_id=through_acct_transaction_id,
            status="running",
            idempotency_scope=idempotency_scope,
            idempotency_key=idempotency_key,
            created_at=now,
        )
        .on_conflict_do_nothing(index_elements=[ibor_nav_run.c.portfolio_id, ibor_nav_run.c.run_type, ibor_nav_run.c.asof_ts])
        .returning(ibor_nav_run.c.id)
    )
    res = await session.execute(nav_run_stmt)
    run_id = res.scalar_one_or_none()

    if run_id is None:
        existing = (
            await session.execute(
                select(ibor_nav_run.c.id).where(
                    ibor_nav_run.c.portfolio_id == portfolio_id,
                    ibor_nav_run.c.run_type == run_type,
                    ibor_nav_run.c.asof_ts == asof_ts,
                )
            )
        ).scalar_one()
        return existing

    nav_rc = Decimal(nav_payload["nav_rc"])
    await session.execute(ibor_nav_result.insert().values(ibor_nav_run_id=run_id, nav_rc=nav_rc, created_at=now))

    for li in nav_payload["line_items"]:
        await session.execute(
            ibor_nav_line_item.insert().values(
                ibor_nav_run_id=run_id,
                instrument_id=int(li["instrument_id"]),
                quantity=Decimal(li["quantity"]),
                price=Decimal(li["price"]) if li["price"] is not None else None,
                price_currency=li["price_currency"],
                market_value_rc=Decimal(li["market_value_rc"]),
                fx_rate_to_rc=Decimal(li["fx_rate_to_rc"]) if li["fx_rate_to_rc"] is not None else None,
                created_at=now,
            )
        )

    await session.execute(update(ibor_nav_run).where(ibor_nav_run.c.id == run_id).values(status="completed", completed_at=now))
    return run_id


async def persist_abor_nav_run(
    session: AsyncSession,
    *,
    run_type: str,
    portfolio_id: int,
    asof_ts: datetime,
    asof_date: date,
    report_currency: str,
    position_snapshot_taken_at: datetime | None,
    through_acct_transaction_id: int | None,
    nav_payload: dict[str, Any],
    idempotency_scope: str | None,
    idempotency_key: str | None,
) -> int:
    now = datetime.now(tz=timezone.utc)
    nav_run_stmt = (
        insert(abor_nav_run)
        .values(
            run_type=run_type,
            portfolio_id=portfolio_id,
            asof_date=asof_date,
            asof_ts=asof_ts,
            report_currency=report_currency,
            position_snapshot_taken_at=position_snapshot_taken_at,
            through_acct_transaction_id=through_acct_transaction_id,
            status="running",
            idempotency_scope=idempotency_scope,
            idempotency_key=idempotency_key,
            created_at=now,
        )
        .on_conflict_do_nothing(index_elements=[abor_nav_run.c.portfolio_id, abor_nav_run.c.run_type, abor_nav_run.c.asof_date])
        .returning(abor_nav_run.c.id)
    )
    res = await session.execute(nav_run_stmt)
    run_id = res.scalar_one_or_none()

    if run_id is None:
        existing = (
            await session.execute(
                select(abor_nav_run.c.id).where(
                    abor_nav_run.c.portfolio_id == portfolio_id,
                    abor_nav_run.c.run_type == run_type,
                    abor_nav_run.c.asof_date == asof_date,
                )
            )
        ).scalar_one()
        return existing

    nav_rc = Decimal(nav_payload["nav_rc"])
    await session.execute(abor_nav_result.insert().values(abor_nav_run_id=run_id, nav_rc=nav_rc, created_at=now))

    for li in nav_payload["line_items"]:
        await session.execute(
            abor_nav_line_item.insert().values(
                abor_nav_run_id=run_id,
                instrument_id=int(li["instrument_id"]),
                quantity=Decimal(li["quantity"]),
                price=Decimal(li["price"]) if li["price"] is not None else None,
                price_currency=li["price_currency"],
                price_asof_ts=datetime.fromisoformat(li["price_asof_ts"]) if li.get("price_asof_ts") else None,
                price_source_id=li.get("price_source_id"),
                market_value_rc=Decimal(li["market_value_rc"]),
                fx_rate_to_rc=Decimal(li["fx_rate_to_rc"]) if li["fx_rate_to_rc"] is not None else None,
                fx_rate_asof_ts=datetime.fromisoformat(li["fx_rate_asof_ts"]) if li.get("fx_rate_asof_ts") else None,
                fx_rate_source_id=li.get("fx_rate_source_id"),
                created_at=now,
            )
        )

    await session.execute(update(abor_nav_run).where(abor_nav_run.c.id == run_id).values(status="completed", completed_at=now))
    return run_id
