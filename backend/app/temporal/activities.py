from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from temporalio import activity

from ..db.session import SessionLocal
from ..db.tables import (
    acct_entry,
    acct_transaction,
    ca_portfolio_rule,
    ca_effect,
    ca_election,
    ca_event,
    instrument,
    portfolio,
    position_current,
    position_snapshot_eod,
    txn_staging,
)
from ..idempotency import get_idempotent_response, store_idempotent_response
from ..redis_cache import redis_cache
from ..state_machine import TemporalContext, advance_status

from ..nav.service import compute_abor_nav, persist_abor_nav_run


def _temporal_ctx() -> TemporalContext:
    info = activity.info()
    run_id = getattr(info, "workflow_run_id", None)
    if run_id is None:
        run_id = getattr(info, "run_id", None)
    return TemporalContext(workflow_id=info.workflow_id, run_id=run_id, activity_id=info.activity_id)


def _parse_staging_id(staging_id: str) -> int:
    value = int(staging_id)
    if value <= 0:
        raise RuntimeError("staging_id_invalid")
    return value


def _trade_type_from_quantity(quantity: Decimal) -> str:
    return "BUY" if quantity >= 0 else "SELL"


def _entry_role_from_source_system(source_system: str | None) -> str:
    if source_system in ("modify_reversal", "delete_reversal"):
        return "reversal"
    if source_system == "modify_replacement":
        return "replacement"
    return "normal"


@activity.defn
async def precheck_activity(staging_id: str) -> dict:
    scope = f"activity:advance_status:{staging_id}"
    key = "to:pre_check"
    async with SessionLocal() as session:
        cached = await get_idempotent_response(session, scope=scope, key=key)
        if cached:
            return cached

        sid = _parse_staging_id(staging_id)
        row = (await session.execute(select(txn_staging).where(txn_staging.c.id == sid))).mappings().first()
        if not row:
            raise RuntimeError("staging_not_found")
        if row["lifecycle"] != "active":
            raise RuntimeError("staging_not_active")
        if row["status"] not in ("entry", "pre_check"):
            raise RuntimeError(f"unexpected_status:{row['status']}")

        if row["quantity"] == 0:
            raise RuntimeError("quantity_zero")
        if row["price"] <= 0:
            raise RuntimeError("price_invalid")

        if row["status"] == "entry":
            await advance_status(
                session,
                staging_id=sid,
                from_status="entry",
                to_status="pre_check",
                triggered_by="temporal",
                idempotency_scope=scope,
                idempotency_key=key,
                temporal=_temporal_ctx(),
            )

        resp = {"staging_id": staging_id, "status": "pre_check"}
        await store_idempotent_response(session, scope=scope, key=key, response_payload=resp)
        await session.commit()
        return resp


@activity.defn
async def position_activity(staging_id: str) -> dict:
    scope = f"activity:advance_status:{staging_id}"
    key = "to:position"
    async with SessionLocal() as session:
        cached = await get_idempotent_response(session, scope=scope, key=key)
        if cached:
            return cached

        sid = _parse_staging_id(staging_id)
        row = (await session.execute(select(txn_staging).where(txn_staging.c.id == sid))).mappings().first()
        if not row:
            raise RuntimeError("staging_not_found")
        if row["lifecycle"] != "active":
            raise RuntimeError("staging_not_active")
        if row["status"] not in ("pre_check", "position"):
            raise RuntimeError(f"unexpected_status:{row['status']}")

        now = datetime.now(tz=timezone.utc)
        amount = row["qc_gross_amount"]
        if amount is None:
            amount = Decimal(row["quantity"]) * Decimal(row["price"])  # type: ignore[arg-type]

        qty = Decimal(row["quantity"])  # type: ignore[arg-type]
        trade_type = _trade_type_from_quantity(qty)
        entry_role = _entry_role_from_source_system(row.get("source_system"))
        reference_entry_id = None
        if entry_role in ("reversal", "replacement"):
            reference_entry_id = (
                await session.execute(
                    select(acct_transaction.c.id)
                    .where(
                        acct_transaction.c.deal_block_id == row["deal_block_id"],
                        acct_transaction.c.entry_role == "normal",
                    )
                    .order_by(acct_transaction.c.created_at.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()

        acct_txn_stmt = (
            acct_transaction.insert()
            .values(
                staging_id=sid,
                deal_block_id=row["deal_block_id"],
                deal_allocation_id=row["deal_allocation_id"],
                effective_date=row["trade_date"],
                posted_at=now,
                description="staging_post",
                trade_type=trade_type,
                entry_role=entry_role,
                reversal_of_acct_transaction_id=reference_entry_id if entry_role == "reversal" else None,
                replacement_of_entry_id=reference_entry_id if entry_role == "replacement" else None,
                created_at=now,
            )
            .returning(acct_transaction.c.id)
        )
        acct_txn_res = await session.execute(acct_txn_stmt)
        acct_txn_id = acct_txn_res.scalar_one()

        await session.execute(
            acct_entry.insert().values(
                acct_transaction_id=acct_txn_id,
                portfolio_id=row["portfolio_id"],
                instrument_id=row["instrument_id"],
                account_code="POSITION",
                drcr="DR" if qty > 0 else "CR",
                quantity=row["quantity"],
                amount=amount,
                currency=row["quote_currency"],
                created_at=now,
            )
        )

        if row["portfolio_id"] is not None:
            pos_insert = insert(position_current).values(
                portfolio_id=row["portfolio_id"],
                instrument_id=row["instrument_id"],
                quantity=row["quantity"],
                cost_basis_rc=row["rc_gross_amount"],
                last_acct_transaction_id=acct_txn_id,
                updated_at=now,
            )
            pos_upsert = pos_insert.on_conflict_do_update(
                index_elements=[position_current.c.portfolio_id, position_current.c.instrument_id],
                set_={
                    "quantity": position_current.c.quantity + row["quantity"],
                    "cost_basis_rc": row["rc_gross_amount"],
                    "last_acct_transaction_id": acct_txn_id,
                    "version_uuid": func.gen_random_uuid(),
                    "updated_at": now,
                },
            )
            await session.execute(pos_upsert)

        if row["status"] == "pre_check":
            await advance_status(
                session,
                staging_id=sid,
                from_status="pre_check",
                to_status="position",
                triggered_by="temporal",
                idempotency_scope=scope,
                idempotency_key=key,
                temporal=_temporal_ctx(),
            )

        if row["portfolio_id"] is not None:
            pos_row = (
                await session.execute(
                    select(
                        position_current.c.quantity,
                        position_current.c.version_uuid,
                        position_current.c.updated_at,
                    ).where(
                        position_current.c.portfolio_id == row["portfolio_id"],
                        position_current.c.instrument_id == row["instrument_id"],
                    )
                )
            ).first()
            if pos_row:
                await redis_cache.set_position(
                    portfolio_id=str(row["portfolio_id"]),
                    instrument_id=str(row["instrument_id"]),
                    payload={
                        "quantity": str(pos_row[0]),
                        "version_uuid": str(pos_row[1]),
                        "updated_at": pos_row[2].isoformat() if pos_row[2] else None,
                        "source": "db",
                    },
                )

        resp = {"staging_id": staging_id, "status": "position", "acct_transaction_id": str(acct_txn_id)}
        await store_idempotent_response(session, scope=scope, key=key, response_payload=resp)
        await session.commit()
        return resp


@activity.defn
async def allocation_activity(staging_id: str) -> dict:
    scope = f"activity:advance_status:{staging_id}"
    key = "to:allocated"
    async with SessionLocal() as session:
        cached = await get_idempotent_response(session, scope=scope, key=key)
        if cached:
            return cached

        sid = _parse_staging_id(staging_id)
        row = (await session.execute(select(txn_staging).where(txn_staging.c.id == sid))).mappings().first()
        if not row:
            raise RuntimeError("staging_not_found")
        if row["lifecycle"] != "active":
            raise RuntimeError("staging_not_active")
        if row["status"] not in ("position", "allocated"):
            raise RuntimeError(f"unexpected_status:{row['status']}")

        if row["level"] == "allocation" and row["portfolio_id"] is None:
            raise RuntimeError("allocation_requires_portfolio")

        if row["status"] == "position":
            await advance_status(
                session,
                staging_id=sid,
                from_status="position",
                to_status="allocated",
                triggered_by="temporal",
                idempotency_scope=scope,
                idempotency_key=key,
                temporal=_temporal_ctx(),
            )

        resp = {"staging_id": staging_id, "status": "allocated"}
        await store_idempotent_response(session, scope=scope, key=key, response_payload=resp)
        await session.commit()
        return resp


@activity.defn
async def settle_activity(staging_id: str) -> dict:
    scope = f"activity:advance_status:{staging_id}"
    key = "to:settled"
    async with SessionLocal() as session:
        cached = await get_idempotent_response(session, scope=scope, key=key)
        if cached:
            return cached

        sid = _parse_staging_id(staging_id)
        row = (await session.execute(select(txn_staging).where(txn_staging.c.id == sid))).mappings().first()
        if not row:
            raise RuntimeError("staging_not_found")
        if row["lifecycle"] != "active":
            raise RuntimeError("staging_not_active")
        if row["status"] not in ("allocated", "settled"):
            raise RuntimeError(f"unexpected_status:{row['status']}")

        if row["status"] == "allocated":
            await advance_status(
                session,
                staging_id=sid,
                from_status="allocated",
                to_status="settled",
                triggered_by="temporal",
                idempotency_scope=scope,
                idempotency_key=key,
                temporal=_temporal_ctx(),
            )

        resp = {"staging_id": staging_id, "status": "settled"}
        await store_idempotent_response(session, scope=scope, key=key, response_payload=resp)
        await session.commit()
        return resp


@activity.defn
async def abor_nav_snapshot_positions_activity(portfolio_id: str, asof_date: str) -> dict:
    scope = f"activity:abor_snapshot:{portfolio_id}:{asof_date}"
    key = "apply"
    async with SessionLocal() as session:
        cached = await get_idempotent_response(session, scope=scope, key=key)
        if cached:
            return cached

        pid = int(portfolio_id)
        d = date.fromisoformat(asof_date)
        now = datetime.now(tz=timezone.utc)

        pos_rows = (
            await session.execute(
                select(
                    position_current.c.instrument_id,
                    position_current.c.quantity,
                    position_current.c.cost_basis_rc,
                    position_current.c.last_acct_transaction_id,
                ).where(position_current.c.portfolio_id == pid)
            )
        ).all()

        for iid, qty, cost_basis_rc, last_txn_id in pos_rows:
            stmt = insert(position_snapshot_eod).values(
                asof_date=d,
                portfolio_id=pid,
                instrument_id=iid,
                quantity=qty,
                cost_basis_rc=cost_basis_rc,
                through_acct_transaction_id=last_txn_id,
                created_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    position_snapshot_eod.c.asof_date,
                    position_snapshot_eod.c.portfolio_id,
                    position_snapshot_eod.c.instrument_id,
                ],
                set_={
                    "quantity": qty,
                    "cost_basis_rc": cost_basis_rc,
                    "through_acct_transaction_id": last_txn_id,
                    "created_at": now,
                },
            )
            await session.execute(stmt)

        resp = {"portfolio_id": portfolio_id, "asof_date": asof_date, "snapshot": "ok"}
        await store_idempotent_response(session, scope=scope, key=key, response_payload=resp)
        await session.commit()
        return resp


@activity.defn
async def abor_nav_compute_activity(portfolio_id: str, asof_date: str) -> dict:
    scope = f"activity:abor_nav:{portfolio_id}:{asof_date}"
    key = "compute"
    async with SessionLocal() as session:
        cached = await get_idempotent_response(session, scope=scope, key=key)
        if cached:
            return cached

        pid = int(portfolio_id)
        d = date.fromisoformat(asof_date)

        rc = (
            await session.execute(select(portfolio.c.report_currency).where(portfolio.c.id == pid))
        ).scalar_one()

        nav_payload = await compute_abor_nav(session, portfolio_id=pid, report_currency=str(rc), asof_date=d)

        snapshot_taken_at = (
            await session.execute(
                select(func.max(position_snapshot_eod.c.created_at)).where(
                    position_snapshot_eod.c.portfolio_id == pid,
                    position_snapshot_eod.c.asof_date == d,
                )
            )
        ).scalar_one_or_none()

        run_id = await persist_abor_nav_run(
            session,
            run_type="eod",
            portfolio_id=pid,
            asof_ts=datetime.fromisoformat(nav_payload["asof_ts"]),
            asof_date=d,
            report_currency=str(rc),
            position_snapshot_taken_at=snapshot_taken_at,
            through_acct_transaction_id=None,
            nav_payload=nav_payload,
            idempotency_scope=scope,
            idempotency_key=key,
        )
        await session.commit()

        resp = {"portfolio_id": portfolio_id, "asof_date": asof_date, "nav_run_id": str(run_id)}
        await store_idempotent_response(session, scope=scope, key=key, response_payload=resp)
        await session.commit()
        return resp


async def _get_or_create_cash_instrument_id(session, *, currency_code: str) -> int:
    security_id = f"CASH_{currency_code}"
    row = (
        await session.execute(
            select(instrument.c.id).where(
                instrument.c.instrument_type == "cash",
                instrument.c.security_id == security_id,
            )
        )
    ).first()
    if row:
        return int(row[0])

    now = datetime.now(tz=timezone.utc)
    ins = (
        instrument.insert()
        .values(
            instrument_type="cash",
            security_id=security_id,
            name=f"Cash {currency_code}",
            currency=currency_code,
            lifecycle="active",
            created_at=now,
            updated_at=now,
        )
        .returning(instrument.c.id)
    )
    res = await session.execute(ins)
    return int(res.scalar_one())


@activity.defn
async def ca_process_event_activity(ca_event_id: str) -> dict:
    scope = f"activity:ca_event:{ca_event_id}"
    key = "process"
    async with SessionLocal() as session:
        cached = await get_idempotent_response(session, scope=scope, key=key)
        if cached:
            return cached

        eid = int(ca_event_id)
        event = (await session.execute(select(ca_event).where(ca_event.c.id == eid))).mappings().first()
        if not event:
            raise RuntimeError("ca_event_not_found")
        if event["lifecycle"] != "active":
            raise RuntimeError("ca_event_not_active")
        if event["status"] in ("processed", "cancelled"):
            resp = {"ca_event_id": ca_event_id, "status": event["status"]}
            await store_idempotent_response(session, scope=scope, key=key, response_payload=resp)
            await session.commit()
            return resp

        holders = (
            await session.execute(
                select(position_current.c.portfolio_id, position_current.c.quantity).where(
                    position_current.c.instrument_id == event["instrument_id"],
                    position_current.c.quantity != 0,
                )
            )
        ).all()

    processed = 0
    for pid, shares in holders:
        async with SessionLocal() as s2:
            async with s2.begin():
                # Election gate
                req_election = bool(event["require_election"])
                rule_req = (
                    await s2.execute(
                        select(ca_portfolio_rule.c.require_election).where(
                            ca_portfolio_rule.c.portfolio_id == pid,
                            ca_portfolio_rule.c.ca_type == event["ca_type"],
                        )
                    )
                ).scalar_one_or_none()
                if rule_req is not None:
                    req_election = req_election or bool(rule_req)

                if req_election:
                    choice = (
                        await s2.execute(
                            select(ca_election.c.choice).where(
                                ca_election.c.ca_event_id == eid,
                                ca_election.c.portfolio_id == pid,
                            )
                        )
                    ).scalar_one_or_none()
                    if choice != "accept":
                        continue

                # Claim per-portfolio effect to prevent duplicates on retries.
                claim = (
                    insert(ca_effect)
                    .values(
                        ca_event_id=eid,
                        portfolio_id=pid,
                        cash_amount=Decimal("0"),
                        share_delta=Decimal("0"),
                        processed_at=datetime.now(tz=timezone.utc),
                    )
                    .on_conflict_do_nothing(index_elements=[ca_effect.c.ca_event_id, ca_effect.c.portfolio_id])
                    .returning(ca_effect.c.id)
                )
                res = await s2.execute(claim)
                claimed_row = res.first()
                if not claimed_row:
                    continue

                now = datetime.now(tz=timezone.utc)
                acct_txn_stmt = (
                    acct_transaction.insert()
                    .values(
                        staging_id=None,
                        deal_block_id=None,
                        deal_allocation_id=None,
                        effective_date=event["pay_date"] or event["ex_date"],
                        posted_at=now,
                        description=f"ca:{event['ca_type']}",
                        trade_type="BUY",
                        entry_role="normal",
                        created_at=now,
                    )
                    .returning(acct_transaction.c.id)
                )
                acct_txn_id = (await s2.execute(acct_txn_stmt)).scalar_one()

                cash_amount = Decimal("0")
                share_delta = Decimal("0")

                if event["ca_type"] == "cash_dividend":
                    per_share = Decimal(event["cash_amount_per_share"])
                    cash_amount = Decimal(shares) * per_share
                    ccy = event["currency"]
                    if not ccy:
                        ccy = (
                            await s2.execute(select(portfolio.c.report_currency).where(portfolio.c.id == pid))
                        ).scalar_one()
                    cash_instr_id = await _get_or_create_cash_instrument_id(s2, currency_code=str(ccy))

                    pos_insert = insert(position_current).values(
                        portfolio_id=pid,
                        instrument_id=cash_instr_id,
                        quantity=cash_amount,
                        cost_basis_rc=None,
                        last_acct_transaction_id=acct_txn_id,
                        updated_at=now,
                    )
                    pos_upsert = pos_insert.on_conflict_do_update(
                        index_elements=[position_current.c.portfolio_id, position_current.c.instrument_id],
                        set_={
                            "quantity": position_current.c.quantity + cash_amount,
                            "last_acct_transaction_id": acct_txn_id,
                            "version_uuid": func.gen_random_uuid(),
                            "updated_at": now,
                        },
                    )
                    await s2.execute(pos_upsert)

                    await s2.execute(
                        acct_entry.insert().values(
                            acct_transaction_id=acct_txn_id,
                            portfolio_id=pid,
                            instrument_id=cash_instr_id,
                            account_code="CASH",
                            drcr="DR",
                            quantity=cash_amount,
                            amount=cash_amount,
                            currency=str(ccy),
                            created_at=now,
                        )
                    )
                    await s2.execute(
                        acct_entry.insert().values(
                            acct_transaction_id=acct_txn_id,
                            portfolio_id=pid,
                            instrument_id=event["instrument_id"],
                            account_code="DIVIDEND_INCOME",
                            drcr="CR",
                            quantity=None,
                            amount=cash_amount,
                            currency=str(ccy),
                            created_at=now,
                        )
                    )

                elif event["ca_type"] == "stock_split":
                    ratio = Decimal(event["split_numerator"]) / Decimal(event["split_denominator"])
                    new_shares = Decimal(shares) * ratio
                    share_delta = new_shares - Decimal(shares)

                    upd = (
                        position_current.update()
                        .where(
                            position_current.c.portfolio_id == pid,
                            position_current.c.instrument_id == event["instrument_id"],
                        )
                        .values(
                            quantity=position_current.c.quantity + share_delta,
                            last_acct_transaction_id=acct_txn_id,
                            version_uuid=func.gen_random_uuid(),
                            updated_at=now,
                        )
                    )
                    await s2.execute(upd)

                    rc = (
                        await s2.execute(select(portfolio.c.report_currency).where(portfolio.c.id == pid))
                    ).scalar_one()
                    await s2.execute(
                        acct_entry.insert().values(
                            acct_transaction_id=acct_txn_id,
                            portfolio_id=pid,
                            instrument_id=event["instrument_id"],
                            account_code="STOCK_SPLIT",
                            drcr="DR" if share_delta >= 0 else "CR",
                            quantity=share_delta,
                            amount=Decimal("0"),
                            currency=str(rc),
                            created_at=now,
                        )
                    )

                await s2.execute(
                    ca_effect.update()
                    .where(ca_effect.c.ca_event_id == eid, ca_effect.c.portfolio_id == pid)
                    .values(
                        acct_transaction_id=acct_txn_id,
                        cash_amount=cash_amount,
                        share_delta=share_delta,
                        processed_at=now,
                    )
                )
                processed += 1

    async with SessionLocal() as session:
        await session.execute(ca_event.update().where(ca_event.c.id == eid).values(status="processed"))
        resp = {"ca_event_id": ca_event_id, "status": "processed", "processed_portfolios": processed}
        await store_idempotent_response(session, scope=scope, key=key, response_payload=resp)
        await session.commit()
        return resp
