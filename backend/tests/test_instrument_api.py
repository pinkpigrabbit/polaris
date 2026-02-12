from __future__ import annotations

from uuid import uuid4

import pytest


async def _ensure_instrument_extension_ready(db_engine) -> None:
    from sqlalchemy import text

    async with db_engine.begin() as conn:
        ready = (
            await conn.execute(
                text(
                    """
                    SELECT
                      to_regclass('public.instrument_master') IS NOT NULL
                      AND to_regclass('public.instrument_identifier') IS NOT NULL
                      AND to_regclass('public.instrument_type_id_rule') IS NOT NULL
                      AND to_regclass('public.security_type_rule') IS NOT NULL
                    """
                )
            )
        ).scalar_one()
        if not ready:
            pytest.skip("instrument 확장 스키마가 적용되지 않았습니다. db/schema.sql 최신본을 적용하세요.")

        await conn.execute(text("INSERT INTO currency(code, name) VALUES ('USD','US Dollar') ON CONFLICT DO NOTHING"))
        await conn.execute(
            text(
                """
                INSERT INTO security_id_type(code, description, is_system)
                VALUES ('BBG_TICKER', 'Bloomberg Ticker', TRUE)
                ON CONFLICT (code) DO NOTHING
                """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO security_id_type(code, description, is_system)
                VALUES ('ISIN', 'ISIN', TRUE)
                ON CONFLICT (code) DO NOTHING
                """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO instrument_type_id_rule(instrument_type, default_id_type_code, updated_by)
                VALUES ('stock', 'BBG_TICKER', 'test')
                ON CONFLICT (instrument_type) DO UPDATE SET default_id_type_code = EXCLUDED.default_id_type_code
                """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO instrument_type_id_rule(instrument_type, default_id_type_code, updated_by)
                VALUES ('futures', 'BBG_TICKER', 'test')
                ON CONFLICT (instrument_type) DO UPDATE SET default_id_type_code = EXCLUDED.default_id_type_code
                """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO security_type_rule(
                  security_type,
                  currency,
                  nav_rule,
                  accrued_interest_method,
                  price_unit,
                  default_settlement_days
                )
                VALUES ('equity_common', 'USD', 'EQUITY_MARK_TO_MARKET', 'NONE', 'amount', 2)
                ON CONFLICT (security_type, currency) DO NOTHING
                """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO security_type_rule(
                  security_type,
                  currency,
                  nav_rule,
                  accrued_interest_method,
                  price_unit,
                  default_settlement_days
                )
                VALUES ('futures_index', 'USD', 'FUTURES_MARK_TO_MARKET', 'NONE', 'index_points', 1)
                ON CONFLICT (security_type, currency) DO NOTHING
                """
            )
        )


@pytest.mark.asyncio
async def test_create_equity_instrument_with_default_security_id(fastapi_client, db_engine):
    from sqlalchemy import text

    await _ensure_instrument_extension_ready(db_engine)

    symbol = f"AAPL_{uuid4().hex[:8]}"
    payload = {
        "instrument_type": "stock",
        "symbol": symbol,
        "currency": "usd",
        "full_name": "Apple Inc.",
        "short_name": "Apple",
        "security_type": "equity_common",
        "identifiers": [
            {"id_type_code": "BBG_TICKER", "id_value": f"{symbol} US"},
            {"id_type_code": "ISIN", "id_value": f"US{uuid4().hex[:10].upper()}"},
        ],
        "equity": {
            "shares_outstanding": 15000000000,
            "free_float_shares": 14500000000,
            "share_class": "Common",
        },
    }

    resp = await fastapi_client.post("/instruments", json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["instrument_type"] == "stock"
    assert body["currency"] == "USD"
    assert body["security_id"] == f"{symbol} US"
    assert body["default_id_type_code"] == "BBG_TICKER"
    assert body["default_settlement_days"] == 2
    assert body["equity"]["shares_outstanding"] == 15000000000

    instrument_id = int(body["instrument_id"])
    async with db_engine.begin() as conn:
        master = (
            await conn.execute(
                text(
                    """
                    SELECT security_id, full_name, short_name, security_type
                    FROM instrument_master
                    WHERE instrument_id = :iid
                    """
                ),
                {"iid": instrument_id},
            )
        ).first()
        assert master is not None
        assert master[0] == f"{symbol} US"
        assert master[3] == "equity_common"

        id_rows = (
            await conn.execute(
                text(
                    """
                    SELECT id_type_code, id_value, is_primary
                    FROM instrument_identifier
                    WHERE instrument_id = :iid
                    ORDER BY id_type_code
                    """
                ),
                {"iid": instrument_id},
            )
        ).all()
        assert len(id_rows) == 2
        assert sum(1 for r in id_rows if r[2]) == 1


@pytest.mark.asyncio
async def test_create_instrument_requires_default_identifier(fastapi_client, db_engine):
    await _ensure_instrument_extension_ready(db_engine)

    payload = {
        "instrument_type": "stock",
        "symbol": f"MSFT_{uuid4().hex[:8]}",
        "currency": "USD",
        "full_name": "Microsoft Corp.",
        "short_name": "Microsoft",
        "security_type": "equity_common",
        "identifiers": [
            {"id_type_code": "ISIN", "id_value": f"US{uuid4().hex[:10].upper()}"},
        ],
        "equity": {
            "shares_outstanding": 1000000000,
        },
    }

    resp = await fastapi_client.post("/instruments", json=payload)
    assert resp.status_code == 400
    assert resp.json()["detail"] == "default_identifier_missing"


@pytest.mark.asyncio
async def test_create_futures_instrument_with_subtype(fastapi_client, db_engine):
    await _ensure_instrument_extension_ready(db_engine)

    sym = f"ES_{uuid4().hex[:6]}"
    payload = {
        "instrument_type": "futures",
        "symbol": sym,
        "currency": "USD",
        "full_name": "E-mini S&P 500",
        "short_name": "ES",
        "security_type": "futures_index",
        "identifiers": [
            {"id_type_code": "BBG_TICKER", "id_value": f"{sym} Index", "is_primary": True},
        ],
        "futures": {
            "contract_size": "50",
            "multiplier": "50",
            "tick_size": "0.25",
            "settlement_type": "cash",
        },
    }

    resp = await fastapi_client.post("/instruments", json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["security_id"] == f"{sym} Index"
    assert body["futures"]["contract_size"] == "50.000000000000"
    assert body["futures"]["settlement_type"] == "cash"
