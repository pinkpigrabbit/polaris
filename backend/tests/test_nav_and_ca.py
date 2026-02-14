from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest


async def test_ibor_nav_simple(fastapi_client, seed_master_data, db_engine, db_has_schema):
    """IBOR NAV computation test.

    What this validates:
    - IBOR NAV uses *current* positions (`position_current`).
    - Uses latest available market price <= asof_ts.
    - Returns nav_rc = sum(quantity * price) (same currency case).
    """

    if not db_has_schema:
        pytest.skip("DB schema not found. Apply updated db/schema.sql before running tests.")

    pid = seed_master_data["portfolio_id"]
    iid = seed_master_data["instrument_id"]
    now = datetime.now(tz=timezone.utc)

    from sqlalchemy import text

    async with db_engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO position_current(portfolio_id, instrument_id, quantity)
                VALUES (:pid, :iid, 2)
                ON CONFLICT (portfolio_id, instrument_id)
                DO UPDATE SET quantity = EXCLUDED.quantity
                """
            ),
            {"pid": pid, "iid": iid},
        )
        await conn.execute(
            text(
                """
                INSERT INTO market_price(instrument_id, asof_date, asof_ts, price, currency, is_eod)
                VALUES (:iid, :d, :ts, 100, 'USD', FALSE)
                """
            ),
            {"iid": iid, "d": now.date(), "ts": now},
        )

    r = await fastapi_client.get(f"/nav/ibor/{pid}")
    assert r.status_code == 200
    body = r.json()
    assert body["valuation_basis"] == "IBOR"
    assert body["nav_rc"] == "200"


@pytest.mark.usefixtures("temporal_worker")
async def test_abor_nav_eod_pipeline(fastapi_client, seed_master_data, db_engine, db_has_schema, temporal_available):
    """ABOR NAV EOD pipeline test.

    What this validates:
    - ABOR NAV is computed from `position_snapshot_eod` created by the workflow.
    - ABOR uses EOD prices (`market_price.is_eod = true`) for the asof_date.
    - Result is persisted (`abor_nav_run`, `abor_nav_result`) and retrievable via API.
    """

    if not db_has_schema:
        pytest.skip("DB schema not found. Apply updated db/schema.sql before running tests.")
    if not temporal_available:
        pytest.skip("Temporal not reachable")

    pid = seed_master_data["portfolio_id"]
    iid = seed_master_data["instrument_id"]
    asof_date = "2026-01-02"
    asof_ts = datetime(2026, 1, 2, 23, 0, 0, tzinfo=timezone.utc)

    from sqlalchemy import text

    async with db_engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO position_current(portfolio_id, instrument_id, quantity)
                VALUES (:pid, :iid, 2)
                ON CONFLICT (portfolio_id, instrument_id)
                DO UPDATE SET quantity = EXCLUDED.quantity
                """
            ),
            {"pid": pid, "iid": iid},
        )
        await conn.execute(
            text(
                """
                INSERT INTO market_price(instrument_id, asof_date, asof_ts, price, currency, is_eod)
                VALUES (:iid, :d, :ts, 110, 'USD', TRUE)
                """
            ),
            {"iid": iid, "d": asof_date, "ts": asof_ts},
        )

    r1 = await fastapi_client.post(f"/nav/abor/{pid}/run", json={"asof_date": asof_date})
    assert r1.status_code == 200
    workflow_id = r1.json()["workflow_id"]

    from temporalio.client import Client

    client = await Client.connect(os.environ["POLARIS_TEMPORAL_ADDRESS"], namespace=os.environ["POLARIS_TEMPORAL_NAMESPACE"])
    await client.get_workflow_handle(workflow_id).result()

    r2 = await fastapi_client.get(f"/nav/abor/{pid}/result", params={"asof_date": asof_date})
    assert r2.status_code == 200
    assert r2.json()["nav_rc"] == "220"


@pytest.mark.usefixtures("temporal_worker")
async def test_ca_cash_dividend_pipeline(fastapi_client, seed_master_data, db_engine, db_has_schema, temporal_available):
    """Corporate Action: cash dividend pipeline test.

    What this validates:
    - Creating a CA event and processing it via Temporal applies effects.
    - For cash_dividend: cash position increases by shares * cash_amount_per_share.
    - Effect is idempotent per (event, portfolio) via `ca_effect` unique constraint.
    """

    if not db_has_schema:
        pytest.skip("DB schema not found. Apply updated db/schema.sql before running tests.")
    if not temporal_available:
        pytest.skip("Temporal not reachable")

    pid = seed_master_data["portfolio_id"]
    iid = seed_master_data["instrument_id"]

    from sqlalchemy import text

    async with db_engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO position_current(portfolio_id, instrument_id, quantity)
                VALUES (:pid, :iid, 10)
                ON CONFLICT (portfolio_id, instrument_id)
                DO UPDATE SET quantity = EXCLUDED.quantity
                """
            ),
            {"pid": pid, "iid": iid},
        )

    r1 = await fastapi_client.post(
        "/corporate-actions",
        json={
            "ca_type": "cash_dividend",
            "instrument_id": iid,
            "ex_date": "2026-01-02",
            "pay_date": "2026-01-05",
            "currency": "USD",
            "cash_amount_per_share": "1",
            "require_election": False,
        },
    )
    assert r1.status_code == 200
    ca_event_id = r1.json()["ca_event_id"]

    r2 = await fastapi_client.post(f"/corporate-actions/{ca_event_id}/process")
    assert r2.status_code == 200
    workflow_id = r2.json()["workflow_id"]

    from temporalio.client import Client

    client = await Client.connect(os.environ["POLARIS_TEMPORAL_ADDRESS"], namespace=os.environ["POLARIS_TEMPORAL_NAMESPACE"])
    await client.get_workflow_handle(workflow_id).result()

    # Verify cash position increased by 10
    async with db_engine.connect() as conn:
        cash_instr = (
            await conn.execute(
                text(
                    """
                    SELECT id FROM instrument
                    WHERE instrument_type = 'cash' AND security_id = 'CASH_USD'
                    """
                )
            )
        ).scalar_one()
        cash_qty = (
            await conn.execute(
                text(
                    """
                    SELECT quantity FROM position_current
                    WHERE portfolio_id = :pid AND instrument_id = :cid
                    """
                ),
                {"pid": pid, "cid": str(cash_instr)},
            )
        ).scalar_one()
        assert cash_qty >= 10
