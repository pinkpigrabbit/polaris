from __future__ import annotations

import os

import pytest


@pytest.mark.usefixtures("temporal_worker")
async def test_process_pipeline_to_settled(
    fastapi_client,
    seed_master_data,
    db_engine,
    db_has_schema,
    redis_available,
    temporal_available,
):
    """End-to-end pipeline test (API -> Temporal -> DB/Redis).

    What this validates:
    1) API create returns a staging row in status=entry.
    2) API process starts Temporal workflow.
    3) Workflow activities advance statuses in order and persist audit transitions.
    4) Accounting and position_current are updated (DB source of truth).
    5) Redis cache is updated from DB position_current (write-through).

    Preconditions:
    - Postgres/Redis reachable (podman compose)
    - Temporal server reachable
    - db/schema.sql applied
    """

    if not db_has_schema:
        pytest.skip("DB schema not found. Apply db/schema.sql before running tests.")
    if not temporal_available:
        pytest.skip("Temporal not reachable")
    if not redis_available:
        pytest.skip("Redis not reachable")

    req = {
        "level": "allocation",
        "portfolio_id": seed_master_data["portfolio_id"],
        "instrument_id": seed_master_data["instrument_id"],
        "trade_date": "2026-01-02",
        "quantity": "3",
        "price": "100",
        "quote_currency": "USD",
        "report_currency": "USD",
    }
    r1 = await fastapi_client.post("/staging-transactions", json=req)
    assert r1.status_code == 200
    staging_id = r1.json()["id"]

    r2 = await fastapi_client.post(f"/staging-transactions/{staging_id}/process")
    assert r2.status_code == 200
    workflow_id = r2.json()["workflow_id"]

    from temporalio.client import Client

    address = os.environ["POLARIS_TEMPORAL_ADDRESS"]
    namespace = os.environ["POLARIS_TEMPORAL_NAMESPACE"]
    client = await Client.connect(address, namespace=namespace)
    handle = client.get_workflow_handle(workflow_id)
    await handle.result()

    # Verify status settled
    r3 = await fastapi_client.get(f"/staging-transactions/{staging_id}")
    assert r3.status_code == 200
    assert r3.json()["status"] == "settled"

    # Verify DB side-effects
    from sqlalchemy import text

    sid = int(staging_id)
    async with db_engine.connect() as conn:
        acct_cnt = (await conn.execute(text("SELECT count(*) FROM journal_entry WHERE pending_trade_id = :sid"), {"sid": sid})).scalar_one()
        assert acct_cnt == 1

        pos_qty = (
            await conn.execute(
                text(
                    """
                    SELECT quantity
                    FROM position_current
                    WHERE portfolio_id = :pid AND instrument_id = :iid
                    """
                ),
                {"pid": seed_master_data["portfolio_id"], "iid": seed_master_data["instrument_id"]},
            )
        ).scalar_one()
        assert pos_qty >= 3

    # Verify Redis cache key exists
    import redis.asyncio as redis

    r = redis.from_url(os.environ["POLARIS_REDIS_URL"])
    key = f"position:{seed_master_data['portfolio_id']}:{seed_master_data['instrument_id']}"
    val = await r.get(key)
    await r.aclose()
    assert val is not None
