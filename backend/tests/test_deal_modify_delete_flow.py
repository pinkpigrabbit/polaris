from __future__ import annotations

import os
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import text
from temporalio.client import Client


@pytest.mark.usefixtures("temporal_worker")
async def test_deal_modify_and_delete_flow(
    fastapi_client,
    db_engine,
    db_has_schema,
    temporal_available,
):
    if not db_has_schema:
        pytest.skip("DB schema not found. Apply db/schema.sql before running tests.")
    if not temporal_available:
        pytest.skip("Temporal server not reachable on POLARIS_TEMPORAL_ADDRESS")

    today = datetime.now(tz=timezone.utc).date()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)
    now_utc = datetime.now(tz=timezone.utc)
    yesterday_eod_ts = datetime.combine(yesterday, time(23, 0, 0), tzinfo=timezone.utc)
    today_price_ts = now_utc - timedelta(minutes=1)
    tomorrow_eod_ts = datetime.combine(tomorrow, time(23, 0, 0), tzinfo=timezone.utc)

    token_1 = uuid4().hex[:8]
    token_2 = uuid4().hex[:8]

    print("\n================ [Deal 정정/삭제 시나리오 시작] ================")

    print("\n[1단계] 기준 데이터 준비")
    async with db_engine.begin() as conn:
        await conn.execute(text("INSERT INTO currency(code, name) VALUES ('USD','US Dollar') ON CONFLICT DO NOTHING"))

        p1_row = await conn.execute(
            text(
                """
                INSERT INTO portfolio(code, name, report_currency)
                VALUES (:code, :name, 'USD')
                RETURNING id
                """
            ),
            {
                "code": f"MOD-P1-{token_1}",
                "name": "정정삭제 테스트 포트폴리오 1",
            },
        )
        portfolio_1_id = str(p1_row.scalar_one())

        p2_row = await conn.execute(
            text(
                """
                INSERT INTO portfolio(code, name, report_currency)
                VALUES (:code, :name, 'USD')
                RETURNING id
                """
            ),
            {
                "code": f"MOD-P2-{token_2}",
                "name": "정정삭제 테스트 포트폴리오 2",
            },
        )
        portfolio_2_id = str(p2_row.scalar_one())

        instrument_row = await conn.execute(
            text(
                """
                    INSERT INTO instrument(instrument_type, symbol, name, currency, lifecycle, created_at, updated_at)
                    VALUES ('stock', 'AAPL US', 'Apple Inc. US', 'USD', 'active', now(), now())
                    ON CONFLICT (instrument_type, symbol)
                    DO UPDATE SET
                      name = EXCLUDED.name,
                  currency = EXCLUDED.currency,
                  lifecycle = 'active',
                  updated_at = now()
                RETURNING id
                """
            ),
            {},
        )
        instrument_id = str(instrument_row.scalar_one())
        print(f"포트폴리오1: {portfolio_1_id}")
        print(f"포트폴리오2: {portfolio_2_id}")
        print("종목: AAPL US")

        await conn.execute(
            text(
                """
                INSERT INTO market_price(instrument_id, asof_date, asof_ts, price, currency, is_eod)
                VALUES (:iid, :asof_date, :asof_ts, 600, 'USD', TRUE)
                """
            ),
            {
                "iid": instrument_id,
                "asof_date": yesterday,
                "asof_ts": yesterday_eod_ts,
            },
        )
        await conn.execute(
            text(
                """
                INSERT INTO market_price(instrument_id, asof_date, asof_ts, price, currency, is_eod)
                VALUES (:iid, :asof_date, :asof_ts, 550, 'USD', TRUE)
                """
            ),
            {
                "iid": instrument_id,
                "asof_date": today,
                "asof_ts": today_price_ts,
            },
        )
        await conn.execute(
            text(
                """
                INSERT INTO market_price(instrument_id, asof_date, asof_ts, price, currency, is_eod)
                VALUES (:iid, :asof_date, :asof_ts, 540, 'USD', TRUE)
                """
            ),
            {
                "iid": instrument_id,
                "asof_date": tomorrow,
                "asof_ts": tomorrow_eod_ts,
            },
        )

    temporal_client = await Client.connect(
        os.environ["POLARIS_TEMPORAL_ADDRESS"],
        namespace=os.environ["POLARIS_TEMPORAL_NAMESPACE"],
    )

    async def process_block_staging(block_staging_id: str) -> list[str]:
        process_res = await fastapi_client.post(f"/staging-transactions/deals/{block_staging_id}/process")
        print(f"process 응답: {process_res.status_code}, {process_res.json()}")
        assert process_res.status_code == 200
        started = process_res.json()["started"]
        staging_ids: list[str] = []
        for item in started:
            staging_id = item["staging_id"]
            workflow_id = item["workflow_id"]
            await temporal_client.get_workflow_handle(workflow_id).result()
            r = await fastapi_client.get(f"/staging-transactions/{staging_id}")
            print(f"staging 상태({staging_id}): {r.status_code}, {r.json()}")
            assert r.status_code == 200
            assert r.json()["status"] == "settled"
            staging_ids.append(staging_id)
        return staging_ids

    async def assert_positions(expected_p1: str, expected_p2: str):
        async with db_engine.connect() as conn:
            p1_qty = (
                await conn.execute(
                    text(
                        """
                        SELECT quantity
                        FROM position_current
                        WHERE portfolio_id = :pid AND instrument_id = :iid
                        """
                    ),
                    {"pid": portfolio_1_id, "iid": instrument_id},
                )
            ).scalar_one()
            p2_qty = (
                await conn.execute(
                    text(
                        """
                        SELECT quantity
                        FROM position_current
                        WHERE portfolio_id = :pid AND instrument_id = :iid
                        """
                    ),
                    {"pid": portfolio_2_id, "iid": instrument_id},
                )
            ).scalar_one()
        print(f"포지션 수량 확인: P1={p1_qty}, P2={p2_qty}")
        assert Decimal(p1_qty) == Decimal(expected_p1)
        assert Decimal(p2_qty) == Decimal(expected_p2)

    async def assert_transaction_count(staging_ids: list[str], expected_count: int):
        async with db_engine.connect() as conn:
            total = 0
            for sid in staging_ids:
                c = (
                    await conn.execute(
                        text("SELECT count(*) FROM journal_entry WHERE pending_trade_id = :sid"),
                        {"sid": int(sid)},
                    )
                ).scalar_one()
                total += int(c)
        print(f"거래 건수 확인: {total}")
        assert total == expected_count

    async def assert_ibor(expected_p1: str, expected_p2: str):
        ibor_1 = await fastapi_client.get(f"/nav/ibor/{portfolio_1_id}")
        ibor_2 = await fastapi_client.get(f"/nav/ibor/{portfolio_2_id}")
        print(f"IBOR 응답 P1: {ibor_1.status_code}, {ibor_1.json()}")
        print(f"IBOR 응답 P2: {ibor_2.status_code}, {ibor_2.json()}")
        assert ibor_1.status_code == 200
        assert ibor_2.status_code == 200
        assert ibor_1.json()["nav_rc"] == expected_p1
        assert ibor_2.json()["nav_rc"] == expected_p2

    async def assert_abor(asof_date, expected_p1: str, expected_p2: str):
        abor_run_1 = await fastapi_client.post(
            f"/nav/abor/{portfolio_1_id}/run",
            json={"asof_date": asof_date.isoformat()},
        )
        abor_run_2 = await fastapi_client.post(
            f"/nav/abor/{portfolio_2_id}/run",
            json={"asof_date": asof_date.isoformat()},
        )
        assert abor_run_1.status_code == 200
        assert abor_run_2.status_code == 200
        await temporal_client.get_workflow_handle(abor_run_1.json()["workflow_id"]).result()
        await temporal_client.get_workflow_handle(abor_run_2.json()["workflow_id"]).result()

        abor_res_1 = await fastapi_client.get(
            f"/nav/abor/{portfolio_1_id}/result",
            params={"asof_date": asof_date.isoformat()},
        )
        abor_res_2 = await fastapi_client.get(
            f"/nav/abor/{portfolio_2_id}/result",
            params={"asof_date": asof_date.isoformat()},
        )
        print(f"ABOR 응답 P1: {abor_res_1.status_code}, {abor_res_1.json()}")
        print(f"ABOR 응답 P2: {abor_res_2.status_code}, {abor_res_2.json()}")
        assert abor_res_1.status_code == 200
        assert abor_res_2.status_code == 200
        assert abor_res_1.json()["nav_rc"] == expected_p1
        assert abor_res_2.json()["nav_rc"] == expected_p2

    all_staging_ids: list[str] = []

    print("\n[2단계] 원거래(BUY) 등록 및 반영")
    create_res = await fastapi_client.post(
        "/staging-transactions/deals",
        json={
            "transaction_type": "BuyEquity",
            "instrument_id": instrument_id,
            "trade_date": yesterday.isoformat(),
            "settle_date": today.isoformat(),
            "quantity": "300",
            "price": "500",
            "quote_currency": "USD",
            "report_currency": "USD",
            "allocations": [
                {"portfolio_id": portfolio_1_id, "quantity": "100"},
                {"portfolio_id": portfolio_2_id, "quantity": "200"},
            ],
        },
    )
    print(f"BUY 생성 응답: {create_res.status_code}, {create_res.json()}")
    assert create_res.status_code == 200
    deal_block_id = create_res.json()["deal_block_id"]
    buy_staging_ids = await process_block_staging(create_res.json()["block_staging_id"])
    all_staging_ids.extend(buy_staging_ids)

    print("\n[3단계] BUY 반영 확인(transaction/position/IBOR/ABOR)")
    await assert_transaction_count(all_staging_ids, expected_count=2)
    await assert_positions("100", "200")
    await assert_ibor("55000", "110000")
    await assert_abor(yesterday, "60000", "120000")

    print("\n[4단계] deal 수량 정정(MODIFY) 후 반영")
    modify_res = await fastapi_client.patch(
        f"/staging-transactions/deals/{deal_block_id}",
        json={
            "quantity": "450",
            "allocations": [
                {"portfolio_id": portfolio_1_id, "quantity": "150"},
                {"portfolio_id": portfolio_2_id, "quantity": "300"},
            ],
        },
    )
    print(f"MODIFY 응답: {modify_res.status_code}, {modify_res.json()}")
    assert modify_res.status_code == 200
    assert modify_res.json()["block_delta_quantity"] == "150"

    modify_staging_ids = await process_block_staging(modify_res.json()["block_staging_id"])
    all_staging_ids.extend(modify_staging_ids)

    print("\n[5단계] MODIFY 반영 확인(transaction/position/IBOR/ABOR)")
    await assert_transaction_count(all_staging_ids, expected_count=6)
    await assert_positions("150", "300")
    await assert_ibor("82500", "165000")
    await assert_abor(today, "81000", "162000")

    print("\n[6단계] deal 삭제(DELETE) 후 반영")
    delete_res = await fastapi_client.delete(f"/staging-transactions/deals/{deal_block_id}")
    print(f"DELETE 응답: {delete_res.status_code}, {delete_res.json()}")
    assert delete_res.status_code == 200
    assert delete_res.json()["block_delta_quantity"] == "-450"

    delete_staging_ids = await process_block_staging(delete_res.json()["block_staging_id"])
    all_staging_ids.extend(delete_staging_ids)

    print("\n[7단계] DELETE 반영 확인(transaction/position/IBOR/ABOR)")
    await assert_transaction_count(all_staging_ids, expected_count=8)
    await assert_positions("0", "0")
    await assert_ibor("0", "0")
    await assert_abor(tomorrow, "0", "0")

    async with db_engine.connect() as conn:
        lifecycle = (
            await conn.execute(
                text("SELECT lifecycle FROM deal_block WHERE id = :bid"),
                {"bid": deal_block_id},
            )
        ).scalar_one()
    print(f"deal_block lifecycle: {lifecycle}")
    assert lifecycle == "deleted"

    print("\n[최종] deal 정정/삭제에 따른 transaction/position/IBOR/ABOR 반영 검증 완료")
    print("================ [Deal 정정/삭제 시나리오 종료] ================\n")
