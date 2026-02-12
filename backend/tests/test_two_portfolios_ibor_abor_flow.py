from __future__ import annotations

import os
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import text
from temporalio.client import Client


@pytest.mark.usefixtures("temporal_worker")
async def test_two_portfolios_aapl_buy_flow_to_ibor_and_abor(
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
    now_utc = datetime.now(tz=timezone.utc)
    yesterday_eod_ts = datetime.combine(yesterday, time(23, 0, 0), tzinfo=timezone.utc)
    today_price_ts = now_utc - timedelta(minutes=1)

    token_1 = uuid4().hex[:8]
    token_2 = uuid4().hex[:8]

    print("\n================ [시나리오 시작] ================")
    print(f"오늘 기준일: {today.isoformat()}")
    print(f"어제 기준일: {yesterday.isoformat()}")
    print("전제: AAPL US를 어제 USD 500에 매수")
    print("포트폴리오1 수량=100, 포트폴리오2 수량=200")
    print("가격 가정: 어제 EOD=600, 오늘(IBOR 기준)=550")

    print("\n[1단계] 기준 데이터 준비(currency/portfolio/instrument/market_price)")
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
                "code": f"P1-{token_1}",
                "name": "테스트 포트폴리오 1",
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
                "code": f"P2-{token_2}",
                "name": "테스트 포트폴리오 2",
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

    print(f"포트폴리오1 ID: {portfolio_1_id}")
    print(f"포트폴리오2 ID: {portfolio_2_id}")
    print(f"AAPL US instrument_id: {instrument_id}")

    temporal_client = await Client.connect(
        os.environ["POLARIS_TEMPORAL_ADDRESS"],
        namespace=os.environ["POLARIS_TEMPORAL_NAMESPACE"],
    )

    print("\n[2단계] 화면 입력 1회 호출: block + allocation 분해 생성")
    deal_req = {
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
    }
    print(f"deal 요청 바디: {deal_req}")
    create_res = await fastapi_client.post("/staging-transactions/deals", json=deal_req)
    print(f"deal 생성 응답 코드: {create_res.status_code}")
    print(f"deal 생성 응답 바디: {create_res.json()}")
    assert create_res.status_code == 200

    block_staging_id = create_res.json()["block_staging_id"]
    allocation_stagings = create_res.json()["allocation_stagings"]
    assert len(allocation_stagings) == 2

    print("\n[3단계] 분해 결과 검증(block/alloc 금액 합계)")
    allocation_amount_sum = sum(Decimal(item["amount_qc"]) for item in allocation_stagings)
    block_amount_qc = Decimal(create_res.json()["block_amount_qc"])
    print(f"block_amount_qc: {block_amount_qc}")
    print(f"allocation_amount_qc 합계: {allocation_amount_sum}")
    assert block_amount_qc == Decimal("150000")
    assert allocation_amount_sum == block_amount_qc

    print("\n[4단계] deal 단위 process 호출(내부에서 allocation workflow들 시작)")
    process_res = await fastapi_client.post(f"/staging-transactions/deals/{block_staging_id}/process")
    print(f"deal process 응답 코드: {process_res.status_code}")
    print(f"deal process 응답 바디: {process_res.json()}")
    assert process_res.status_code == 200

    started = process_res.json()["started"]
    assert len(started) == 2
    allocation_staging_ids = [item["staging_id"] for item in started]
    workflow_ids = [item["workflow_id"] for item in started]

    for wf_id in workflow_ids:
        print(f"Temporal workflow 완료 대기: {wf_id}")
        await temporal_client.get_workflow_handle(wf_id).result()
        print(f"Temporal workflow 완료: {wf_id}")

    print("\n[4-1단계] allocation staging 상태/전이 이력 검증")
    for sid in allocation_staging_ids:
        r = await fastapi_client.get(f"/staging-transactions/{sid}")
        print(f"staging 조회({sid}) 코드: {r.status_code}, 바디: {r.json()}")
        assert r.status_code == 200
        assert r.json()["status"] == "settled"

        async with db_engine.connect() as conn:
            txn_count = (
                await conn.execute(
                    text("SELECT count(*) FROM journal_entry WHERE pending_trade_id = :sid"),
                    {"sid": int(sid)},
                )
            ).scalar_one()
            print(f"계정 분개 개수({sid}): {txn_count}")
            assert txn_count >= 1

    print("\n[4단계] 포지션 수량 검증(매수 반영)")
    async with db_engine.connect() as conn:
        qty_1 = (
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
        qty_2 = (
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

    print(f"포트폴리오1 현재 수량: {qty_1}")
    print(f"포트폴리오2 현재 수량: {qty_2}")
    assert Decimal(qty_1) == Decimal("100")
    assert Decimal(qty_2) == Decimal("200")

    print("\n[5단계] IBOR(오늘 기준, 가격 550) 검증")
    ibor_1 = await fastapi_client.get(f"/nav/ibor/{portfolio_1_id}")
    ibor_2 = await fastapi_client.get(f"/nav/ibor/{portfolio_2_id}")
    print(f"IBOR 응답(포트폴리오1): {ibor_1.status_code}, {ibor_1.json()}")
    print(f"IBOR 응답(포트폴리오2): {ibor_2.status_code}, {ibor_2.json()}")
    assert ibor_1.status_code == 200
    assert ibor_2.status_code == 200
    assert ibor_1.json()["valuation_basis"] == "IBOR"
    assert ibor_2.json()["valuation_basis"] == "IBOR"
    assert ibor_1.json()["nav_rc"] == "55000"
    assert ibor_2.json()["nav_rc"] == "110000"

    print("\n[6단계] ABOR(어제 기준, EOD 가격 600) 검증")
    abor_run_1 = await fastapi_client.post(
        f"/nav/abor/{portfolio_1_id}/run",
        json={"asof_date": yesterday.isoformat()},
    )
    abor_run_2 = await fastapi_client.post(
        f"/nav/abor/{portfolio_2_id}/run",
        json={"asof_date": yesterday.isoformat()},
    )
    print(f"ABOR run 응답(포트폴리오1): {abor_run_1.status_code}, {abor_run_1.json()}")
    print(f"ABOR run 응답(포트폴리오2): {abor_run_2.status_code}, {abor_run_2.json()}")
    assert abor_run_1.status_code == 200
    assert abor_run_2.status_code == 200

    abor_workflow_id_1 = abor_run_1.json()["workflow_id"]
    abor_workflow_id_2 = abor_run_2.json()["workflow_id"]
    await temporal_client.get_workflow_handle(abor_workflow_id_1).result()
    await temporal_client.get_workflow_handle(abor_workflow_id_2).result()

    abor_res_1 = await fastapi_client.get(
        f"/nav/abor/{portfolio_1_id}/result",
        params={"asof_date": yesterday.isoformat()},
    )
    abor_res_2 = await fastapi_client.get(
        f"/nav/abor/{portfolio_2_id}/result",
        params={"asof_date": yesterday.isoformat()},
    )
    print(f"ABOR 결과 응답(포트폴리오1): {abor_res_1.status_code}, {abor_res_1.json()}")
    print(f"ABOR 결과 응답(포트폴리오2): {abor_res_2.status_code}, {abor_res_2.json()}")
    assert abor_res_1.status_code == 200
    assert abor_res_2.status_code == 200
    assert abor_res_1.json()["nav_rc"] == "60000"
    assert abor_res_2.json()["nav_rc"] == "120000"

    print("\n[최종 검증 완료]")
    print(f"block staging ID: {block_staging_id}")
    print(f"allocation staging IDs: {allocation_staging_ids}")
    print("IBOR(오늘, 550) / ABOR(어제, 600) 값이 기대치와 일치합니다.")
    print("================ [시나리오 종료] ================\n")


@pytest.mark.usefixtures("temporal_worker")
async def test_two_instruments_separate_buy_sum_flow(
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
    now_utc = datetime.now(tz=timezone.utc)
    yesterday_ts = datetime.combine(yesterday, time(23, 1, 0), tzinfo=timezone.utc)
    today_ts = now_utc - timedelta(minutes=2)

    token_1 = uuid4().hex[:8]
    token_2 = uuid4().hex[:8]

    print("\n================ [2종목 합산 시나리오 시작] ================")
    print(f"기준일(어제): {yesterday.isoformat()}, 기준일(오늘): {today.isoformat()}")

    print("\n[1단계] 마스터/가격 데이터 준비")
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
                "code": f"SUM-P1-{token_1}",
                "name": "2종목 합산 테스트 포트폴리오 1",
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
                "code": f"SUM-P2-{token_2}",
                "name": "2종목 합산 테스트 포트폴리오 2",
            },
        )
        portfolio_2_id = str(p2_row.scalar_one())

        aapl_row = await conn.execute(
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
        aapl_id = str(aapl_row.scalar_one())

        msft_row = await conn.execute(
            text(
                """
                    INSERT INTO instrument(instrument_type, symbol, name, currency, lifecycle, created_at, updated_at)
                    VALUES ('stock', 'MSFT US', 'Microsoft Corp. US', 'USD', 'active', now(), now())
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
        msft_id = str(msft_row.scalar_one())

        await conn.execute(
            text(
                """
                INSERT INTO market_price(instrument_id, asof_date, asof_ts, price, currency, is_eod)
                VALUES (:iid, :asof_date, :asof_ts, :price, 'USD', TRUE)
                """
            ),
            {"iid": aapl_id, "asof_date": yesterday, "asof_ts": yesterday_ts, "price": Decimal("600")},
        )
        await conn.execute(
            text(
                """
                INSERT INTO market_price(instrument_id, asof_date, asof_ts, price, currency, is_eod)
                VALUES (:iid, :asof_date, :asof_ts, :price, 'USD', TRUE)
                """
            ),
            {"iid": msft_id, "asof_date": yesterday, "asof_ts": yesterday_ts, "price": Decimal("400")},
        )
        await conn.execute(
            text(
                """
                INSERT INTO market_price(instrument_id, asof_date, asof_ts, price, currency, is_eod)
                VALUES (:iid, :asof_date, :asof_ts, :price, 'USD', TRUE)
                """
            ),
            {"iid": aapl_id, "asof_date": today, "asof_ts": today_ts, "price": Decimal("550")},
        )
        await conn.execute(
            text(
                """
                INSERT INTO market_price(instrument_id, asof_date, asof_ts, price, currency, is_eod)
                VALUES (:iid, :asof_date, :asof_ts, :price, 'USD', TRUE)
                """
            ),
            {"iid": msft_id, "asof_date": today, "asof_ts": today_ts, "price": Decimal("420")},
        )

    print(f"포트폴리오1: {portfolio_1_id}")
    print(f"포트폴리오2: {portfolio_2_id}")

    temporal_client = await Client.connect(
        os.environ["POLARIS_TEMPORAL_ADDRESS"],
        namespace=os.environ["POLARIS_TEMPORAL_NAMESPACE"],
    )

    async def create_and_process_symbol_trade(
        symbol: str,
        transaction_type: str,
        instrument_id: str,
        total_quantity: str,
        price: str,
        quantity_p1: str,
        quantity_p2: str,
    ) -> list[str]:
        print(
            f"\n[2단계] {symbol} {transaction_type} 호출(총수량={total_quantity}, 가격={price}, "
            f"포트폴리오1={quantity_p1}, 포트폴리오2={quantity_p2})"
        )
        deal_req = {
            "transaction_type": transaction_type,
            "instrument_id": instrument_id,
            "trade_date": yesterday.isoformat(),
            "settle_date": today.isoformat(),
            "quantity": total_quantity,
            "price": price,
            "quote_currency": "USD",
            "report_currency": "USD",
            "allocations": [
                {"portfolio_id": portfolio_1_id, "quantity": quantity_p1},
                {"portfolio_id": portfolio_2_id, "quantity": quantity_p2},
            ],
        }
        create_res = await fastapi_client.post("/staging-transactions/deals", json=deal_req)
        print(f"{symbol} deal 생성 응답: {create_res.status_code}, {create_res.json()}")
        assert create_res.status_code == 200

        block_staging_id = create_res.json()["block_staging_id"]
        process_res = await fastapi_client.post(f"/staging-transactions/deals/{block_staging_id}/process")
        print(f"{symbol} deal process 응답: {process_res.status_code}, {process_res.json()}")
        assert process_res.status_code == 200

        started = process_res.json()["started"]
        assert len(started) == 2

        staging_ids: list[str] = []
        for item in started:
            staging_id = item["staging_id"]
            workflow_id = item["workflow_id"]
            await temporal_client.get_workflow_handle(workflow_id).result()
            r = await fastapi_client.get(f"/staging-transactions/{staging_id}")
            print(f"{symbol} staging 상태: {r.status_code}, {r.json()}")
            assert r.status_code == 200
            assert r.json()["status"] == "settled"
            staging_ids.append(staging_id)
        return staging_ids

    aapl_staging_ids = await create_and_process_symbol_trade(
        "AAPL US",
        "BuyEquity",
        aapl_id,
        "300",
        "500",
        "100",
        "200",
    )
    msft_staging_ids = await create_and_process_symbol_trade(
        "MSFT US",
        "BuyEquity",
        msft_id,
        "300",
        "300",
        "100",
        "200",
    )
    all_staging_ids = aapl_staging_ids + msft_staging_ids

    print("\n[3단계] transactions/positions 합산 검증")
    async with db_engine.connect() as conn:
        txn_count = (
            await conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM journal_entry
                    WHERE pending_trade_id IN (:s1, :s2, :s3, :s4)
                    """
                ),
                {
                    "s1": all_staging_ids[0],
                    "s2": all_staging_ids[1],
                    "s3": all_staging_ids[2],
                    "s4": all_staging_ids[3],
                },
            )
        ).scalar_one()
        assert txn_count == 4

        position_amount_sum = (
            await conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(ae.amount), 0)
                    FROM journal_entry_line ae
                    JOIN journal_entry atx ON atx.id = ae.journal_entry_id
                    WHERE atx.pending_trade_id IN (:s1, :s2, :s3, :s4)
                      AND ae.account_code = 'POSITION'
                    """
                ),
                {
                    "s1": all_staging_ids[0],
                    "s2": all_staging_ids[1],
                    "s3": all_staging_ids[2],
                    "s4": all_staging_ids[3],
                },
            )
        ).scalar_one()
        print(f"POSITION 금액 합계: {position_amount_sum}")
        assert Decimal(position_amount_sum) == Decimal("240000")

        p1_aapl_qty = (
            await conn.execute(
                text(
                    """
                    SELECT quantity
                    FROM position_current
                    WHERE portfolio_id = :pid AND instrument_id = :iid
                    """
                ),
                {"pid": portfolio_1_id, "iid": aapl_id},
            )
        ).scalar_one()
        p2_aapl_qty = (
            await conn.execute(
                text(
                    """
                    SELECT quantity
                    FROM position_current
                    WHERE portfolio_id = :pid AND instrument_id = :iid
                    """
                ),
                {"pid": portfolio_2_id, "iid": aapl_id},
            )
        ).scalar_one()
        p1_msft_qty = (
            await conn.execute(
                text(
                    """
                    SELECT quantity
                    FROM position_current
                    WHERE portfolio_id = :pid AND instrument_id = :iid
                    """
                ),
                {"pid": portfolio_1_id, "iid": msft_id},
            )
        ).scalar_one()
        p2_msft_qty = (
            await conn.execute(
                text(
                    """
                    SELECT quantity
                    FROM position_current
                    WHERE portfolio_id = :pid AND instrument_id = :iid
                    """
                ),
                {"pid": portfolio_2_id, "iid": msft_id},
            )
        ).scalar_one()
        print(
            f"포지션 수량 P1(AAPL={p1_aapl_qty},MSFT={p1_msft_qty}), "
            f"P2(AAPL={p2_aapl_qty},MSFT={p2_msft_qty})"
        )
        assert Decimal(p1_aapl_qty) == Decimal("100")
        assert Decimal(p2_aapl_qty) == Decimal("200")
        assert Decimal(p1_msft_qty) == Decimal("100")
        assert Decimal(p2_msft_qty) == Decimal("200")

    print("\n[4단계] IBOR 합산 검증(오늘 가격: AAPL 550 + MSFT 420)")
    ibor_1 = await fastapi_client.get(f"/nav/ibor/{portfolio_1_id}")
    ibor_2 = await fastapi_client.get(f"/nav/ibor/{portfolio_2_id}")
    print(f"IBOR 응답 P1: {ibor_1.status_code}, {ibor_1.json()}")
    print(f"IBOR 응답 P2: {ibor_2.status_code}, {ibor_2.json()}")
    assert ibor_1.status_code == 200
    assert ibor_2.status_code == 200
    assert ibor_1.json()["nav_rc"] == "97000"
    assert ibor_2.json()["nav_rc"] == "194000"

    ibor_snapshot_1 = await fastapi_client.post(f"/nav/ibor/{portfolio_1_id}/snapshot")
    ibor_snapshot_2 = await fastapi_client.post(f"/nav/ibor/{portfolio_2_id}/snapshot")
    assert ibor_snapshot_1.status_code == 200
    assert ibor_snapshot_2.status_code == 200
    ibor_run_id_1 = ibor_snapshot_1.json()["nav_run_id"]
    ibor_run_id_2 = ibor_snapshot_2.json()["nav_run_id"]

    async with db_engine.connect() as conn:
        ibor_nav_value_1 = (
            await conn.execute(
                text("SELECT nav_rc FROM ibor_nav_result WHERE ibor_nav_run_id = :rid"),
                {"rid": ibor_run_id_1},
            )
        ).scalar_one()
        ibor_nav_value_2 = (
            await conn.execute(
                text("SELECT nav_rc FROM ibor_nav_result WHERE ibor_nav_run_id = :rid"),
                {"rid": ibor_run_id_2},
            )
        ).scalar_one()
        assert Decimal(ibor_nav_value_1) == Decimal("97000")
        assert Decimal(ibor_nav_value_2) == Decimal("194000")

    print("\n[5단계] ABOR 합산 검증(어제 EOD: AAPL 600 + MSFT 400)")
    abor_run_1 = await fastapi_client.post(
        f"/nav/abor/{portfolio_1_id}/run",
        json={"asof_date": yesterday.isoformat()},
    )
    abor_run_2 = await fastapi_client.post(
        f"/nav/abor/{portfolio_2_id}/run",
        json={"asof_date": yesterday.isoformat()},
    )
    assert abor_run_1.status_code == 200
    assert abor_run_2.status_code == 200
    await temporal_client.get_workflow_handle(abor_run_1.json()["workflow_id"]).result()
    await temporal_client.get_workflow_handle(abor_run_2.json()["workflow_id"]).result()

    abor_result_1 = await fastapi_client.get(
        f"/nav/abor/{portfolio_1_id}/result",
        params={"asof_date": yesterday.isoformat()},
    )
    abor_result_2 = await fastapi_client.get(
        f"/nav/abor/{portfolio_2_id}/result",
        params={"asof_date": yesterday.isoformat()},
    )
    print(f"ABOR result 응답 P1: {abor_result_1.status_code}, {abor_result_1.json()}")
    print(f"ABOR result 응답 P2: {abor_result_2.status_code}, {abor_result_2.json()}")
    assert abor_result_1.status_code == 200
    assert abor_result_2.status_code == 200
    assert abor_result_1.json()["nav_rc"] == "100000"
    assert abor_result_2.json()["nav_rc"] == "200000"

    abor_run_id_1 = abor_result_1.json()["nav_run_id"]
    abor_run_id_2 = abor_result_2.json()["nav_run_id"]

    async with db_engine.connect() as conn:
        abor_line_sum_1 = (
            await conn.execute(
                text("SELECT COALESCE(SUM(market_value_rc), 0) FROM abor_nav_line_item WHERE abor_nav_run_id = :rid"),
                {"rid": abor_run_id_1},
            )
        ).scalar_one()
        abor_line_sum_2 = (
            await conn.execute(
                text("SELECT COALESCE(SUM(market_value_rc), 0) FROM abor_nav_line_item WHERE abor_nav_run_id = :rid"),
                {"rid": abor_run_id_2},
            )
        ).scalar_one()
        assert Decimal(abor_line_sum_1) == Decimal("100000")
        assert Decimal(abor_line_sum_2) == Decimal("200000")

    print("\n[6단계] 두 포트폴리오에서 두 종목을 절반씩 SELL")
    sell_aapl_staging_ids = await create_and_process_symbol_trade(
        "AAPL US",
        "SellEquity",
        aapl_id,
        "150",
        "500",
        "50",
        "100",
    )
    sell_msft_staging_ids = await create_and_process_symbol_trade(
        "MSFT US",
        "SellEquity",
        msft_id,
        "150",
        "300",
        "50",
        "100",
    )
    sell_staging_ids = sell_aapl_staging_ids + sell_msft_staging_ids
    all_staging_ids = all_staging_ids + sell_staging_ids

    print("\n[7단계] SELL 이후 transactions/positions 재검증")
    async with db_engine.connect() as conn:
        total_txn_count = (
            await conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM journal_entry
                    WHERE pending_trade_id IN (:s1, :s2, :s3, :s4, :s5, :s6, :s7, :s8)
                    """
                ),
                {
                    "s1": all_staging_ids[0],
                    "s2": all_staging_ids[1],
                    "s3": all_staging_ids[2],
                    "s4": all_staging_ids[3],
                    "s5": all_staging_ids[4],
                    "s6": all_staging_ids[5],
                    "s7": all_staging_ids[6],
                    "s8": all_staging_ids[7],
                },
            )
        ).scalar_one()
        assert total_txn_count == 8

        sell_position_qty_sum = (
            await conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(ae.quantity), 0)
                    FROM journal_entry_line ae
                    JOIN journal_entry atx ON atx.id = ae.journal_entry_id
                    WHERE atx.pending_trade_id IN (:s5, :s6, :s7, :s8)
                      AND ae.account_code = 'POSITION'
                    """
                ),
                {
                    "s5": all_staging_ids[4],
                    "s6": all_staging_ids[5],
                    "s7": all_staging_ids[6],
                    "s8": all_staging_ids[7],
                },
            )
        ).scalar_one()
        print(f"SELL POSITION 수량 합계: {sell_position_qty_sum}")
        assert Decimal(sell_position_qty_sum) == Decimal("-300")

        p1_aapl_qty_after_sell = (
            await conn.execute(
                text(
                    """
                    SELECT quantity
                    FROM position_current
                    WHERE portfolio_id = :pid AND instrument_id = :iid
                    """
                ),
                {"pid": portfolio_1_id, "iid": aapl_id},
            )
        ).scalar_one()
        p2_aapl_qty_after_sell = (
            await conn.execute(
                text(
                    """
                    SELECT quantity
                    FROM position_current
                    WHERE portfolio_id = :pid AND instrument_id = :iid
                    """
                ),
                {"pid": portfolio_2_id, "iid": aapl_id},
            )
        ).scalar_one()
        p1_msft_qty_after_sell = (
            await conn.execute(
                text(
                    """
                    SELECT quantity
                    FROM position_current
                    WHERE portfolio_id = :pid AND instrument_id = :iid
                    """
                ),
                {"pid": portfolio_1_id, "iid": msft_id},
            )
        ).scalar_one()
        p2_msft_qty_after_sell = (
            await conn.execute(
                text(
                    """
                    SELECT quantity
                    FROM position_current
                    WHERE portfolio_id = :pid AND instrument_id = :iid
                    """
                ),
                {"pid": portfolio_2_id, "iid": msft_id},
            )
        ).scalar_one()
        print(
            f"SELL 후 포지션 P1(AAPL={p1_aapl_qty_after_sell},MSFT={p1_msft_qty_after_sell}), "
            f"P2(AAPL={p2_aapl_qty_after_sell},MSFT={p2_msft_qty_after_sell})"
        )
        assert Decimal(p1_aapl_qty_after_sell) == Decimal("50")
        assert Decimal(p2_aapl_qty_after_sell) == Decimal("100")
        assert Decimal(p1_msft_qty_after_sell) == Decimal("50")
        assert Decimal(p2_msft_qty_after_sell) == Decimal("100")

    print("\n[8단계] SELL 이후 IBOR 재검증")
    ibor_after_sell_1 = await fastapi_client.get(f"/nav/ibor/{portfolio_1_id}")
    ibor_after_sell_2 = await fastapi_client.get(f"/nav/ibor/{portfolio_2_id}")
    print(f"SELL 후 IBOR 응답 P1: {ibor_after_sell_1.status_code}, {ibor_after_sell_1.json()}")
    print(f"SELL 후 IBOR 응답 P2: {ibor_after_sell_2.status_code}, {ibor_after_sell_2.json()}")
    assert ibor_after_sell_1.status_code == 200
    assert ibor_after_sell_2.status_code == 200
    assert ibor_after_sell_1.json()["nav_rc"] == "48500"
    assert ibor_after_sell_2.json()["nav_rc"] == "97000"

    print("\n[9단계] SELL 이후 ABOR 재검증(오늘 EOD: AAPL 550 + MSFT 420)")
    abor_after_sell_run_1 = await fastapi_client.post(
        f"/nav/abor/{portfolio_1_id}/run",
        json={"asof_date": today.isoformat()},
    )
    abor_after_sell_run_2 = await fastapi_client.post(
        f"/nav/abor/{portfolio_2_id}/run",
        json={"asof_date": today.isoformat()},
    )
    assert abor_after_sell_run_1.status_code == 200
    assert abor_after_sell_run_2.status_code == 200
    await temporal_client.get_workflow_handle(abor_after_sell_run_1.json()["workflow_id"]).result()
    await temporal_client.get_workflow_handle(abor_after_sell_run_2.json()["workflow_id"]).result()

    abor_after_sell_result_1 = await fastapi_client.get(
        f"/nav/abor/{portfolio_1_id}/result",
        params={"asof_date": today.isoformat()},
    )
    abor_after_sell_result_2 = await fastapi_client.get(
        f"/nav/abor/{portfolio_2_id}/result",
        params={"asof_date": today.isoformat()},
    )
    print(f"SELL 후 ABOR result 응답 P1: {abor_after_sell_result_1.status_code}, {abor_after_sell_result_1.json()}")
    print(f"SELL 후 ABOR result 응답 P2: {abor_after_sell_result_2.status_code}, {abor_after_sell_result_2.json()}")
    assert abor_after_sell_result_1.status_code == 200
    assert abor_after_sell_result_2.status_code == 200
    assert abor_after_sell_result_1.json()["nav_rc"] == "48000"
    assert abor_after_sell_result_2.json()["nav_rc"] == "96000"

    print("\n[최종] BUY 후 SELL(절반 청산)까지 transaction/position/IBOR/ABOR 반영 검증이 완료되었습니다.")
    print("================ [2종목 합산 시나리오 종료] ================\n")
