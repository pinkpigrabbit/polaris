from __future__ import annotations

import os
import sys
from pathlib import Path
import asyncio
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio


# Allow `import app` when running `pytest` from repo root.
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


if sys.platform.startswith("win") and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# Ensure env defaults exist before importing app modules that eagerly create Settings/Engine.
os.environ.setdefault("POLARIS_DATABASE_URL", "postgresql+psycopg://polaris:polaris@127.0.0.1:5433/polaris")
os.environ.setdefault("POLARIS_REDIS_URL", "redis://localhost:6380/0")
os.environ.setdefault("POLARIS_TEMPORAL_ADDRESS", "localhost:7233")
os.environ.setdefault("POLARIS_TEMPORAL_NAMESPACE", "default")
_TEST_TASK_QUEUE = f"staging-txns-test-{uuid4().hex[:8]}"
os.environ.setdefault("POLARIS_TEMPORAL_TASK_QUEUE", _TEST_TASK_QUEUE)


@pytest_asyncio.fixture(scope="session")
async def db_engine():
    from app.db.session import engine

    return engine


@pytest_asyncio.fixture(scope="session")
async def db_has_schema(db_engine) -> bool:
    from sqlalchemy import text

    try:
        async with db_engine.connect() as conn:
            res = await conn.execute(
                text(
                    """
                    SELECT
                      to_regclass('public.pending_trade') IS NOT NULL
                      AND to_regclass('public.ibor_nav_run') IS NOT NULL
                      AND to_regclass('public.abor_nav_run') IS NOT NULL
                      AND to_regclass('public.ca_event') IS NOT NULL
                    """
                )
            )
            return bool(res.scalar_one())
    except Exception:
        return False


@pytest_asyncio.fixture(scope="session")
async def redis_available() -> bool:
    import redis.asyncio as redis

    url = os.environ["POLARIS_REDIS_URL"]
    try:
        r = redis.from_url(url)
        await r.execute_command("PING")
        await r.aclose()
        return True
    except Exception:
        return False


@pytest_asyncio.fixture(scope="session", autouse=True)
async def ensure_temporal_server() -> AsyncIterator[None]:
    """Ensure Temporal server exists for integration tests.

    - Use externally provided Temporal if reachable.
    - Otherwise start Temporal test server via temporalio.testing.
    """

    from temporalio.client import Client

    address = os.environ["POLARIS_TEMPORAL_ADDRESS"]
    namespace = os.environ["POLARIS_TEMPORAL_NAMESPACE"]

    try:
        await Client.connect(address, namespace=namespace)
        yield
        return
    except Exception:
        pass

    from temporalio.testing import WorkflowEnvironment

    host, _, port_text = address.partition(":")
    port = int(port_text) if port_text else 7233
    bind_ip = "127.0.0.1" if host in ("localhost", "127.0.0.1") else host

    env = await WorkflowEnvironment.start_local(
        namespace=namespace,
        ip=bind_ip,
        port=port,
    )
    os.environ["POLARIS_TEMPORAL_ADDRESS"] = f"{bind_ip}:{port}"
    os.environ["POLARIS_TEMPORAL_NAMESPACE"] = namespace
    try:
        yield
    finally:
        await env.shutdown()


@pytest_asyncio.fixture
async def seed_master_data(db_engine, db_has_schema) -> dict:
    """Seed minimal reference rows required by FK constraints.

    Inserts:
    - currency(USD)
    - portfolio
    - instrument

    If schema isn't present, the test will be skipped.
    """

    if not db_has_schema:
        pytest.skip("DB schema not found. Apply db/schema.sql before running tests.")

    from sqlalchemy import text

    token = uuid4().hex[:8]

    async with db_engine.begin() as conn:
        await conn.execute(text("INSERT INTO currency(code, name) VALUES ('USD','US Dollar') ON CONFLICT DO NOTHING"))
        portfolio_row = await conn.execute(
            text(
                """
                INSERT INTO portfolio(code, name, report_currency)
                VALUES (:code, :name, 'USD')
                RETURNING id
                """
            ),
            {"code": f"T-{token}", "name": "Test Portfolio"},
        )
        portfolio_id = int(portfolio_row.scalar_one())

        instrument_row = await conn.execute(
            text(
                """
                INSERT INTO instrument(instrument_type, security_id, name, currency, lifecycle, created_at, updated_at)
                VALUES ('stock', :security_id, 'Test Instrument', 'USD', 'active', now(), now())
                RETURNING id
                """
            ),
            {"security_id": f"TEST{token}"},
        )
        instrument_id = int(instrument_row.scalar_one())

    return {"portfolio_id": str(portfolio_id), "instrument_id": str(instrument_id)}


@pytest_asyncio.fixture
async def fastapi_client() -> AsyncIterator:
    import httpx

    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture(scope="session")
async def temporal_available(ensure_temporal_server) -> bool:
    try:
        from temporalio.client import Client

        await Client.connect(
            os.environ["POLARIS_TEMPORAL_ADDRESS"],
            namespace=os.environ["POLARIS_TEMPORAL_NAMESPACE"],
        )
        return True
    except Exception:
        return False


@pytest_asyncio.fixture
async def temporal_worker(temporal_available):
    """Start an in-process Temporal worker for integration tests."""

    if not temporal_available:
        pytest.skip("Temporal server not reachable on POLARIS_TEMPORAL_ADDRESS")

    from temporalio.client import Client
    from temporalio.worker import Worker

    from app.settings import settings
    from app.temporal.activities import (
        abor_nav_compute_activity,
        abor_nav_snapshot_positions_activity,
        allocation_activity,
        ca_process_event_activity,
        position_activity,
        precheck_activity,
        settle_activity,
    )
    from app.temporal.workflows import AborNavWorkflow, CorporateActionWorkflow, StagingTransactionWorkflow

    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    async with Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[StagingTransactionWorkflow, AborNavWorkflow, CorporateActionWorkflow],
        activities=[
            precheck_activity,
            position_activity,
            allocation_activity,
            settle_activity,
            abor_nav_snapshot_positions_activity,
            abor_nav_compute_activity,
            ca_process_event_activity,
        ],
    ):
        yield
