from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from .activities import (
    abor_nav_compute_activity,
    abor_nav_snapshot_positions_activity,
    allocation_activity,
    ca_process_event_activity,
    position_activity,
    precheck_activity,
    settle_activity,
)


@workflow.defn
class StagingTransactionWorkflow:
    @workflow.run
    async def run(self, staging_id: str) -> str:
        retry = RetryPolicy(maximum_attempts=10)

        await workflow.execute_activity(
            precheck_activity,
            staging_id,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=retry,
        )
        await workflow.execute_activity(
            position_activity,
            staging_id,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry,
        )
        await workflow.execute_activity(
            allocation_activity,
            staging_id,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry,
        )
        await workflow.execute_activity(
            settle_activity,
            staging_id,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry,
        )
        return "ok"


@workflow.defn
class AborNavWorkflow:
    @workflow.run
    async def run(self, portfolio_id: str, asof_date: str) -> str:
        retry = RetryPolicy(maximum_attempts=10)

        await workflow.execute_activity(
            abor_nav_snapshot_positions_activity,
            args=[portfolio_id, asof_date],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry,
        )
        await workflow.execute_activity(
            abor_nav_compute_activity,
            args=[portfolio_id, asof_date],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry,
        )
        return "ok"


@workflow.defn
class CorporateActionWorkflow:
    @workflow.run
    async def run(self, ca_event_id: str) -> str:
        retry = RetryPolicy(maximum_attempts=10)
        await workflow.execute_activity(
            ca_process_event_activity,
            ca_event_id,
            start_to_close_timeout=timedelta(seconds=300),
            retry_policy=retry,
        )
        return "ok"
