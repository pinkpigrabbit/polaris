from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from ..settings import settings
from .activities import (
    abor_nav_compute_activity,
    abor_nav_snapshot_positions_activity,
    allocation_activity,
    ca_process_event_activity,
    position_activity,
    precheck_activity,
    settle_activity,
)
from .workflows import AborNavWorkflow, CorporateActionWorkflow, StagingTransactionWorkflow


async def main() -> None:
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
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
