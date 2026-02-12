from __future__ import annotations

from temporalio.client import Client

from ..settings import settings


_client: Client | None = None


async def get_temporal_client() -> Client:
    global _client
    if _client is None:
        _client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    return _client
