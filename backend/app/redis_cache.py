from __future__ import annotations

import asyncio
import json
from typing import Any

import redis.asyncio as redis

from .settings import settings


def _dumps(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), sort_keys=True, default=str)


class RedisCache:
    def __init__(self) -> None:
        self._client: redis.Redis | None = None
        self._loop_id: int | None = None

    async def _get_client(self) -> redis.Redis:
        loop = asyncio.get_running_loop()
        current_loop_id = id(loop)
        if self._client is None or self._loop_id != current_loop_id:
            if self._client is not None:
                try:
                    await self._client.aclose()
                except Exception:
                    pass
            self._client = redis.from_url(settings.redis_url)
            self._loop_id = current_loop_id
        return self._client

    async def _set_json(self, *, key: str, payload: dict[str, Any]) -> None:
        client = await self._get_client()
        try:
            await client.set(key, _dumps(payload))
        except RuntimeError:
            self._client = redis.from_url(settings.redis_url)
            self._loop_id = id(asyncio.get_running_loop())
            await self._client.set(key, _dumps(payload))

    async def set_position(self, *, portfolio_id: str, instrument_id: str, payload: dict[str, Any]) -> None:
        key = f"position:{portfolio_id}:{instrument_id}"
        await self._set_json(key=key, payload=payload)

    async def set_ibor_nav(self, *, portfolio_id: str, payload: dict[str, Any]) -> None:
        key = f"nav:ibor:{portfolio_id}"
        await self._set_json(key=key, payload=payload)


redis_cache = RedisCache()
