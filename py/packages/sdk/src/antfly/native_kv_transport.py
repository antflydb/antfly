"""Bounded asynchronous upload for generated native checkpoint operations."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any, BinaryIO

import httpx


async def native_kv_request_async(client: httpx.AsyncClient, **kwargs: Any) -> httpx.Response:
    """Stream the caller-owned file without buffering it or blocking the loop."""
    source: BinaryIO = kwargs["content"]

    async def chunks() -> AsyncIterator[bytes]:
        while chunk := await asyncio.to_thread(source.read, 64 * 1024):
            yield chunk

    kwargs["content"] = chunks()
    return await client.request(**kwargs)
