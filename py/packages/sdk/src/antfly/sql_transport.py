# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Bounded, non-redirecting SQL transport shared by generated and public clients.

No retry loop is introduced here. A caller-supplied HTTPX custom transport is
responsible for not replaying requests internally; HTTPX's default transport
does not retry possibly delivered requests.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

MAX_SQL_REQUEST_BYTES = 4 << 20
MAX_SQL_RESPONSE_BYTES = 16 << 20


def encode_sql_request(value: Any) -> bytes:
    # Preserve Python's exact integers; reject NaN/infinity rather than silently
    # emitting non-JSON values. Escape-expanded UTF-8 bytes count toward the cap.
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_SQL_REQUEST_BYTES:
        raise ValueError("SQL request exceeds 4 MiB")
    return encoded


def _kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    result = dict(kwargs)
    if "json" in result:
        result["content"] = encode_sql_request(result.pop("json"))
    result["follow_redirects"] = False
    return result


def _response(source: httpx.Response, body: bytearray) -> httpx.Response:
    # iter_bytes already decoded content encodings. Constructing a response
    # with the original encoding header would try to decompress it twice.
    headers = httpx.Headers(source.headers)
    headers.pop("content-encoding", None)
    headers.pop("content-length", None)
    return httpx.Response(source.status_code, headers=headers, content=bytes(body), request=source.request)


def sql_request(client: httpx.Client, **kwargs: Any) -> httpx.Response:
    with client.stream(**_kwargs(kwargs)) as response:
        body = bytearray()
        for chunk in response.iter_bytes(chunk_size=64 << 10):
            if len(body) + len(chunk) > MAX_SQL_RESPONSE_BYTES:
                raise ValueError("SQL response exceeds 16 MiB")
            body.extend(chunk)
        return _response(response, body)


async def sql_request_async(client: httpx.AsyncClient, **kwargs: Any) -> httpx.Response:
    async with client.stream(**_kwargs(kwargs)) as response:
        body = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=64 << 10):
            if len(body) + len(chunk) > MAX_SQL_RESPONSE_BYTES:
                raise ValueError("SQL response exceeds 16 MiB")
            body.extend(chunk)
        return _response(response, body)
