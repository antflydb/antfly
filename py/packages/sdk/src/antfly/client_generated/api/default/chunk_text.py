from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.termite_chunk_request import TermiteChunkRequest
from ...models.termite_chunk_response import TermiteChunkResponse
from ...models.termite_error import TermiteError
from ...types import Response


def _get_kwargs(
    *,
    body: TermiteChunkRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ml/v1/chunk",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> TermiteChunkResponse | TermiteError | None:
    if response.status_code == 200:
        response_200 = TermiteChunkResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = TermiteError.from_dict(response.json())

        return response_400

    if response.status_code == 500:
        response_500 = TermiteError.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[TermiteChunkResponse | TermiteError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteChunkRequest,
) -> Response[TermiteChunkResponse | TermiteError]:
    r"""Chunk text into smaller segments

     Splits text into smaller chunks using semantic or fixed-size chunking models.

    ## Models

    ### Fixed Chunking (always available)
    - Simple token-based splitting with overlap
    - Use model=\"fixed\"
    - Fast and deterministic

    ### ONNX Models
    - Semantic chunking based on content similarity
    - Models auto-discovered from `models_dir/chunkers/`
    - Falls back to fixed chunking if model fails

    ## Caching

    Results are cached in memory for 2 minutes. Cache key includes both config and text content.

    Args:
        body (TermiteChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteChunkResponse | TermiteError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteChunkRequest,
) -> TermiteChunkResponse | TermiteError | None:
    r"""Chunk text into smaller segments

     Splits text into smaller chunks using semantic or fixed-size chunking models.

    ## Models

    ### Fixed Chunking (always available)
    - Simple token-based splitting with overlap
    - Use model=\"fixed\"
    - Fast and deterministic

    ### ONNX Models
    - Semantic chunking based on content similarity
    - Models auto-discovered from `models_dir/chunkers/`
    - Falls back to fixed chunking if model fails

    ## Caching

    Results are cached in memory for 2 minutes. Cache key includes both config and text content.

    Args:
        body (TermiteChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteChunkResponse | TermiteError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteChunkRequest,
) -> Response[TermiteChunkResponse | TermiteError]:
    r"""Chunk text into smaller segments

     Splits text into smaller chunks using semantic or fixed-size chunking models.

    ## Models

    ### Fixed Chunking (always available)
    - Simple token-based splitting with overlap
    - Use model=\"fixed\"
    - Fast and deterministic

    ### ONNX Models
    - Semantic chunking based on content similarity
    - Models auto-discovered from `models_dir/chunkers/`
    - Falls back to fixed chunking if model fails

    ## Caching

    Results are cached in memory for 2 minutes. Cache key includes both config and text content.

    Args:
        body (TermiteChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteChunkResponse | TermiteError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteChunkRequest,
) -> TermiteChunkResponse | TermiteError | None:
    r"""Chunk text into smaller segments

     Splits text into smaller chunks using semantic or fixed-size chunking models.

    ## Models

    ### Fixed Chunking (always available)
    - Simple token-based splitting with overlap
    - Use model=\"fixed\"
    - Fast and deterministic

    ### ONNX Models
    - Semantic chunking based on content similarity
    - Models auto-discovered from `models_dir/chunkers/`
    - Falls back to fixed chunking if model fails

    ## Caching

    Results are cached in memory for 2 minutes. Cache key includes both config and text content.

    Args:
        body (TermiteChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteChunkResponse | TermiteError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
