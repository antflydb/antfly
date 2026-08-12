from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_chunk_request import InferenceChunkRequest
from ...models.inference_chunk_response import InferenceChunkResponse
from ...models.inference_error import InferenceError
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceChunkRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/chunk",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceChunkResponse | InferenceError | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceChunkResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 500:
        response_500 = InferenceError.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = InferenceTransientCapacityError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceChunkResponse | InferenceError | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceChunkRequest,
) -> Response[InferenceChunkResponse | InferenceError | InferenceTransientCapacityError]:
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
        body (InferenceChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceChunkResponse | InferenceError | InferenceTransientCapacityError]
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
    body: InferenceChunkRequest,
) -> InferenceChunkResponse | InferenceError | InferenceTransientCapacityError | None:
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
        body (InferenceChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceChunkResponse | InferenceError | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceChunkRequest,
) -> Response[InferenceChunkResponse | InferenceError | InferenceTransientCapacityError]:
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
        body (InferenceChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceChunkResponse | InferenceError | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceChunkRequest,
) -> InferenceChunkResponse | InferenceError | InferenceTransientCapacityError | None:
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
        body (InferenceChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceChunkResponse | InferenceError | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
