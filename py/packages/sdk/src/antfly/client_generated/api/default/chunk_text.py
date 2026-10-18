from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_chunk_embedding_response import InferenceChunkEmbeddingResponse
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
) -> InferenceChunkEmbeddingResponse | InferenceChunkResponse | InferenceError | InferenceTransientCapacityError | None:
    if response.status_code == 200:

        def _parse_response_200(data: object) -> InferenceChunkEmbeddingResponse | InferenceChunkResponse:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_inference_chunk_operation_response_type_0 = InferenceChunkResponse.from_dict(data)

                return componentsschemas_inference_chunk_operation_response_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_inference_chunk_operation_response_type_1 = InferenceChunkEmbeddingResponse.from_dict(
                data
            )

            return componentsschemas_inference_chunk_operation_response_type_1

        response_200 = _parse_response_200(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 409:
        response_409 = InferenceError.from_dict(response.json())

        return response_409

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
) -> Response[
    InferenceChunkEmbeddingResponse | InferenceChunkResponse | InferenceError | InferenceTransientCapacityError
]:
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
) -> Response[
    InferenceChunkEmbeddingResponse | InferenceChunkResponse | InferenceError | InferenceTransientCapacityError
]:
    r"""Chunk content with optional model-aware embeddings

     Splits content into chunks. Without `embedding`, the fixed chunker and
    legacy response remain unchanged. With `embedding`, the resolved target
    model constrains boundaries for text, PDF pages, images or explicit tiles,
    composited GIF frames, audio windows and consecutive video windows.

    Each model-aware result pairs source coordinates and an optional vector
    or error. `include_content` independently controls payload inclusion;
    video uses the original source and sampling recipe. `execute: false`
    plans without executing the embedding model. `stream: true` delivers
    bounded NDJSON records followed by terminal coverage. A cap, gap or lost
    terminal record must not be treated as complete source coverage.
    Continuations bind the original source version and semantic plan;
    retries may deterministically recompute and must deduplicate chunk IDs.

    ## Models

    ### Fixed Chunking
    - Token-count splitting with overlap for text; fixed time windows for audio
    - Use model=\"fixed\" (aliases `fixed_bert`, `fixed_bpe`, and `fixed-bert-tokenizer` are accepted)
    - Fast and deterministic
    - Other model names are rejected as unsupported

    Args:
        body (InferenceChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceChunkEmbeddingResponse | InferenceChunkResponse | InferenceError | InferenceTransientCapacityError]
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
) -> InferenceChunkEmbeddingResponse | InferenceChunkResponse | InferenceError | InferenceTransientCapacityError | None:
    r"""Chunk content with optional model-aware embeddings

     Splits content into chunks. Without `embedding`, the fixed chunker and
    legacy response remain unchanged. With `embedding`, the resolved target
    model constrains boundaries for text, PDF pages, images or explicit tiles,
    composited GIF frames, audio windows and consecutive video windows.

    Each model-aware result pairs source coordinates and an optional vector
    or error. `include_content` independently controls payload inclusion;
    video uses the original source and sampling recipe. `execute: false`
    plans without executing the embedding model. `stream: true` delivers
    bounded NDJSON records followed by terminal coverage. A cap, gap or lost
    terminal record must not be treated as complete source coverage.
    Continuations bind the original source version and semantic plan;
    retries may deterministically recompute and must deduplicate chunk IDs.

    ## Models

    ### Fixed Chunking
    - Token-count splitting with overlap for text; fixed time windows for audio
    - Use model=\"fixed\" (aliases `fixed_bert`, `fixed_bpe`, and `fixed-bert-tokenizer` are accepted)
    - Fast and deterministic
    - Other model names are rejected as unsupported

    Args:
        body (InferenceChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceChunkEmbeddingResponse | InferenceChunkResponse | InferenceError | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceChunkRequest,
) -> Response[
    InferenceChunkEmbeddingResponse | InferenceChunkResponse | InferenceError | InferenceTransientCapacityError
]:
    r"""Chunk content with optional model-aware embeddings

     Splits content into chunks. Without `embedding`, the fixed chunker and
    legacy response remain unchanged. With `embedding`, the resolved target
    model constrains boundaries for text, PDF pages, images or explicit tiles,
    composited GIF frames, audio windows and consecutive video windows.

    Each model-aware result pairs source coordinates and an optional vector
    or error. `include_content` independently controls payload inclusion;
    video uses the original source and sampling recipe. `execute: false`
    plans without executing the embedding model. `stream: true` delivers
    bounded NDJSON records followed by terminal coverage. A cap, gap or lost
    terminal record must not be treated as complete source coverage.
    Continuations bind the original source version and semantic plan;
    retries may deterministically recompute and must deduplicate chunk IDs.

    ## Models

    ### Fixed Chunking
    - Token-count splitting with overlap for text; fixed time windows for audio
    - Use model=\"fixed\" (aliases `fixed_bert`, `fixed_bpe`, and `fixed-bert-tokenizer` are accepted)
    - Fast and deterministic
    - Other model names are rejected as unsupported

    Args:
        body (InferenceChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceChunkEmbeddingResponse | InferenceChunkResponse | InferenceError | InferenceTransientCapacityError]
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
) -> InferenceChunkEmbeddingResponse | InferenceChunkResponse | InferenceError | InferenceTransientCapacityError | None:
    r"""Chunk content with optional model-aware embeddings

     Splits content into chunks. Without `embedding`, the fixed chunker and
    legacy response remain unchanged. With `embedding`, the resolved target
    model constrains boundaries for text, PDF pages, images or explicit tiles,
    composited GIF frames, audio windows and consecutive video windows.

    Each model-aware result pairs source coordinates and an optional vector
    or error. `include_content` independently controls payload inclusion;
    video uses the original source and sampling recipe. `execute: false`
    plans without executing the embedding model. `stream: true` delivers
    bounded NDJSON records followed by terminal coverage. A cap, gap or lost
    terminal record must not be treated as complete source coverage.
    Continuations bind the original source version and semantic plan;
    retries may deterministically recompute and must deduplicate chunk IDs.

    ## Models

    ### Fixed Chunking
    - Token-count splitting with overlap for text; fixed time windows for audio
    - Use model=\"fixed\" (aliases `fixed_bert`, `fixed_bpe`, and `fixed-bert-tokenizer` are accepted)
    - Fast and deterministic
    - Other model names are rejected as unsupported

    Args:
        body (InferenceChunkRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceChunkEmbeddingResponse | InferenceChunkResponse | InferenceError | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
