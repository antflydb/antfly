from http import HTTPStatus
from typing import Any

import httpx

from ....native_kv_transport import native_kv_request_async
from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_generate_batch_response import InferenceGenerateBatchResponse
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import File, Response


def _get_kwargs(
    *,
    body: File,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/generate/context/resume/batch",
    }

    _kwargs["content"] = body.payload

    headers["Content-Type"] = "application/vnd.antfly.attachments.v1"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceGenerateBatchResponse | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceGenerateBatchResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 413:
        response_413 = InferenceError.from_dict(response.json())

        return response_413

    if response.status_code == 503:
        response_503 = InferenceTransientCapacityError.from_dict(response.json())

        return response_503

    if response.status_code == 507:
        response_507 = InferenceError.from_dict(response.json())

        return response_507

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferenceGenerateBatchResponse | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: File,
) -> Response[InferenceError | InferenceGenerateBatchResponse | InferenceTransientCapacityError]:
    """Resume one checkpoint and generate serial ordinary siblings

     Accepts one AFATT001 envelope whose JSON metadata is a GenerateBatchRequest
    and whose single application/vnd.antfly.kv-checkpoint attachment belongs
    to the first request. The remaining requests use ordinary exact generation.
    All requests run serially in submission order and release their private
    generation state before the next request starts. The checkpoint remains
    immutable and reusable; ordinary siblings never inherit its compacted KV.

    The batch must contain 2 through 16 requests with distinct nonempty
    custom_id values of at most 128 bytes, the same model and backend
    preference. Every item must explicitly use mode=eager and max_tokens from
    1 through 256. The endpoint disables prompt caching for every item;
    prompt_cache may be false or omitted. Explicit prompt_cache=true,
    streaming, context policies, prompt-cache keys, draft models,
    compiled targets, and context-rewriting options are rejected before any
    item executes. Metadata and each item response are limited to 2 MiB;
    the checkpoint is limited to 2 GiB.

    The first request uses the same identity, prefix, resource and release
    certificate checks as single-checkpoint resume. A failed native resume
    returns its normal error and no sibling executes. Once native resume
    succeeds, ordinary sibling failures are returned as indexed batch errors.
    Parent cancellation and its absolute deadline apply to every item.

    Args:
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceGenerateBatchResponse | InferenceTransientCapacityError]
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
    body: File,
) -> InferenceError | InferenceGenerateBatchResponse | InferenceTransientCapacityError | None:
    """Resume one checkpoint and generate serial ordinary siblings

     Accepts one AFATT001 envelope whose JSON metadata is a GenerateBatchRequest
    and whose single application/vnd.antfly.kv-checkpoint attachment belongs
    to the first request. The remaining requests use ordinary exact generation.
    All requests run serially in submission order and release their private
    generation state before the next request starts. The checkpoint remains
    immutable and reusable; ordinary siblings never inherit its compacted KV.

    The batch must contain 2 through 16 requests with distinct nonempty
    custom_id values of at most 128 bytes, the same model and backend
    preference. Every item must explicitly use mode=eager and max_tokens from
    1 through 256. The endpoint disables prompt caching for every item;
    prompt_cache may be false or omitted. Explicit prompt_cache=true,
    streaming, context policies, prompt-cache keys, draft models,
    compiled targets, and context-rewriting options are rejected before any
    item executes. Metadata and each item response are limited to 2 MiB;
    the checkpoint is limited to 2 GiB.

    The first request uses the same identity, prefix, resource and release
    certificate checks as single-checkpoint resume. A failed native resume
    returns its normal error and no sibling executes. Once native resume
    succeeds, ordinary sibling failures are returned as indexed batch errors.
    Parent cancellation and its absolute deadline apply to every item.

    Args:
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceGenerateBatchResponse | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: File,
) -> Response[InferenceError | InferenceGenerateBatchResponse | InferenceTransientCapacityError]:
    """Resume one checkpoint and generate serial ordinary siblings

     Accepts one AFATT001 envelope whose JSON metadata is a GenerateBatchRequest
    and whose single application/vnd.antfly.kv-checkpoint attachment belongs
    to the first request. The remaining requests use ordinary exact generation.
    All requests run serially in submission order and release their private
    generation state before the next request starts. The checkpoint remains
    immutable and reusable; ordinary siblings never inherit its compacted KV.

    The batch must contain 2 through 16 requests with distinct nonempty
    custom_id values of at most 128 bytes, the same model and backend
    preference. Every item must explicitly use mode=eager and max_tokens from
    1 through 256. The endpoint disables prompt caching for every item;
    prompt_cache may be false or omitted. Explicit prompt_cache=true,
    streaming, context policies, prompt-cache keys, draft models,
    compiled targets, and context-rewriting options are rejected before any
    item executes. Metadata and each item response are limited to 2 MiB;
    the checkpoint is limited to 2 GiB.

    The first request uses the same identity, prefix, resource and release
    certificate checks as single-checkpoint resume. A failed native resume
    returns its normal error and no sibling executes. Once native resume
    succeeds, ordinary sibling failures are returned as indexed batch errors.
    Parent cancellation and its absolute deadline apply to every item.

    Args:
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceGenerateBatchResponse | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await native_kv_request_async(client.get_async_httpx_client(), **kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: File,
) -> InferenceError | InferenceGenerateBatchResponse | InferenceTransientCapacityError | None:
    """Resume one checkpoint and generate serial ordinary siblings

     Accepts one AFATT001 envelope whose JSON metadata is a GenerateBatchRequest
    and whose single application/vnd.antfly.kv-checkpoint attachment belongs
    to the first request. The remaining requests use ordinary exact generation.
    All requests run serially in submission order and release their private
    generation state before the next request starts. The checkpoint remains
    immutable and reusable; ordinary siblings never inherit its compacted KV.

    The batch must contain 2 through 16 requests with distinct nonempty
    custom_id values of at most 128 bytes, the same model and backend
    preference. Every item must explicitly use mode=eager and max_tokens from
    1 through 256. The endpoint disables prompt caching for every item;
    prompt_cache may be false or omitted. Explicit prompt_cache=true,
    streaming, context policies, prompt-cache keys, draft models,
    compiled targets, and context-rewriting options are rejected before any
    item executes. Metadata and each item response are limited to 2 MiB;
    the checkpoint is limited to 2 GiB.

    The first request uses the same identity, prefix, resource and release
    certificate checks as single-checkpoint resume. A failed native resume
    returns its normal error and no sibling executes. Once native resume
    succeeds, ordinary sibling failures are returned as indexed batch errors.
    Parent cancellation and its absolute deadline apply to every item.

    Args:
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceGenerateBatchResponse | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
