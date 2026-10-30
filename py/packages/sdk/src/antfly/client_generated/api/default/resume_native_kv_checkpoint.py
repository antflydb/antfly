from http import HTTPStatus
from typing import Any

import httpx

from ....native_kv_transport import native_kv_request_async
from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_generate_response import InferenceGenerateResponse
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import File, Response


def _get_kwargs(
    *,
    body: File,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/generate/context/resume",
    }

    _kwargs["content"] = body.payload

    headers["Content-Type"] = "application/vnd.antfly.attachments.v1"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceGenerateResponse | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceGenerateResponse.from_dict(response.json())

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
) -> Response[InferenceError | InferenceGenerateResponse | InferenceTransientCapacityError]:
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
) -> Response[InferenceError | InferenceGenerateResponse | InferenceTransientCapacityError]:
    """Generate from a portable native KV checkpoint

     Accepts one AFATT001 envelope whose JSON metadata is a GenerateRequest
    and whose single application/vnd.antfly.kv-checkpoint attachment is a
    complete native checkpoint. The metadata supplies the complete messages,
    tool definitions and template options. Their rendered tokens must have
    the checkpoint tokens as an exact prefix; prefix rewriting is rejected.
    Only the suffix is evaluated. An empty suffix uses saved next-token
    logits, without replaying the final prefix token. At most 8192 prompt
    tokens may be appended, and the full prompt plus reserved output must
    fit the 32768-token native checkpoint window.

    The loaded model supplies the trusted identity and geometry. Checksum,
    framing, token-prefix, layout and resource checks run before generation.
    Unsupported routes and malformed files do not replace a live cache.
    The response uses normal generation JSON or SSE according to metadata.stream.
    The original checkpoint remains immutable and reusable.

    Args:
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceGenerateResponse | InferenceTransientCapacityError]
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
) -> InferenceError | InferenceGenerateResponse | InferenceTransientCapacityError | None:
    """Generate from a portable native KV checkpoint

     Accepts one AFATT001 envelope whose JSON metadata is a GenerateRequest
    and whose single application/vnd.antfly.kv-checkpoint attachment is a
    complete native checkpoint. The metadata supplies the complete messages,
    tool definitions and template options. Their rendered tokens must have
    the checkpoint tokens as an exact prefix; prefix rewriting is rejected.
    Only the suffix is evaluated. An empty suffix uses saved next-token
    logits, without replaying the final prefix token. At most 8192 prompt
    tokens may be appended, and the full prompt plus reserved output must
    fit the 32768-token native checkpoint window.

    The loaded model supplies the trusted identity and geometry. Checksum,
    framing, token-prefix, layout and resource checks run before generation.
    Unsupported routes and malformed files do not replace a live cache.
    The response uses normal generation JSON or SSE according to metadata.stream.
    The original checkpoint remains immutable and reusable.

    Args:
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceGenerateResponse | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: File,
) -> Response[InferenceError | InferenceGenerateResponse | InferenceTransientCapacityError]:
    """Generate from a portable native KV checkpoint

     Accepts one AFATT001 envelope whose JSON metadata is a GenerateRequest
    and whose single application/vnd.antfly.kv-checkpoint attachment is a
    complete native checkpoint. The metadata supplies the complete messages,
    tool definitions and template options. Their rendered tokens must have
    the checkpoint tokens as an exact prefix; prefix rewriting is rejected.
    Only the suffix is evaluated. An empty suffix uses saved next-token
    logits, without replaying the final prefix token. At most 8192 prompt
    tokens may be appended, and the full prompt plus reserved output must
    fit the 32768-token native checkpoint window.

    The loaded model supplies the trusted identity and geometry. Checksum,
    framing, token-prefix, layout and resource checks run before generation.
    Unsupported routes and malformed files do not replace a live cache.
    The response uses normal generation JSON or SSE according to metadata.stream.
    The original checkpoint remains immutable and reusable.

    Args:
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceGenerateResponse | InferenceTransientCapacityError]
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
) -> InferenceError | InferenceGenerateResponse | InferenceTransientCapacityError | None:
    """Generate from a portable native KV checkpoint

     Accepts one AFATT001 envelope whose JSON metadata is a GenerateRequest
    and whose single application/vnd.antfly.kv-checkpoint attachment is a
    complete native checkpoint. The metadata supplies the complete messages,
    tool definitions and template options. Their rendered tokens must have
    the checkpoint tokens as an exact prefix; prefix rewriting is rejected.
    Only the suffix is evaluated. An empty suffix uses saved next-token
    logits, without replaying the final prefix token. At most 8192 prompt
    tokens may be appended, and the full prompt plus reserved output must
    fit the 32768-token native checkpoint window.

    The loaded model supplies the trusted identity and geometry. Checksum,
    framing, token-prefix, layout and resource checks run before generation.
    Unsupported routes and malformed files do not replace a live cache.
    The response uses normal generation JSON or SSE according to metadata.stream.
    The original checkpoint remains immutable and reusable.

    Args:
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceGenerateResponse | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
