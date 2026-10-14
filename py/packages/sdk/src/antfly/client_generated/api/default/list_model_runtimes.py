from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_model_runtime_response import InferenceModelRuntimeResponse
from ...types import Response


def _get_kwargs() -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/ai/v1/models/runtime",
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceModelRuntimeResponse | None:
    if response.status_code == 200:
        response_200 = InferenceModelRuntimeResponse.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 500:
        response_500 = InferenceError.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = InferenceError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferenceModelRuntimeResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[InferenceError | InferenceModelRuntimeResponse]:
    """Inspect locally cached model runtimes

     Returns a side-effect-free snapshot of model runtimes that are loading or
    cached by this inference process. Reading this endpoint does not keep a
    runtime alive or renew its idle eviction timer. A loaded runtime is a
    published cache entry; its weights may still use a streamed, paged, or
    backend-managed storage policy.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceModelRuntimeResponse]
    """

    kwargs = _get_kwargs()

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
) -> InferenceError | InferenceModelRuntimeResponse | None:
    """Inspect locally cached model runtimes

     Returns a side-effect-free snapshot of model runtimes that are loading or
    cached by this inference process. Reading this endpoint does not keep a
    runtime alive or renew its idle eviction timer. A loaded runtime is a
    published cache entry; its weights may still use a streamed, paged, or
    backend-managed storage policy.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceModelRuntimeResponse
    """

    return sync_detailed(
        client=client,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[InferenceError | InferenceModelRuntimeResponse]:
    """Inspect locally cached model runtimes

     Returns a side-effect-free snapshot of model runtimes that are loading or
    cached by this inference process. Reading this endpoint does not keep a
    runtime alive or renew its idle eviction timer. A loaded runtime is a
    published cache entry; its weights may still use a streamed, paged, or
    backend-managed storage policy.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceModelRuntimeResponse]
    """

    kwargs = _get_kwargs()

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
) -> InferenceError | InferenceModelRuntimeResponse | None:
    """Inspect locally cached model runtimes

     Returns a side-effect-free snapshot of model runtimes that are loading or
    cached by this inference process. Reading this endpoint does not keep a
    runtime alive or renew its idle eviction timer. A loaded runtime is a
    published cache entry; its weights may still use a streamed, paged, or
    backend-managed storage policy.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceModelRuntimeResponse
    """

    return (
        await asyncio_detailed(
            client=client,
        )
    ).parsed
