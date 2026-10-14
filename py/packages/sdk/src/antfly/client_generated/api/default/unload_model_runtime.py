from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_model_runtime_unload_request import InferenceModelRuntimeUnloadRequest
from ...models.inference_model_runtime_unload_response import InferenceModelRuntimeUnloadResponse
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceModelRuntimeUnloadRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/models/unload",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceModelRuntimeUnloadResponse | None:
    if response.status_code == 200:
        response_200 = InferenceModelRuntimeUnloadResponse.from_dict(response.json())

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

    if response.status_code == 503:
        response_503 = InferenceError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferenceModelRuntimeUnloadResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceModelRuntimeUnloadRequest,
) -> Response[InferenceError | InferenceModelRuntimeUnloadResponse]:
    """Unload one exact local runtime incarnation

     Evicts the exact runtime identified by runtime_id only when it is idle.
    Missing or stale identifiers are idempotent and never evict a replacement
    incarnation. Loading, active, and startup-managed runtimes return 409.

    Args:
        body (InferenceModelRuntimeUnloadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceModelRuntimeUnloadResponse]
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
    body: InferenceModelRuntimeUnloadRequest,
) -> InferenceError | InferenceModelRuntimeUnloadResponse | None:
    """Unload one exact local runtime incarnation

     Evicts the exact runtime identified by runtime_id only when it is idle.
    Missing or stale identifiers are idempotent and never evict a replacement
    incarnation. Loading, active, and startup-managed runtimes return 409.

    Args:
        body (InferenceModelRuntimeUnloadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceModelRuntimeUnloadResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceModelRuntimeUnloadRequest,
) -> Response[InferenceError | InferenceModelRuntimeUnloadResponse]:
    """Unload one exact local runtime incarnation

     Evicts the exact runtime identified by runtime_id only when it is idle.
    Missing or stale identifiers are idempotent and never evict a replacement
    incarnation. Loading, active, and startup-managed runtimes return 409.

    Args:
        body (InferenceModelRuntimeUnloadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceModelRuntimeUnloadResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceModelRuntimeUnloadRequest,
) -> InferenceError | InferenceModelRuntimeUnloadResponse | None:
    """Unload one exact local runtime incarnation

     Evicts the exact runtime identified by runtime_id only when it is idle.
    Missing or stale identifiers are idempotent and never evict a replacement
    incarnation. Loading, active, and startup-managed runtimes return 409.

    Args:
        body (InferenceModelRuntimeUnloadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceModelRuntimeUnloadResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
