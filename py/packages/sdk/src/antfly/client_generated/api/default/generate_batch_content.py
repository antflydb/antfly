from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_generate_batch_request import InferenceGenerateBatchRequest
from ...models.inference_generate_batch_response import InferenceGenerateBatchResponse
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceGenerateBatchRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/generate/batch",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceGenerateBatchResponse | None:
    if response.status_code == 200:
        response_200 = InferenceGenerateBatchResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

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
) -> Response[InferenceError | InferenceGenerateBatchResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceGenerateBatchRequest,
) -> Response[InferenceError | InferenceGenerateBatchResponse]:
    """Generate text for a synchronous batch of requests

     Runs multiple non-streaming generation requests as one synchronous batch.
    Compatible native requests for the same model are executed through the
    native batched KV decoder; unsupported per-item options are returned as
    per-item errors without failing sibling requests.

    This endpoint implements the synchronous form only. Future async durable
    batching will use the same request item shape with `mode: async`.

    Args:
        body (InferenceGenerateBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceGenerateBatchResponse]
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
    body: InferenceGenerateBatchRequest,
) -> InferenceError | InferenceGenerateBatchResponse | None:
    """Generate text for a synchronous batch of requests

     Runs multiple non-streaming generation requests as one synchronous batch.
    Compatible native requests for the same model are executed through the
    native batched KV decoder; unsupported per-item options are returned as
    per-item errors without failing sibling requests.

    This endpoint implements the synchronous form only. Future async durable
    batching will use the same request item shape with `mode: async`.

    Args:
        body (InferenceGenerateBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceGenerateBatchResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceGenerateBatchRequest,
) -> Response[InferenceError | InferenceGenerateBatchResponse]:
    """Generate text for a synchronous batch of requests

     Runs multiple non-streaming generation requests as one synchronous batch.
    Compatible native requests for the same model are executed through the
    native batched KV decoder; unsupported per-item options are returned as
    per-item errors without failing sibling requests.

    This endpoint implements the synchronous form only. Future async durable
    batching will use the same request item shape with `mode: async`.

    Args:
        body (InferenceGenerateBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceGenerateBatchResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceGenerateBatchRequest,
) -> InferenceError | InferenceGenerateBatchResponse | None:
    """Generate text for a synchronous batch of requests

     Runs multiple non-streaming generation requests as one synchronous batch.
    Compatible native requests for the same model are executed through the
    native batched KV decoder; unsupported per-item options are returned as
    per-item errors without failing sibling requests.

    This endpoint implements the synchronous form only. Future async durable
    batching will use the same request item shape with `mode: async`.

    Args:
        body (InferenceGenerateBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceGenerateBatchResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
