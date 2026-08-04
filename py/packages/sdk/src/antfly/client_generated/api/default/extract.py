from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.extraction_request import ExtractionRequest
from ...models.extraction_response import ExtractionResponse
from ...models.inference_error import InferenceError
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import Response


def _get_kwargs(
    *,
    body: ExtractionRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/extract",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ExtractionResponse | InferenceError | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = ExtractionResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = InferenceError.from_dict(response.json())

        return response_404

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
) -> Response[ExtractionResponse | InferenceError | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: ExtractionRequest,
) -> Response[ExtractionResponse | InferenceError | InferenceTransientCapacityError]:
    """Extract entities, relations, classifications, and structures

     Schema-driven extraction over shared AI content parts. This is the
    canonical public API for named entity recognition, relation extraction,
    text/document classification, token classification, and structured
    document extraction.

    Args:
        body (ExtractionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ExtractionResponse | InferenceError | InferenceTransientCapacityError]
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
    body: ExtractionRequest,
) -> ExtractionResponse | InferenceError | InferenceTransientCapacityError | None:
    """Extract entities, relations, classifications, and structures

     Schema-driven extraction over shared AI content parts. This is the
    canonical public API for named entity recognition, relation extraction,
    text/document classification, token classification, and structured
    document extraction.

    Args:
        body (ExtractionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ExtractionResponse | InferenceError | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: ExtractionRequest,
) -> Response[ExtractionResponse | InferenceError | InferenceTransientCapacityError]:
    """Extract entities, relations, classifications, and structures

     Schema-driven extraction over shared AI content parts. This is the
    canonical public API for named entity recognition, relation extraction,
    text/document classification, token classification, and structured
    document extraction.

    Args:
        body (ExtractionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ExtractionResponse | InferenceError | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: ExtractionRequest,
) -> ExtractionResponse | InferenceError | InferenceTransientCapacityError | None:
    """Extract entities, relations, classifications, and structures

     Schema-driven extraction over shared AI content parts. This is the
    canonical public API for named entity recognition, relation extraction,
    text/document classification, token classification, and structured
    document extraction.

    Args:
        body (ExtractionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ExtractionResponse | InferenceError | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
