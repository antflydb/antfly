from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.transaction_commit_request import TransactionCommitRequest
from ...models.transaction_status_response import TransactionStatusResponse
from ...types import Response


def _get_kwargs(
    transaction_id: str,
    *,
    body: TransactionCommitRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/transactions/{transaction_id}/stage".format(
            transaction_id=quote(str(transaction_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | TransactionStatusResponse | None:
    if response.status_code == 200:
        response_200 = TransactionStatusResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | TransactionStatusResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    transaction_id: str,
    *,
    client: AuthenticatedClient,
    body: TransactionCommitRequest,
) -> Response[Error | TransactionStatusResponse]:
    """Stage a transaction commit request

    Args:
        transaction_id (str):
        body (TransactionCommitRequest): Stateless OCC (Optimistic Concurrency Control)
            transaction commit request.

            The client reads documents (capturing version tokens from the X-Antfly-Version
            response header on lookups), computes writes locally, then submits everything
            in this single commit request. The server validates that all read versions
            still match before executing writes atomically via 2PC.

            **No server-side state**: There is no "begin" endpoint. The client manages
            its own read set and submits the full transaction in one request.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TransactionStatusResponse]
    """

    kwargs = _get_kwargs(
        transaction_id=transaction_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    transaction_id: str,
    *,
    client: AuthenticatedClient,
    body: TransactionCommitRequest,
) -> Error | TransactionStatusResponse | None:
    """Stage a transaction commit request

    Args:
        transaction_id (str):
        body (TransactionCommitRequest): Stateless OCC (Optimistic Concurrency Control)
            transaction commit request.

            The client reads documents (capturing version tokens from the X-Antfly-Version
            response header on lookups), computes writes locally, then submits everything
            in this single commit request. The server validates that all read versions
            still match before executing writes atomically via 2PC.

            **No server-side state**: There is no "begin" endpoint. The client manages
            its own read set and submits the full transaction in one request.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TransactionStatusResponse
    """

    return sync_detailed(
        transaction_id=transaction_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    transaction_id: str,
    *,
    client: AuthenticatedClient,
    body: TransactionCommitRequest,
) -> Response[Error | TransactionStatusResponse]:
    """Stage a transaction commit request

    Args:
        transaction_id (str):
        body (TransactionCommitRequest): Stateless OCC (Optimistic Concurrency Control)
            transaction commit request.

            The client reads documents (capturing version tokens from the X-Antfly-Version
            response header on lookups), computes writes locally, then submits everything
            in this single commit request. The server validates that all read versions
            still match before executing writes atomically via 2PC.

            **No server-side state**: There is no "begin" endpoint. The client manages
            its own read set and submits the full transaction in one request.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TransactionStatusResponse]
    """

    kwargs = _get_kwargs(
        transaction_id=transaction_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    transaction_id: str,
    *,
    client: AuthenticatedClient,
    body: TransactionCommitRequest,
) -> Error | TransactionStatusResponse | None:
    """Stage a transaction commit request

    Args:
        transaction_id (str):
        body (TransactionCommitRequest): Stateless OCC (Optimistic Concurrency Control)
            transaction commit request.

            The client reads documents (capturing version tokens from the X-Antfly-Version
            response header on lookups), computes writes locally, then submits everything
            in this single commit request. The server validates that all read versions
            still match before executing writes atomically via 2PC.

            **No server-side state**: There is no "begin" endpoint. The client manages
            its own read set and submits the full transaction in one request.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TransactionStatusResponse
    """

    return (
        await asyncio_detailed(
            transaction_id=transaction_id,
            client=client,
            body=body,
        )
    ).parsed
