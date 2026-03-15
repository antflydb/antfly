from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.transaction_commit_request import TransactionCommitRequest
from ...models.transaction_commit_response import TransactionCommitResponse
from ...types import Response


def _get_kwargs(
    *,
    body: TransactionCommitRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/transactions/commit",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, TransactionCommitResponse]]:
    if response.status_code == 200:
        response_200 = TransactionCommitResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = TransactionCommitResponse.from_dict(response.json())

        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Error, TransactionCommitResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: TransactionCommitRequest,
) -> Response[Union[Error, TransactionCommitResponse]]:
    r"""Commit an OCC transaction

     Commit a stateless OCC (Optimistic Concurrency Control) transaction.

    **Workflow**:
    1. Read documents using regular lookup endpoints, capturing the
       `X-Antfly-Version` response header for each read
    2. Compute writes locally based on the read values
    3. Submit this commit request with the read set (keys + versions)
       and the write set (batch operations per table)

    The server validates that all read versions still match current state.
    If any version has changed, the transaction is aborted with a 409 Conflict
    response containing details about which key conflicted.

    If all versions match, writes are executed atomically via 2PC.

    **No server-side state**: There is no \"begin transaction\" endpoint.
    The client manages its own read set.

    Args:
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
        Response[Union[Error, TransactionCommitResponse]]
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
    client: AuthenticatedClient,
    body: TransactionCommitRequest,
) -> Optional[Union[Error, TransactionCommitResponse]]:
    r"""Commit an OCC transaction

     Commit a stateless OCC (Optimistic Concurrency Control) transaction.

    **Workflow**:
    1. Read documents using regular lookup endpoints, capturing the
       `X-Antfly-Version` response header for each read
    2. Compute writes locally based on the read values
    3. Submit this commit request with the read set (keys + versions)
       and the write set (batch operations per table)

    The server validates that all read versions still match current state.
    If any version has changed, the transaction is aborted with a 409 Conflict
    response containing details about which key conflicted.

    If all versions match, writes are executed atomically via 2PC.

    **No server-side state**: There is no \"begin transaction\" endpoint.
    The client manages its own read set.

    Args:
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
        Union[Error, TransactionCommitResponse]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: TransactionCommitRequest,
) -> Response[Union[Error, TransactionCommitResponse]]:
    r"""Commit an OCC transaction

     Commit a stateless OCC (Optimistic Concurrency Control) transaction.

    **Workflow**:
    1. Read documents using regular lookup endpoints, capturing the
       `X-Antfly-Version` response header for each read
    2. Compute writes locally based on the read values
    3. Submit this commit request with the read set (keys + versions)
       and the write set (batch operations per table)

    The server validates that all read versions still match current state.
    If any version has changed, the transaction is aborted with a 409 Conflict
    response containing details about which key conflicted.

    If all versions match, writes are executed atomically via 2PC.

    **No server-side state**: There is no \"begin transaction\" endpoint.
    The client manages its own read set.

    Args:
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
        Response[Union[Error, TransactionCommitResponse]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: TransactionCommitRequest,
) -> Optional[Union[Error, TransactionCommitResponse]]:
    r"""Commit an OCC transaction

     Commit a stateless OCC (Optimistic Concurrency Control) transaction.

    **Workflow**:
    1. Read documents using regular lookup endpoints, capturing the
       `X-Antfly-Version` response header for each read
    2. Compute writes locally based on the read values
    3. Submit this commit request with the read set (keys + versions)
       and the write set (batch operations per table)

    The server validates that all read versions still match current state.
    If any version has changed, the transaction is aborted with a 409 Conflict
    response containing details about which key conflicted.

    If all versions match, writes are executed atomically via 2PC.

    **No server-side state**: There is no \"begin transaction\" endpoint.
    The client manages its own read set.

    Args:
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
        Union[Error, TransactionCommitResponse]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
