from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.transaction_session_status import TransactionSessionStatus
from ...types import Response


def _get_kwargs(
    transaction_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/transactions/{transaction_id}/status".format(
            transaction_id=quote(str(transaction_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | TransactionSessionStatus | None:
    if response.status_code == 200:
        response_200 = TransactionSessionStatus.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | TransactionSessionStatus]:
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
) -> Response[Error | TransactionSessionStatus]:
    """Inspect your transaction receipt

     Returns outcome and lease metadata for a transaction bound to the
    authenticated principal. This endpoint never returns staged documents,
    table names, keys, read snapshots, or execution plans. The creator can
    inspect the outcome after table permissions are revoked or a table is
    dropped. A missing, expired, or other principal's receipt returns 404;
    absence does not establish that a mutation aborted or permit replay.

    Args:
        transaction_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TransactionSessionStatus]
    """

    kwargs = _get_kwargs(
        transaction_id=transaction_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    transaction_id: str,
    *,
    client: AuthenticatedClient,
) -> Error | TransactionSessionStatus | None:
    """Inspect your transaction receipt

     Returns outcome and lease metadata for a transaction bound to the
    authenticated principal. This endpoint never returns staged documents,
    table names, keys, read snapshots, or execution plans. The creator can
    inspect the outcome after table permissions are revoked or a table is
    dropped. A missing, expired, or other principal's receipt returns 404;
    absence does not establish that a mutation aborted or permit replay.

    Args:
        transaction_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TransactionSessionStatus
    """

    return sync_detailed(
        transaction_id=transaction_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    transaction_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | TransactionSessionStatus]:
    """Inspect your transaction receipt

     Returns outcome and lease metadata for a transaction bound to the
    authenticated principal. This endpoint never returns staged documents,
    table names, keys, read snapshots, or execution plans. The creator can
    inspect the outcome after table permissions are revoked or a table is
    dropped. A missing, expired, or other principal's receipt returns 404;
    absence does not establish that a mutation aborted or permit replay.

    Args:
        transaction_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TransactionSessionStatus]
    """

    kwargs = _get_kwargs(
        transaction_id=transaction_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    transaction_id: str,
    *,
    client: AuthenticatedClient,
) -> Error | TransactionSessionStatus | None:
    """Inspect your transaction receipt

     Returns outcome and lease metadata for a transaction bound to the
    authenticated principal. This endpoint never returns staged documents,
    table names, keys, read snapshots, or execution plans. The creator can
    inspect the outcome after table permissions are revoked or a table is
    dropped. A missing, expired, or other principal's receipt returns 404;
    absence does not establish that a mutation aborted or permit replay.

    Args:
        transaction_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TransactionSessionStatus
    """

    return (
        await asyncio_detailed(
            transaction_id=transaction_id,
            client=client,
        )
    ).parsed
