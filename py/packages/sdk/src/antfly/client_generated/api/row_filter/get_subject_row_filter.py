from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.row_filter_entry import RowFilterEntry
from ...types import Response


def _get_kwargs(
    subject: str,
    table: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/auth/v1/subjects/{subject}/row-filters/{table}".format(
            subject=quote(str(subject), safe=""),
            table=quote(str(table), safe=""),
        ),
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | RowFilterEntry | None:
    if response.status_code == 200:
        response_200 = RowFilterEntry.from_dict(response.json())

        return response_200

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
) -> Response[Error | RowFilterEntry]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    subject: str,
    table: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | RowFilterEntry]:
    """Get row filter for an auth subject on a table

     Returns the row filter policy directly attached to the specified subject and table.

    Args:
        subject (str):  Example: role:tenant_reader.
        table (str):  Example: orders.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowFilterEntry]
    """

    kwargs = _get_kwargs(
        subject=subject,
        table=table,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    subject: str,
    table: str,
    *,
    client: AuthenticatedClient,
) -> Error | RowFilterEntry | None:
    """Get row filter for an auth subject on a table

     Returns the row filter policy directly attached to the specified subject and table.

    Args:
        subject (str):  Example: role:tenant_reader.
        table (str):  Example: orders.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowFilterEntry
    """

    return sync_detailed(
        subject=subject,
        table=table,
        client=client,
    ).parsed


async def asyncio_detailed(
    subject: str,
    table: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | RowFilterEntry]:
    """Get row filter for an auth subject on a table

     Returns the row filter policy directly attached to the specified subject and table.

    Args:
        subject (str):  Example: role:tenant_reader.
        table (str):  Example: orders.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowFilterEntry]
    """

    kwargs = _get_kwargs(
        subject=subject,
        table=table,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    subject: str,
    table: str,
    *,
    client: AuthenticatedClient,
) -> Error | RowFilterEntry | None:
    """Get row filter for an auth subject on a table

     Returns the row filter policy directly attached to the specified subject and table.

    Args:
        subject (str):  Example: role:tenant_reader.
        table (str):  Example: orders.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowFilterEntry
    """

    return (
        await asyncio_detailed(
            subject=subject,
            table=table,
            client=client,
        )
    ).parsed
