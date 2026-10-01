from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.row_filter_entry import RowFilterEntry
from ...types import UNSET, Response, Unset


def _get_kwargs(
    user_name: str,
    table: str,
    *,
    database: str | Unset = UNSET,
    namespace: str | Unset = UNSET,
    all_tables: bool | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["database"] = database

    params["namespace"] = namespace

    params["all_tables"] = all_tables

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/auth/v1/users/{user_name}/row-filters/{table}".format(
            user_name=quote(str(user_name), safe=""),
            table=quote(str(table), safe=""),
        ),
        "params": params,
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
    user_name: str,
    table: str,
    *,
    client: AuthenticatedClient,
    database: str | Unset = UNSET,
    namespace: str | Unset = UNSET,
    all_tables: bool | Unset = UNSET,
) -> Response[Error | RowFilterEntry]:
    """Get row filter for a user on a table

     Returns the row filter policy for the specified user and table.

    Args:
        user_name (str):  Example: johndoe.
        table (str):  Example: orders.
        database (str | Unset):
        namespace (str | Unset):
        all_tables (bool | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowFilterEntry]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        table=table,
        database=database,
        namespace=namespace,
        all_tables=all_tables,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    user_name: str,
    table: str,
    *,
    client: AuthenticatedClient,
    database: str | Unset = UNSET,
    namespace: str | Unset = UNSET,
    all_tables: bool | Unset = UNSET,
) -> Error | RowFilterEntry | None:
    """Get row filter for a user on a table

     Returns the row filter policy for the specified user and table.

    Args:
        user_name (str):  Example: johndoe.
        table (str):  Example: orders.
        database (str | Unset):
        namespace (str | Unset):
        all_tables (bool | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowFilterEntry
    """

    return sync_detailed(
        user_name=user_name,
        table=table,
        client=client,
        database=database,
        namespace=namespace,
        all_tables=all_tables,
    ).parsed


async def asyncio_detailed(
    user_name: str,
    table: str,
    *,
    client: AuthenticatedClient,
    database: str | Unset = UNSET,
    namespace: str | Unset = UNSET,
    all_tables: bool | Unset = UNSET,
) -> Response[Error | RowFilterEntry]:
    """Get row filter for a user on a table

     Returns the row filter policy for the specified user and table.

    Args:
        user_name (str):  Example: johndoe.
        table (str):  Example: orders.
        database (str | Unset):
        namespace (str | Unset):
        all_tables (bool | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowFilterEntry]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        table=table,
        database=database,
        namespace=namespace,
        all_tables=all_tables,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    user_name: str,
    table: str,
    *,
    client: AuthenticatedClient,
    database: str | Unset = UNSET,
    namespace: str | Unset = UNSET,
    all_tables: bool | Unset = UNSET,
) -> Error | RowFilterEntry | None:
    """Get row filter for a user on a table

     Returns the row filter policy for the specified user and table.

    Args:
        user_name (str):  Example: johndoe.
        table (str):  Example: orders.
        database (str | Unset):
        namespace (str | Unset):
        all_tables (bool | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowFilterEntry
    """

    return (
        await asyncio_detailed(
            user_name=user_name,
            table=table,
            client=client,
            database=database,
            namespace=namespace,
            all_tables=all_tables,
        )
    ).parsed
