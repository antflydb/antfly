from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.row_filter_entry import RowFilterEntry
from ...types import Response


def _get_kwargs(
    user_name: str,
    table: str,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/users/{user_name}/row-filters/{table}",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, RowFilterEntry]]:
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
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Error, RowFilterEntry]]:
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
) -> Response[Union[Error, RowFilterEntry]]:
    """Get row filter for a user on a table

     Returns the row filter policy for the specified user and table.

    Args:
        user_name (str):  Example: johndoe.
        table (str):  Example: orders.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, RowFilterEntry]]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        table=table,
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
) -> Optional[Union[Error, RowFilterEntry]]:
    """Get row filter for a user on a table

     Returns the row filter policy for the specified user and table.

    Args:
        user_name (str):  Example: johndoe.
        table (str):  Example: orders.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, RowFilterEntry]
    """

    return sync_detailed(
        user_name=user_name,
        table=table,
        client=client,
    ).parsed


async def asyncio_detailed(
    user_name: str,
    table: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[Error, RowFilterEntry]]:
    """Get row filter for a user on a table

     Returns the row filter policy for the specified user and table.

    Args:
        user_name (str):  Example: johndoe.
        table (str):  Example: orders.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, RowFilterEntry]]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        table=table,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    user_name: str,
    table: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[Error, RowFilterEntry]]:
    """Get row filter for a user on a table

     Returns the row filter policy for the specified user and table.

    Args:
        user_name (str):  Example: johndoe.
        table (str):  Example: orders.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, RowFilterEntry]
    """

    return (
        await asyncio_detailed(
            user_name=user_name,
            table=table,
            client=client,
        )
    ).parsed
