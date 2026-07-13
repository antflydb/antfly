from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.backup_list_response import BackupListResponse
from ...models.error import Error
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    location: str,
    connection: str,
    limit: int | Unset = 100,
    cursor: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["location"] = location

    params["connection"] = connection

    params["limit"] = limit

    params["cursor"] = cursor

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/backups",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> BackupListResponse | Error | None:
    if response.status_code == 200:
        response_200 = BackupListResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[BackupListResponse | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    location: str,
    connection: str,
    limit: int | Unset = 100,
    cursor: str | Unset = UNSET,
) -> Response[BackupListResponse | Error]:
    """List available backups

     Lists one bounded page of cluster-level backups in stable manifest-key
    order at the specified location. Returns metadata about each backup
    including the tables included, timestamp, and Antfly version. Pass the
    returned `next_cursor` unchanged to retrieve the next page.

    Args:
        location (str):
        connection (str):
        limit (int | Unset):  Default: 100.
        cursor (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[BackupListResponse | Error]
    """

    kwargs = _get_kwargs(
        location=location,
        connection=connection,
        limit=limit,
        cursor=cursor,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    location: str,
    connection: str,
    limit: int | Unset = 100,
    cursor: str | Unset = UNSET,
) -> BackupListResponse | Error | None:
    """List available backups

     Lists one bounded page of cluster-level backups in stable manifest-key
    order at the specified location. Returns metadata about each backup
    including the tables included, timestamp, and Antfly version. Pass the
    returned `next_cursor` unchanged to retrieve the next page.

    Args:
        location (str):
        connection (str):
        limit (int | Unset):  Default: 100.
        cursor (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        BackupListResponse | Error
    """

    return sync_detailed(
        client=client,
        location=location,
        connection=connection,
        limit=limit,
        cursor=cursor,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    location: str,
    connection: str,
    limit: int | Unset = 100,
    cursor: str | Unset = UNSET,
) -> Response[BackupListResponse | Error]:
    """List available backups

     Lists one bounded page of cluster-level backups in stable manifest-key
    order at the specified location. Returns metadata about each backup
    including the tables included, timestamp, and Antfly version. Pass the
    returned `next_cursor` unchanged to retrieve the next page.

    Args:
        location (str):
        connection (str):
        limit (int | Unset):  Default: 100.
        cursor (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[BackupListResponse | Error]
    """

    kwargs = _get_kwargs(
        location=location,
        connection=connection,
        limit=limit,
        cursor=cursor,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    location: str,
    connection: str,
    limit: int | Unset = 100,
    cursor: str | Unset = UNSET,
) -> BackupListResponse | Error | None:
    """List available backups

     Lists one bounded page of cluster-level backups in stable manifest-key
    order at the specified location. Returns metadata about each backup
    including the tables included, timestamp, and Antfly version. Pass the
    returned `next_cursor` unchanged to retrieve the next page.

    Args:
        location (str):
        connection (str):
        limit (int | Unset):  Default: 100.
        cursor (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        BackupListResponse | Error
    """

    return (
        await asyncio_detailed(
            client=client,
            location=location,
            connection=connection,
            limit=limit,
            cursor=cursor,
        )
    ).parsed
