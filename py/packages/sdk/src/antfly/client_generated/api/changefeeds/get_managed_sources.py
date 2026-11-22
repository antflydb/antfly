from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ...client import AuthenticatedClient, Client
from ...models.changefeed_error import ChangefeedError
from ...models.managed_source_status import ManagedSourceStatus
from ...types import Response


def _get_kwargs(
    table_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/sources/managed".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ChangefeedError | ManagedSourceStatus:
    if response.status_code == 200:
        response_200 = ManagedSourceStatus.from_dict(response.json())

        return response_200

    response_default = ChangefeedError.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ChangefeedError | ManagedSourceStatus]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[ChangefeedError | ManagedSourceStatus]:
    """Read managed-source configuration, progress, and destination capabilities

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedError | ManagedSourceStatus]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient | Client,
) -> ChangefeedError | ManagedSourceStatus | None:
    """Read managed-source configuration, progress, and destination capabilities

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedError | ManagedSourceStatus
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[ChangefeedError | ManagedSourceStatus]:
    """Read managed-source configuration, progress, and destination capabilities

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedError | ManagedSourceStatus]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient | Client,
) -> ChangefeedError | ManagedSourceStatus | None:
    """Read managed-source configuration, progress, and destination capabilities

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedError | ManagedSourceStatus
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
        )
    ).parsed
