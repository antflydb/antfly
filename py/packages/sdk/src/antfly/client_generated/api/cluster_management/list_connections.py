from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.connections_response import ConnectionsResponse
from ...models.error import Error
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    types: str | Unset = UNSET,
    include: str | Unset = UNSET,
    refresh: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["types"] = types

    params["include"] = include

    params["refresh"] = refresh

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/connections",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ConnectionsResponse | Error | None:
    if response.status_code == 200:
        response_200 = ConnectionsResponse.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = Error.from_dict(response.json())

        return response_401

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ConnectionsResponse | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    types: str | Unset = UNSET,
    include: str | Unset = UNSET,
    refresh: str | Unset = UNSET,
) -> Response[ConnectionsResponse | Error]:
    r"""List configured external connections

     Enumerates public external connections configured on this node under
    top-level `connections`: inference providers, web search providers,
    external IO endpoints, and CDC replication sources.

    The default response is config-derived and avoids slow provider calls.
    With include=models, each inference provider is queried live for its
    available models where the provider exposes a listing API. Connections
    that fail to respond are reported with status \"error\" instead of
    failing the whole response. A status of \"configured\" means the
    connection exists but was not live-probed in this response.

    Args:
        types (str | Unset):
        include (str | Unset):
        refresh (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ConnectionsResponse | Error]
    """

    kwargs = _get_kwargs(
        types=types,
        include=include,
        refresh=refresh,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    types: str | Unset = UNSET,
    include: str | Unset = UNSET,
    refresh: str | Unset = UNSET,
) -> ConnectionsResponse | Error | None:
    r"""List configured external connections

     Enumerates public external connections configured on this node under
    top-level `connections`: inference providers, web search providers,
    external IO endpoints, and CDC replication sources.

    The default response is config-derived and avoids slow provider calls.
    With include=models, each inference provider is queried live for its
    available models where the provider exposes a listing API. Connections
    that fail to respond are reported with status \"error\" instead of
    failing the whole response. A status of \"configured\" means the
    connection exists but was not live-probed in this response.

    Args:
        types (str | Unset):
        include (str | Unset):
        refresh (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ConnectionsResponse | Error
    """

    return sync_detailed(
        client=client,
        types=types,
        include=include,
        refresh=refresh,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    types: str | Unset = UNSET,
    include: str | Unset = UNSET,
    refresh: str | Unset = UNSET,
) -> Response[ConnectionsResponse | Error]:
    r"""List configured external connections

     Enumerates public external connections configured on this node under
    top-level `connections`: inference providers, web search providers,
    external IO endpoints, and CDC replication sources.

    The default response is config-derived and avoids slow provider calls.
    With include=models, each inference provider is queried live for its
    available models where the provider exposes a listing API. Connections
    that fail to respond are reported with status \"error\" instead of
    failing the whole response. A status of \"configured\" means the
    connection exists but was not live-probed in this response.

    Args:
        types (str | Unset):
        include (str | Unset):
        refresh (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ConnectionsResponse | Error]
    """

    kwargs = _get_kwargs(
        types=types,
        include=include,
        refresh=refresh,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    types: str | Unset = UNSET,
    include: str | Unset = UNSET,
    refresh: str | Unset = UNSET,
) -> ConnectionsResponse | Error | None:
    r"""List configured external connections

     Enumerates public external connections configured on this node under
    top-level `connections`: inference providers, web search providers,
    external IO endpoints, and CDC replication sources.

    The default response is config-derived and avoids slow provider calls.
    With include=models, each inference provider is queried live for its
    available models where the provider exposes a listing API. Connections
    that fail to respond are reported with status \"error\" instead of
    failing the whole response. A status of \"configured\" means the
    connection exists but was not live-probed in this response.

    Args:
        types (str | Unset):
        include (str | Unset):
        refresh (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ConnectionsResponse | Error
    """

    return (
        await asyncio_detailed(
            client=client,
            types=types,
            include=include,
            refresh=refresh,
        )
    ).parsed
