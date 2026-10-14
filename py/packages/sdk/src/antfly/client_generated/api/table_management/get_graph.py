from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.graph_catalog_entry import GraphCatalogEntry
from ...types import Response


def _get_kwargs(
    graph_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/graphs/{graph_name}".format(
            graph_name=quote(str(graph_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | GraphCatalogEntry | None:
    if response.status_code == 200:
        response_200 = GraphCatalogEntry.from_dict(response.json())

        return response_200

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | GraphCatalogEntry]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    graph_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | GraphCatalogEntry]:
    """Get graph

    Args:
        graph_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GraphCatalogEntry]
    """

    kwargs = _get_kwargs(
        graph_name=graph_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    graph_name: str,
    *,
    client: AuthenticatedClient,
) -> Error | GraphCatalogEntry | None:
    """Get graph

    Args:
        graph_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GraphCatalogEntry
    """

    return sync_detailed(
        graph_name=graph_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    graph_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | GraphCatalogEntry]:
    """Get graph

    Args:
        graph_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GraphCatalogEntry]
    """

    kwargs = _get_kwargs(
        graph_name=graph_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    graph_name: str,
    *,
    client: AuthenticatedClient,
) -> Error | GraphCatalogEntry | None:
    """Get graph

    Args:
        graph_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GraphCatalogEntry
    """

    return (
        await asyncio_detailed(
            graph_name=graph_name,
            client=client,
        )
    ).parsed
