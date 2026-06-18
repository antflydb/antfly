from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.tablespace_catalog_record import TablespaceCatalogRecord
from ...types import Response


def _get_kwargs(
    tablespace_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tablespaces/{tablespace_name}".format(
            tablespace_name=quote(str(tablespace_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | TablespaceCatalogRecord | None:
    if response.status_code == 200:
        response_200 = TablespaceCatalogRecord.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | TablespaceCatalogRecord]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | TablespaceCatalogRecord]:
    """Get tablespace

    Args:
        tablespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TablespaceCatalogRecord]
    """

    kwargs = _get_kwargs(
        tablespace_name=tablespace_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Error | TablespaceCatalogRecord | None:
    """Get tablespace

    Args:
        tablespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TablespaceCatalogRecord
    """

    return sync_detailed(
        tablespace_name=tablespace_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | TablespaceCatalogRecord]:
    """Get tablespace

    Args:
        tablespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TablespaceCatalogRecord]
    """

    kwargs = _get_kwargs(
        tablespace_name=tablespace_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Error | TablespaceCatalogRecord | None:
    """Get tablespace

    Args:
        tablespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TablespaceCatalogRecord
    """

    return (
        await asyncio_detailed(
            tablespace_name=tablespace_name,
            client=client,
        )
    ).parsed
