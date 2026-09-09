from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.namespace_catalog_record import NamespaceCatalogRecord
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | NamespaceCatalogRecord | None:
    if response.status_code == 201:
        response_201 = NamespaceCatalogRecord.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | NamespaceCatalogRecord]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_name: str,
    namespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | Error | NamespaceCatalogRecord]:
    """Create namespace

     Creates a namespace catalog object in a database. The server applies the same semantics as `CREATE
    SCHEMA` for the selected database.

    Args:
        database_name (str):
        namespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | NamespaceCatalogRecord]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Any | Error | NamespaceCatalogRecord | None:
    """Create namespace

     Creates a namespace catalog object in a database. The server applies the same semantics as `CREATE
    SCHEMA` for the selected database.

    Args:
        database_name (str):
        namespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | NamespaceCatalogRecord
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | Error | NamespaceCatalogRecord]:
    """Create namespace

     Creates a namespace catalog object in a database. The server applies the same semantics as `CREATE
    SCHEMA` for the selected database.

    Args:
        database_name (str):
        namespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | NamespaceCatalogRecord]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Any | Error | NamespaceCatalogRecord | None:
    """Create namespace

     Creates a namespace catalog object in a database. The server applies the same semantics as `CREATE
    SCHEMA` for the selected database.

    Args:
        database_name (str):
        namespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | NamespaceCatalogRecord
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            client=client,
        )
    ).parsed
