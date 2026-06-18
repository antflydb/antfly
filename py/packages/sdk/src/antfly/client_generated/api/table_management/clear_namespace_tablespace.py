from http import HTTPStatus
from typing import Any
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
        "method": "delete",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tablespace".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | NamespaceCatalogRecord | None:
    if response.status_code == 200:
        response_200 = NamespaceCatalogRecord.from_dict(response.json())

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
) -> Response[Error | NamespaceCatalogRecord]:
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
) -> Response[Error | NamespaceCatalogRecord]:
    """Clear namespace tablespace

     Clears the namespace catalog object's durable tablespace binding.

    Args:
        database_name (str):
        namespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | NamespaceCatalogRecord]
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
) -> Error | NamespaceCatalogRecord | None:
    """Clear namespace tablespace

     Clears the namespace catalog object's durable tablespace binding.

    Args:
        database_name (str):
        namespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | NamespaceCatalogRecord
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
) -> Response[Error | NamespaceCatalogRecord]:
    """Clear namespace tablespace

     Clears the namespace catalog object's durable tablespace binding.

    Args:
        database_name (str):
        namespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | NamespaceCatalogRecord]
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
) -> Error | NamespaceCatalogRecord | None:
    """Clear namespace tablespace

     Clears the namespace catalog object's durable tablespace binding.

    Args:
        database_name (str):
        namespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | NamespaceCatalogRecord
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            client=client,
        )
    ).parsed
