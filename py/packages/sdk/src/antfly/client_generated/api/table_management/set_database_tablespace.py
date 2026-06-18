from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.catalog_tablespace_binding_request import CatalogTablespaceBindingRequest
from ...models.database_catalog_record import DatabaseCatalogRecord
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    database_name: str,
    *,
    body: CatalogTablespaceBindingRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/db/v1/databases/{database_name}/tablespace".format(
            database_name=quote(str(database_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> DatabaseCatalogRecord | Error | None:
    if response.status_code == 200:
        response_200 = DatabaseCatalogRecord.from_dict(response.json())

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
) -> Response[DatabaseCatalogRecord | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_name: str,
    *,
    client: AuthenticatedClient,
    body: CatalogTablespaceBindingRequest,
) -> Response[DatabaseCatalogRecord | Error]:
    """Set database tablespace

     Binds the database catalog object to an existing tablespace.

    Args:
        database_name (str):
        body (CatalogTablespaceBindingRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DatabaseCatalogRecord | Error]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    *,
    client: AuthenticatedClient,
    body: CatalogTablespaceBindingRequest,
) -> DatabaseCatalogRecord | Error | None:
    """Set database tablespace

     Binds the database catalog object to an existing tablespace.

    Args:
        database_name (str):
        body (CatalogTablespaceBindingRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DatabaseCatalogRecord | Error
    """

    return sync_detailed(
        database_name=database_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    *,
    client: AuthenticatedClient,
    body: CatalogTablespaceBindingRequest,
) -> Response[DatabaseCatalogRecord | Error]:
    """Set database tablespace

     Binds the database catalog object to an existing tablespace.

    Args:
        database_name (str):
        body (CatalogTablespaceBindingRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DatabaseCatalogRecord | Error]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    *,
    client: AuthenticatedClient,
    body: CatalogTablespaceBindingRequest,
) -> DatabaseCatalogRecord | Error | None:
    """Set database tablespace

     Binds the database catalog object to an existing tablespace.

    Args:
        database_name (str):
        body (CatalogTablespaceBindingRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DatabaseCatalogRecord | Error
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            client=client,
            body=body,
        )
    ).parsed
