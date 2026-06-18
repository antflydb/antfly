from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_tablespace_request import CreateTablespaceRequest
from ...models.error import Error
from ...models.tablespace_catalog_record import TablespaceCatalogRecord
from ...types import UNSET, Response, Unset


def _get_kwargs(
    tablespace_name: str,
    *,
    body: CreateTablespaceRequest | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tablespaces/{tablespace_name}".format(
            tablespace_name=quote(str(tablespace_name), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | TablespaceCatalogRecord | None:
    if response.status_code == 201:
        response_201 = TablespaceCatalogRecord.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

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
) -> Response[Any | Error | TablespaceCatalogRecord]:
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
    body: CreateTablespaceRequest | Unset = UNSET,
) -> Response[Any | Error | TablespaceCatalogRecord]:
    """Create tablespace

     Creates a tablespace catalog object. The server applies the same lifecycle semantics as `CREATE
    TABLESPACE`.

    Args:
        tablespace_name (str):
        body (CreateTablespaceRequest | Unset): Tablespace creation request. Placement policy is
            fail-closed until native placement planning consumes it.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | TablespaceCatalogRecord]
    """

    kwargs = _get_kwargs(
        tablespace_name=tablespace_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateTablespaceRequest | Unset = UNSET,
) -> Any | Error | TablespaceCatalogRecord | None:
    """Create tablespace

     Creates a tablespace catalog object. The server applies the same lifecycle semantics as `CREATE
    TABLESPACE`.

    Args:
        tablespace_name (str):
        body (CreateTablespaceRequest | Unset): Tablespace creation request. Placement policy is
            fail-closed until native placement planning consumes it.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | TablespaceCatalogRecord
    """

    return sync_detailed(
        tablespace_name=tablespace_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateTablespaceRequest | Unset = UNSET,
) -> Response[Any | Error | TablespaceCatalogRecord]:
    """Create tablespace

     Creates a tablespace catalog object. The server applies the same lifecycle semantics as `CREATE
    TABLESPACE`.

    Args:
        tablespace_name (str):
        body (CreateTablespaceRequest | Unset): Tablespace creation request. Placement policy is
            fail-closed until native placement planning consumes it.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | TablespaceCatalogRecord]
    """

    kwargs = _get_kwargs(
        tablespace_name=tablespace_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateTablespaceRequest | Unset = UNSET,
) -> Any | Error | TablespaceCatalogRecord | None:
    """Create tablespace

     Creates a tablespace catalog object. The server applies the same lifecycle semantics as `CREATE
    TABLESPACE`.

    Args:
        tablespace_name (str):
        body (CreateTablespaceRequest | Unset): Tablespace creation request. Placement policy is
            fail-closed until native placement planning consumes it.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | TablespaceCatalogRecord
    """

    return (
        await asyncio_detailed(
            tablespace_name=tablespace_name,
            client=client,
            body=body,
        )
    ).parsed
