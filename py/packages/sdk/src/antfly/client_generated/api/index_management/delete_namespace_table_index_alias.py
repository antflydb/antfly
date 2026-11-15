from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    alias_name: str,
    *,
    if_match: str,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    headers["If-Match"] = if_match

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/index-aliases/{alias_name}".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            table_name=quote(str(table_name), safe=""),
            alias_name=quote(str(alias_name), safe=""),
        ),
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | Error | None:
    if response.status_code == 200:
        response_200 = cast(Any, None)
        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 412:
        response_412 = cast(Any, None)
        return response_412

    if response.status_code == 428:
        response_428 = cast(Any, None)
        return response_428

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 501:
        response_501 = cast(Any, None)
        return response_501

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Any | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    alias_name: str,
    *,
    client: AuthenticatedClient,
    if_match: str,
) -> Response[Any | Error]:
    """Conditionally delete an index alias

     Requires table administration and the exact ETag returned by GET. The whole table-definition CAS
    prevents concurrent swaps or recreations from being deleted. Physical indexes and retained PUT
    idempotency receipts remain unchanged. A retry after successful deletion returns 404; a stale
    validator against a recreated alias returns 412.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        alias_name (str):
        if_match (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        alias_name=alias_name,
        if_match=if_match,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    table_name: str,
    alias_name: str,
    *,
    client: AuthenticatedClient,
    if_match: str,
) -> Any | Error | None:
    """Conditionally delete an index alias

     Requires table administration and the exact ETag returned by GET. The whole table-definition CAS
    prevents concurrent swaps or recreations from being deleted. Physical indexes and retained PUT
    idempotency receipts remain unchanged. A retry after successful deletion returns 404; a stale
    validator against a recreated alias returns 412.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        alias_name (str):
        if_match (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        alias_name=alias_name,
        client=client,
        if_match=if_match,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    alias_name: str,
    *,
    client: AuthenticatedClient,
    if_match: str,
) -> Response[Any | Error]:
    """Conditionally delete an index alias

     Requires table administration and the exact ETag returned by GET. The whole table-definition CAS
    prevents concurrent swaps or recreations from being deleted. Physical indexes and retained PUT
    idempotency receipts remain unchanged. A retry after successful deletion returns 404; a stale
    validator against a recreated alias returns 412.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        alias_name (str):
        if_match (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        alias_name=alias_name,
        if_match=if_match,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    table_name: str,
    alias_name: str,
    *,
    client: AuthenticatedClient,
    if_match: str,
) -> Any | Error | None:
    """Conditionally delete an index alias

     Requires table administration and the exact ETag returned by GET. The whole table-definition CAS
    prevents concurrent swaps or recreations from being deleted. Physical indexes and retained PUT
    idempotency receipts remain unchanged. A retry after successful deletion returns 404; a stale
    validator against a recreated alias returns 412.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        alias_name (str):
        if_match (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            alias_name=alias_name,
            client=client,
            if_match=if_match,
        )
    ).parsed
