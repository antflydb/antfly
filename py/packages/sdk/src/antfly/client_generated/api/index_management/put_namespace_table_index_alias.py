from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.index_alias import IndexAlias
from ...models.index_alias_request import IndexAliasRequest
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    alias_name: str,
    *,
    body: IndexAliasRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/index-aliases/{alias_name}".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            table_name=quote(str(table_name), safe=""),
            alias_name=quote(str(alias_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | IndexAlias | None:
    if response.status_code == 200:
        response_200 = IndexAlias.from_dict(response.json())

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


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | IndexAlias]:
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
    body: IndexAliasRequest,
) -> Response[Any | Error | IndexAlias]:
    """Atomically create or swap an index alias

     Requires table administration. The target must satisfy the reported readiness proof. Creation
    requires expected_index to be absent; replacement requires the exact current target. Retry the
    identical request with the same idempotency key to reconcile an ambiguous response, including after
    later swaps. Receipts are retained for at least 24 hours, with at most 128 retained receipts per
    alias, 64 aliases per table, and 512 KiB of retained alias control state per table. Every mutation
    reclaims expired receipts across the table; capacity exhaustion rejects new mutations with HTTP 409.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        alias_name (str):
        body (IndexAliasRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | IndexAlias]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        alias_name=alias_name,
        body=body,
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
    body: IndexAliasRequest,
) -> Any | Error | IndexAlias | None:
    """Atomically create or swap an index alias

     Requires table administration. The target must satisfy the reported readiness proof. Creation
    requires expected_index to be absent; replacement requires the exact current target. Retry the
    identical request with the same idempotency key to reconcile an ambiguous response, including after
    later swaps. Receipts are retained for at least 24 hours, with at most 128 retained receipts per
    alias, 64 aliases per table, and 512 KiB of retained alias control state per table. Every mutation
    reclaims expired receipts across the table; capacity exhaustion rejects new mutations with HTTP 409.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        alias_name (str):
        body (IndexAliasRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | IndexAlias
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        alias_name=alias_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    alias_name: str,
    *,
    client: AuthenticatedClient,
    body: IndexAliasRequest,
) -> Response[Any | Error | IndexAlias]:
    """Atomically create or swap an index alias

     Requires table administration. The target must satisfy the reported readiness proof. Creation
    requires expected_index to be absent; replacement requires the exact current target. Retry the
    identical request with the same idempotency key to reconcile an ambiguous response, including after
    later swaps. Receipts are retained for at least 24 hours, with at most 128 retained receipts per
    alias, 64 aliases per table, and 512 KiB of retained alias control state per table. Every mutation
    reclaims expired receipts across the table; capacity exhaustion rejects new mutations with HTTP 409.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        alias_name (str):
        body (IndexAliasRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | IndexAlias]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        alias_name=alias_name,
        body=body,
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
    body: IndexAliasRequest,
) -> Any | Error | IndexAlias | None:
    """Atomically create or swap an index alias

     Requires table administration. The target must satisfy the reported readiness proof. Creation
    requires expected_index to be absent; replacement requires the exact current target. Retry the
    identical request with the same idempotency key to reconcile an ambiguous response, including after
    later swaps. Receipts are retained for at least 24 hours, with at most 128 retained receipts per
    alias, 64 aliases per table, and 512 KiB of retained alias control state per table. Every mutation
    reclaims expired receipts across the table; capacity exhaustion rejects new mutations with HTTP 409.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        alias_name (str):
        body (IndexAliasRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | IndexAlias
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            alias_name=alias_name,
            client=client,
            body=body,
        )
    ).parsed
