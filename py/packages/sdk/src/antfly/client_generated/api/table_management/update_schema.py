from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.table import Table
from ...models.table_schema import TableSchema
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    *,
    body: TableSchema,
    if_match: str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(if_match, Unset):
        headers["If-Match"] = if_match

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/db/v1/tables/{table_name}/schema".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | Table | None:
    if response.status_code == 200:
        response_200 = Table.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Error | Table]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: TableSchema,
    if_match: str | Unset = UNSET,
) -> Response[Error | Table]:
    """Replace a table's schema

     Replaces the complete table schema. Properties omitted from the request
    are removed. Use PATCH on this path for a partial JSON Merge Patch update.

    Args:
        table_name (str):
        if_match (str | Unset):  Example: "schema-0".
        body (TableSchema): Schema definition for a table with multiple document types

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | Table]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
        if_match=if_match,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: TableSchema,
    if_match: str | Unset = UNSET,
) -> Error | Table | None:
    """Replace a table's schema

     Replaces the complete table schema. Properties omitted from the request
    are removed. Use PATCH on this path for a partial JSON Merge Patch update.

    Args:
        table_name (str):
        if_match (str | Unset):  Example: "schema-0".
        body (TableSchema): Schema definition for a table with multiple document types

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | Table
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        body=body,
        if_match=if_match,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: TableSchema,
    if_match: str | Unset = UNSET,
) -> Response[Error | Table]:
    """Replace a table's schema

     Replaces the complete table schema. Properties omitted from the request
    are removed. Use PATCH on this path for a partial JSON Merge Patch update.

    Args:
        table_name (str):
        if_match (str | Unset):  Example: "schema-0".
        body (TableSchema): Schema definition for a table with multiple document types

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | Table]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
        if_match=if_match,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: TableSchema,
    if_match: str | Unset = UNSET,
) -> Error | Table | None:
    """Replace a table's schema

     Replaces the complete table schema. Properties omitted from the request
    are removed. Use PATCH on this path for a partial JSON Merge Patch update.

    Args:
        table_name (str):
        if_match (str | Unset):  Example: "schema-0".
        body (TableSchema): Schema definition for a table with multiple document types

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | Table
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
            if_match=if_match,
        )
    ).parsed
