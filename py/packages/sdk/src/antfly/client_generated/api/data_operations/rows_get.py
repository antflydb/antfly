from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.rows_get_request import RowsGetRequest
from ...models.rows_get_result_set import RowsGetResultSet
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: RowsGetRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/rows/get".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | RowsGetResultSet | None:
    if response.status_code == 200:
        response_200 = RowsGetResultSet.from_dict(response.json())

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
) -> Response[Error | RowsGetResultSet]:
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
    body: RowsGetRequest,
) -> Response[Error | RowsGetResultSet]:
    """Lookup relational rows by structured row identity

     Point lookup endpoint for relational tables with a declared
    `primary_key`. The request uses structured primary-key selectors or
    declared unique-key selectors. Unique selectors resolve through durable
    unique-owner rows; a missing unique owner returns `found: false`
    without scanning. Responses include row JSON, version, and optional
    diagnostic physical keys.

    Args:
        table_name (str):
        body (RowsGetRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowsGetResultSet]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RowsGetRequest,
) -> Error | RowsGetResultSet | None:
    """Lookup relational rows by structured row identity

     Point lookup endpoint for relational tables with a declared
    `primary_key`. The request uses structured primary-key selectors or
    declared unique-key selectors. Unique selectors resolve through durable
    unique-owner rows; a missing unique owner returns `found: false`
    without scanning. Responses include row JSON, version, and optional
    diagnostic physical keys.

    Args:
        table_name (str):
        body (RowsGetRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowsGetResultSet
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RowsGetRequest,
) -> Response[Error | RowsGetResultSet]:
    """Lookup relational rows by structured row identity

     Point lookup endpoint for relational tables with a declared
    `primary_key`. The request uses structured primary-key selectors or
    declared unique-key selectors. Unique selectors resolve through durable
    unique-owner rows; a missing unique owner returns `found: false`
    without scanning. Responses include row JSON, version, and optional
    diagnostic physical keys.

    Args:
        table_name (str):
        body (RowsGetRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowsGetResultSet]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RowsGetRequest,
) -> Error | RowsGetResultSet | None:
    """Lookup relational rows by structured row identity

     Point lookup endpoint for relational tables with a declared
    `primary_key`. The request uses structured primary-key selectors or
    declared unique-key selectors. Unique selectors resolve through durable
    unique-owner rows; a missing unique owner returns `found: false`
    without scanning. Responses include row JSON, version, and optional
    diagnostic physical keys.

    Args:
        table_name (str):
        body (RowsGetRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowsGetResultSet
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
