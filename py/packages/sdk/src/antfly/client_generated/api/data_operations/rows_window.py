from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.rows_stream_result_set import RowsStreamResultSet
from ...models.rows_window_plan_request import RowsWindowPlanRequest
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: RowsWindowPlanRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/rows/window".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | RowsStreamResultSet | None:
    if response.status_code == 200:
        response_200 = RowsStreamResultSet.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = Error.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 501:
        response_501 = Error.from_dict(response.json())

        return response_501

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | RowsStreamResultSet]:
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
    body: RowsWindowPlanRequest,
) -> Response[Error | RowsStreamResultSet]:
    """Execute a typed relational row window plan

    Args:
        table_name (str):
        body (RowsWindowPlanRequest): Typed row-window plan envelope. Accepts exactly `window`
            plus optional ordered `ctes` and declared `ranges`.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowsStreamResultSet]
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
    body: RowsWindowPlanRequest,
) -> Error | RowsStreamResultSet | None:
    """Execute a typed relational row window plan

    Args:
        table_name (str):
        body (RowsWindowPlanRequest): Typed row-window plan envelope. Accepts exactly `window`
            plus optional ordered `ctes` and declared `ranges`.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowsStreamResultSet
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
    body: RowsWindowPlanRequest,
) -> Response[Error | RowsStreamResultSet]:
    """Execute a typed relational row window plan

    Args:
        table_name (str):
        body (RowsWindowPlanRequest): Typed row-window plan envelope. Accepts exactly `window`
            plus optional ordered `ctes` and declared `ranges`.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowsStreamResultSet]
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
    body: RowsWindowPlanRequest,
) -> Error | RowsStreamResultSet | None:
    """Execute a typed relational row window plan

    Args:
        table_name (str):
        body (RowsWindowPlanRequest): Typed row-window plan envelope. Accepts exactly `window`
            plus optional ordered `ctes` and declared `ranges`.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowsStreamResultSet
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
