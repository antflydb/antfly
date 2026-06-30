from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.sql_statement_request import SqlStatementRequest
from ...models.sql_statement_response import SqlStatementResponse
from ...types import Response


def _get_kwargs(
    *,
    body: SqlStatementRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/sql",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | SqlStatementResponse | None:
    if response.status_code == 200:
        response_200 = SqlStatementResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 408:
        response_408 = cast(Any, None)
        return response_408

    if response.status_code == 501:
        response_501 = cast(Any, None)
        return response_501

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | SqlStatementResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: SqlStatementRequest,
) -> Response[Any | Error | SqlStatementResponse]:
    """Execute SQL text in a logical SQL session

     Executes SQL through Antfly's psql-style HTTP ingress. The request is a
    single synchronous statement and the response returns the logical
    `session_id` that should be supplied on later requests when session
    state such as prepared statements, LISTEN/NOTIFY subscriptions, or SQL
    catalog defaults must be reused. Prepared statements and cursors are
    SQL session state, not durable REST resources. Cursor-backed fetches and
    asynchronous statement jobs are reserved for later extensions.

    Args:
        body (SqlStatementRequest): Synchronous SQL statement request. `session_id` is optional on
            the first
            request and should be reused from prior responses when SQL session state
            must persist across requests.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | SqlStatementResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    body: SqlStatementRequest,
) -> Any | Error | SqlStatementResponse | None:
    """Execute SQL text in a logical SQL session

     Executes SQL through Antfly's psql-style HTTP ingress. The request is a
    single synchronous statement and the response returns the logical
    `session_id` that should be supplied on later requests when session
    state such as prepared statements, LISTEN/NOTIFY subscriptions, or SQL
    catalog defaults must be reused. Prepared statements and cursors are
    SQL session state, not durable REST resources. Cursor-backed fetches and
    asynchronous statement jobs are reserved for later extensions.

    Args:
        body (SqlStatementRequest): Synchronous SQL statement request. `session_id` is optional on
            the first
            request and should be reused from prior responses when SQL session state
            must persist across requests.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | SqlStatementResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: SqlStatementRequest,
) -> Response[Any | Error | SqlStatementResponse]:
    """Execute SQL text in a logical SQL session

     Executes SQL through Antfly's psql-style HTTP ingress. The request is a
    single synchronous statement and the response returns the logical
    `session_id` that should be supplied on later requests when session
    state such as prepared statements, LISTEN/NOTIFY subscriptions, or SQL
    catalog defaults must be reused. Prepared statements and cursors are
    SQL session state, not durable REST resources. Cursor-backed fetches and
    asynchronous statement jobs are reserved for later extensions.

    Args:
        body (SqlStatementRequest): Synchronous SQL statement request. `session_id` is optional on
            the first
            request and should be reused from prior responses when SQL session state
            must persist across requests.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | SqlStatementResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: SqlStatementRequest,
) -> Any | Error | SqlStatementResponse | None:
    """Execute SQL text in a logical SQL session

     Executes SQL through Antfly's psql-style HTTP ingress. The request is a
    single synchronous statement and the response returns the logical
    `session_id` that should be supplied on later requests when session
    state such as prepared statements, LISTEN/NOTIFY subscriptions, or SQL
    catalog defaults must be reused. Prepared statements and cursors are
    SQL session state, not durable REST resources. Cursor-backed fetches and
    asynchronous statement jobs are reserved for later extensions.

    Args:
        body (SqlStatementRequest): Synchronous SQL statement request. `session_id` is optional on
            the first
            request and should be reused from prior responses when SQL session state
            must persist across requests.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | SqlStatementResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
