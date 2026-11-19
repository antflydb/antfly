from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ....sql_transport import sql_request, sql_request_async
from ...client import AuthenticatedClient, Client
from ...models.sql_diagnostic import SQLDiagnostic
from ...models.sql_notification_page import SQLNotificationPage
from ...models.sql_notification_poll_request import SQLNotificationPollRequest
from ...types import Response


def _get_kwargs(
    connection_id: str,
    *,
    body: SQLNotificationPollRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/sql/connections/{connection_id}/notifications".format(
            connection_id=quote(str(connection_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> SQLDiagnostic | SQLNotificationPage:
    if response.status_code == 200:
        response_200 = SQLNotificationPage.from_dict(response.json())

        return response_200

    response_default = SQLDiagnostic.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[SQLDiagnostic | SQLNotificationPage]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    connection_id: str,
    *,
    client: AuthenticatedClient,
    body: SQLNotificationPollRequest,
) -> Response[SQLDiagnostic | SQLNotificationPage]:
    """Receive and acknowledge committed SQL notifications

     Poll a connection's database-scoped LISTEN registrations. This operation
    never evaluates or retries SQL. Delivery is deferred while the connection
    has an active or uncertain transaction. Pages contain at most 128 events
    and 64 KiB of logical channel/payload bytes. A lost response is replayable
    until its cursor is acknowledged. Queue capacity is reserved before the
    sender's irreversible commit decision. Connection close and DISCARD ALL
    retire registrations through the same durable source lifecycle.

    Args:
        connection_id (str):
        body (SQLNotificationPollRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[SQLDiagnostic | SQLNotificationPage]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
        body=body,
    )

    response = sql_request(
        client.get_httpx_client(),
        max_response_bytes=1 << 20,
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    connection_id: str,
    *,
    client: AuthenticatedClient,
    body: SQLNotificationPollRequest,
) -> SQLDiagnostic | SQLNotificationPage | None:
    """Receive and acknowledge committed SQL notifications

     Poll a connection's database-scoped LISTEN registrations. This operation
    never evaluates or retries SQL. Delivery is deferred while the connection
    has an active or uncertain transaction. Pages contain at most 128 events
    and 64 KiB of logical channel/payload bytes. A lost response is replayable
    until its cursor is acknowledged. Queue capacity is reserved before the
    sender's irreversible commit decision. Connection close and DISCARD ALL
    retire registrations through the same durable source lifecycle.

    Args:
        connection_id (str):
        body (SQLNotificationPollRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        SQLDiagnostic | SQLNotificationPage
    """

    return sync_detailed(
        connection_id=connection_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    connection_id: str,
    *,
    client: AuthenticatedClient,
    body: SQLNotificationPollRequest,
) -> Response[SQLDiagnostic | SQLNotificationPage]:
    """Receive and acknowledge committed SQL notifications

     Poll a connection's database-scoped LISTEN registrations. This operation
    never evaluates or retries SQL. Delivery is deferred while the connection
    has an active or uncertain transaction. Pages contain at most 128 events
    and 64 KiB of logical channel/payload bytes. A lost response is replayable
    until its cursor is acknowledged. Queue capacity is reserved before the
    sender's irreversible commit decision. Connection close and DISCARD ALL
    retire registrations through the same durable source lifecycle.

    Args:
        connection_id (str):
        body (SQLNotificationPollRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[SQLDiagnostic | SQLNotificationPage]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
        body=body,
    )

    response = await sql_request_async(client.get_async_httpx_client(), max_response_bytes=1 << 20, **kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    connection_id: str,
    *,
    client: AuthenticatedClient,
    body: SQLNotificationPollRequest,
) -> SQLDiagnostic | SQLNotificationPage | None:
    """Receive and acknowledge committed SQL notifications

     Poll a connection's database-scoped LISTEN registrations. This operation
    never evaluates or retries SQL. Delivery is deferred while the connection
    has an active or uncertain transaction. Pages contain at most 128 events
    and 64 KiB of logical channel/payload bytes. A lost response is replayable
    until its cursor is acknowledged. Queue capacity is reserved before the
    sender's irreversible commit decision. Connection close and DISCARD ALL
    retire registrations through the same durable source lifecycle.

    Args:
        connection_id (str):
        body (SQLNotificationPollRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        SQLDiagnostic | SQLNotificationPage
    """

    return (
        await asyncio_detailed(
            connection_id=connection_id,
            client=client,
            body=body,
        )
    ).parsed
