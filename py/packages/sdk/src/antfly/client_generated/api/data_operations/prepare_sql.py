from http import HTTPStatus
from typing import Any

import httpx

from ....sql_transport import sql_request, sql_request_async
from ...client import AuthenticatedClient, Client
from ...models.sql_diagnostic import SQLDiagnostic
from ...models.sql_prepare_request import SQLPrepareRequest
from ...models.sql_prepared_response import SQLPreparedResponse
from ...types import Response


def _get_kwargs(
    *,
    body: SQLPrepareRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/sql/prepared",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> SQLDiagnostic | SQLPreparedResponse:
    if response.status_code == 200:
        response_200 = SQLPreparedResponse.from_dict(response.json())

        return response_200

    response_default = SQLDiagnostic.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[SQLDiagnostic | SQLPreparedResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: SQLPrepareRequest,
) -> Response[SQLDiagnostic | SQLPreparedResponse]:
    """Create a durable prepared SQL resource

     Binds SELECT, INSERT, UPDATE or DELETE without executing it. The immutable resource belongs to the
    authenticated principal and API node, expires after one hour, and survives transaction COMMIT and
    owner restart with the same durable store and node identity. A session-bound resource is executable
    only while its attached session remains active. Owner failover is not automatic; execute and close
    must reach owner_node_id. Each durable store admits at most 128 resources and 4 MiB of serialized
    prepared state. Expired resources are reclaimed atomically during subsequent creation or close.
    Execution authenticates and authorizes again and rejects changed catalog identities or schemas. When
    session_id is supplied, preparation uses the attached session's authenticated database, namespace
    and setting overlay under its execution lease. The resource is bound to that session; execution re-
    reads current values but rejects a changed setting catalog. A native durable session store is
    required.

    Args:
        body (SQLPrepareRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[SQLDiagnostic | SQLPreparedResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = sql_request(
        client.get_httpx_client(),
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    body: SQLPrepareRequest,
) -> SQLDiagnostic | SQLPreparedResponse | None:
    """Create a durable prepared SQL resource

     Binds SELECT, INSERT, UPDATE or DELETE without executing it. The immutable resource belongs to the
    authenticated principal and API node, expires after one hour, and survives transaction COMMIT and
    owner restart with the same durable store and node identity. A session-bound resource is executable
    only while its attached session remains active. Owner failover is not automatic; execute and close
    must reach owner_node_id. Each durable store admits at most 128 resources and 4 MiB of serialized
    prepared state. Expired resources are reclaimed atomically during subsequent creation or close.
    Execution authenticates and authorizes again and rejects changed catalog identities or schemas. When
    session_id is supplied, preparation uses the attached session's authenticated database, namespace
    and setting overlay under its execution lease. The resource is bound to that session; execution re-
    reads current values but rejects a changed setting catalog. A native durable session store is
    required.

    Args:
        body (SQLPrepareRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        SQLDiagnostic | SQLPreparedResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: SQLPrepareRequest,
) -> Response[SQLDiagnostic | SQLPreparedResponse]:
    """Create a durable prepared SQL resource

     Binds SELECT, INSERT, UPDATE or DELETE without executing it. The immutable resource belongs to the
    authenticated principal and API node, expires after one hour, and survives transaction COMMIT and
    owner restart with the same durable store and node identity. A session-bound resource is executable
    only while its attached session remains active. Owner failover is not automatic; execute and close
    must reach owner_node_id. Each durable store admits at most 128 resources and 4 MiB of serialized
    prepared state. Expired resources are reclaimed atomically during subsequent creation or close.
    Execution authenticates and authorizes again and rejects changed catalog identities or schemas. When
    session_id is supplied, preparation uses the attached session's authenticated database, namespace
    and setting overlay under its execution lease. The resource is bound to that session; execution re-
    reads current values but rejects a changed setting catalog. A native durable session store is
    required.

    Args:
        body (SQLPrepareRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[SQLDiagnostic | SQLPreparedResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await sql_request_async(client.get_async_httpx_client(), **kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: SQLPrepareRequest,
) -> SQLDiagnostic | SQLPreparedResponse | None:
    """Create a durable prepared SQL resource

     Binds SELECT, INSERT, UPDATE or DELETE without executing it. The immutable resource belongs to the
    authenticated principal and API node, expires after one hour, and survives transaction COMMIT and
    owner restart with the same durable store and node identity. A session-bound resource is executable
    only while its attached session remains active. Owner failover is not automatic; execute and close
    must reach owner_node_id. Each durable store admits at most 128 resources and 4 MiB of serialized
    prepared state. Expired resources are reclaimed atomically during subsequent creation or close.
    Execution authenticates and authorizes again and rejects changed catalog identities or schemas. When
    session_id is supplied, preparation uses the attached session's authenticated database, namespace
    and setting overlay under its execution lease. The resource is bound to that session; execution re-
    reads current values but rejects a changed setting catalog. A native durable session store is
    required.

    Args:
        body (SQLPrepareRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        SQLDiagnostic | SQLPreparedResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
