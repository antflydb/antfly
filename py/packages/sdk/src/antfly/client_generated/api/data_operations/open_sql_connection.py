from http import HTTPStatus
from typing import Any

import httpx

from ...client import AuthenticatedClient, Client
from ...models.sql_connection_open_request import SQLConnectionOpenRequest
from ...models.sql_connection_response import SQLConnectionResponse
from ...models.sql_diagnostic import SQLDiagnostic
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: SQLConnectionOpenRequest | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/sql/connections",
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> SQLConnectionResponse | SQLDiagnostic:
    if response.status_code == 200:
        response_200 = SQLConnectionResponse.from_dict(response.json())

        return response_200

    response_default = SQLDiagnostic.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[SQLConnectionResponse | SQLDiagnostic]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: SQLConnectionOpenRequest | Unset = UNSET,
) -> Response[SQLConnectionResponse | SQLDiagnostic]:
    """Open a durable idle HTTP SQL connection

     Creates a principal- and API-node-owned connection with a one-hour idle-use deadline. Active or
    uncertain transactions retain their exact connection binding until completion or reconciliation;
    expiry is not an abort decision. Send connection_id with later SQL and prepared requests. This ID is
    not a transaction session_id. DISCARD ALL resets only this connection's setting overlay and prepared
    resources, and refuses active or uncertain transactions. Do not automatically replay ambiguous
    create responses.

    Args:
        body (SQLConnectionOpenRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[SQLConnectionResponse | SQLDiagnostic]
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
    body: SQLConnectionOpenRequest | Unset = UNSET,
) -> SQLConnectionResponse | SQLDiagnostic | None:
    """Open a durable idle HTTP SQL connection

     Creates a principal- and API-node-owned connection with a one-hour idle-use deadline. Active or
    uncertain transactions retain their exact connection binding until completion or reconciliation;
    expiry is not an abort decision. Send connection_id with later SQL and prepared requests. This ID is
    not a transaction session_id. DISCARD ALL resets only this connection's setting overlay and prepared
    resources, and refuses active or uncertain transactions. Do not automatically replay ambiguous
    create responses.

    Args:
        body (SQLConnectionOpenRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        SQLConnectionResponse | SQLDiagnostic
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: SQLConnectionOpenRequest | Unset = UNSET,
) -> Response[SQLConnectionResponse | SQLDiagnostic]:
    """Open a durable idle HTTP SQL connection

     Creates a principal- and API-node-owned connection with a one-hour idle-use deadline. Active or
    uncertain transactions retain their exact connection binding until completion or reconciliation;
    expiry is not an abort decision. Send connection_id with later SQL and prepared requests. This ID is
    not a transaction session_id. DISCARD ALL resets only this connection's setting overlay and prepared
    resources, and refuses active or uncertain transactions. Do not automatically replay ambiguous
    create responses.

    Args:
        body (SQLConnectionOpenRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[SQLConnectionResponse | SQLDiagnostic]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: SQLConnectionOpenRequest | Unset = UNSET,
) -> SQLConnectionResponse | SQLDiagnostic | None:
    """Open a durable idle HTTP SQL connection

     Creates a principal- and API-node-owned connection with a one-hour idle-use deadline. Active or
    uncertain transactions retain their exact connection binding until completion or reconciliation;
    expiry is not an abort decision. Send connection_id with later SQL and prepared requests. This ID is
    not a transaction session_id. DISCARD ALL resets only this connection's setting overlay and prepared
    resources, and refuses active or uncertain transactions. Do not automatically replay ambiguous
    create responses.

    Args:
        body (SQLConnectionOpenRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        SQLConnectionResponse | SQLDiagnostic
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
