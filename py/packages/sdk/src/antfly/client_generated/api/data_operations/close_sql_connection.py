from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ...client import AuthenticatedClient, Client
from ...models.close_sql_connection_response_200 import CloseSQLConnectionResponse200
from ...models.sql_diagnostic import SQLDiagnostic
from ...types import Response


def _get_kwargs(
    connection_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/db/v1/sql/connections/{connection_id}".format(
            connection_id=quote(str(connection_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> CloseSQLConnectionResponse200 | SQLDiagnostic:
    if response.status_code == 200:
        response_200 = CloseSQLConnectionResponse200.from_dict(response.json())

        return response_200

    response_default = SQLDiagnostic.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[CloseSQLConnectionResponse200 | SQLDiagnostic]:
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
) -> Response[CloseSQLConnectionResponse200 | SQLDiagnostic]:
    """Close an idle SQL connection and its prepared resources

     Refuses an active, beginning or uncertain transaction; route to owner_node_id.

    Args:
        connection_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[CloseSQLConnectionResponse200 | SQLDiagnostic]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    connection_id: str,
    *,
    client: AuthenticatedClient,
) -> CloseSQLConnectionResponse200 | SQLDiagnostic | None:
    """Close an idle SQL connection and its prepared resources

     Refuses an active, beginning or uncertain transaction; route to owner_node_id.

    Args:
        connection_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        CloseSQLConnectionResponse200 | SQLDiagnostic
    """

    return sync_detailed(
        connection_id=connection_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    connection_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[CloseSQLConnectionResponse200 | SQLDiagnostic]:
    """Close an idle SQL connection and its prepared resources

     Refuses an active, beginning or uncertain transaction; route to owner_node_id.

    Args:
        connection_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[CloseSQLConnectionResponse200 | SQLDiagnostic]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    connection_id: str,
    *,
    client: AuthenticatedClient,
) -> CloseSQLConnectionResponse200 | SQLDiagnostic | None:
    """Close an idle SQL connection and its prepared resources

     Refuses an active, beginning or uncertain transaction; route to owner_node_id.

    Args:
        connection_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        CloseSQLConnectionResponse200 | SQLDiagnostic
    """

    return (
        await asyncio_detailed(
            connection_id=connection_id,
            client=client,
        )
    ).parsed
