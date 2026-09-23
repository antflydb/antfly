from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ....sql_transport import sql_request, sql_request_async
from ...client import AuthenticatedClient, Client
from ...models.close_prepared_sql_response_200 import ClosePreparedSQLResponse200
from ...models.sql_diagnostic import SQLDiagnostic
from ...types import Response


def _get_kwargs(
    prepared_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/db/v1/sql/prepared/{prepared_id}".format(
            prepared_id=quote(str(prepared_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ClosePreparedSQLResponse200 | SQLDiagnostic:
    if response.status_code == 200:
        response_200 = ClosePreparedSQLResponse200.from_dict(response.json())

        return response_200

    response_default = SQLDiagnostic.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ClosePreparedSQLResponse200 | SQLDiagnostic]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    prepared_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[ClosePreparedSQLResponse200 | SQLDiagnostic]:
    """Release a durable prepared SQL resource

    Args:
        prepared_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ClosePreparedSQLResponse200 | SQLDiagnostic]
    """

    kwargs = _get_kwargs(
        prepared_id=prepared_id,
    )

    response = sql_request(
        client.get_httpx_client(),
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    prepared_id: str,
    *,
    client: AuthenticatedClient,
) -> ClosePreparedSQLResponse200 | SQLDiagnostic | None:
    """Release a durable prepared SQL resource

    Args:
        prepared_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ClosePreparedSQLResponse200 | SQLDiagnostic
    """

    return sync_detailed(
        prepared_id=prepared_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    prepared_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[ClosePreparedSQLResponse200 | SQLDiagnostic]:
    """Release a durable prepared SQL resource

    Args:
        prepared_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ClosePreparedSQLResponse200 | SQLDiagnostic]
    """

    kwargs = _get_kwargs(
        prepared_id=prepared_id,
    )

    response = await sql_request_async(client.get_async_httpx_client(), **kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    prepared_id: str,
    *,
    client: AuthenticatedClient,
) -> ClosePreparedSQLResponse200 | SQLDiagnostic | None:
    """Release a durable prepared SQL resource

    Args:
        prepared_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ClosePreparedSQLResponse200 | SQLDiagnostic
    """

    return (
        await asyncio_detailed(
            prepared_id=prepared_id,
            client=client,
        )
    ).parsed
