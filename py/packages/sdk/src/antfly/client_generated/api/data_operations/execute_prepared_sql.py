from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ....sql_transport import sql_request, sql_request_async
from ...client import AuthenticatedClient, Client
from ...models.sql_diagnostic import SQLDiagnostic
from ...models.sql_prepared_execution_request import SQLPreparedExecutionRequest
from ...models.sql_response import SQLResponse
from ...types import Response


def _get_kwargs(
    prepared_id: str,
    *,
    body: SQLPreparedExecutionRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/sql/prepared/{prepared_id}/execute".format(
            prepared_id=quote(str(prepared_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> SQLDiagnostic | SQLResponse:
    if response.status_code == 200:
        response_200 = SQLResponse.from_dict(response.json())

        return response_200

    response_default = SQLDiagnostic.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[SQLDiagnostic | SQLResponse]:
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
    body: SQLPreparedExecutionRequest,
) -> Response[SQLDiagnostic | SQLResponse]:
    """Execute a durable prepared SQL resource

     Uses the stored statement, namespace and immutable binding identities. Resource admission linearizes
    when its durable record is loaded; a later close or expiry does not cancel that already admitted
    execution. Mutation outcomes must be reconciled rather than replayed after ambiguous errors.

    Args:
        prepared_id (str):
        body (SQLPreparedExecutionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[SQLDiagnostic | SQLResponse]
    """

    kwargs = _get_kwargs(
        prepared_id=prepared_id,
        body=body,
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
    body: SQLPreparedExecutionRequest,
) -> SQLDiagnostic | SQLResponse | None:
    """Execute a durable prepared SQL resource

     Uses the stored statement, namespace and immutable binding identities. Resource admission linearizes
    when its durable record is loaded; a later close or expiry does not cancel that already admitted
    execution. Mutation outcomes must be reconciled rather than replayed after ambiguous errors.

    Args:
        prepared_id (str):
        body (SQLPreparedExecutionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        SQLDiagnostic | SQLResponse
    """

    return sync_detailed(
        prepared_id=prepared_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    prepared_id: str,
    *,
    client: AuthenticatedClient,
    body: SQLPreparedExecutionRequest,
) -> Response[SQLDiagnostic | SQLResponse]:
    """Execute a durable prepared SQL resource

     Uses the stored statement, namespace and immutable binding identities. Resource admission linearizes
    when its durable record is loaded; a later close or expiry does not cancel that already admitted
    execution. Mutation outcomes must be reconciled rather than replayed after ambiguous errors.

    Args:
        prepared_id (str):
        body (SQLPreparedExecutionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[SQLDiagnostic | SQLResponse]
    """

    kwargs = _get_kwargs(
        prepared_id=prepared_id,
        body=body,
    )

    response = await sql_request_async(client.get_async_httpx_client(), **kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    prepared_id: str,
    *,
    client: AuthenticatedClient,
    body: SQLPreparedExecutionRequest,
) -> SQLDiagnostic | SQLResponse | None:
    """Execute a durable prepared SQL resource

     Uses the stored statement, namespace and immutable binding identities. Resource admission linearizes
    when its durable record is loaded; a later close or expiry does not cancel that already admitted
    execution. Mutation outcomes must be reconciled rather than replayed after ambiguous errors.

    Args:
        prepared_id (str):
        body (SQLPreparedExecutionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        SQLDiagnostic | SQLResponse
    """

    return (
        await asyncio_detailed(
            prepared_id=prepared_id,
            client=client,
            body=body,
        )
    ).parsed
