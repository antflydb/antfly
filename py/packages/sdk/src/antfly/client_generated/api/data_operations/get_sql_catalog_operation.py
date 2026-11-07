from http import HTTPStatus
from typing import Any

import httpx

from ...client import AuthenticatedClient, Client
from ...models.sql_catalog_operation_receipt import SQLCatalogOperationReceipt
from ...models.sql_catalog_recovery_request import SQLCatalogRecoveryRequest
from ...models.sql_diagnostic import SQLDiagnostic
from ...types import Response


def _get_kwargs(
    *,
    body: SQLCatalogRecoveryRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/sql/operations/status",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> SQLCatalogOperationReceipt | SQLDiagnostic:
    if response.status_code == 200:
        response_200 = SQLCatalogOperationReceipt.from_dict(response.json())

        return response_200

    response_default = SQLDiagnostic.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[SQLCatalogOperationReceipt | SQLDiagnostic]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: SQLCatalogRecoveryRequest,
) -> Response[SQLCatalogOperationReceipt | SQLDiagnostic]:
    """Observe an exact retained SQL catalog operation

     Read-only, body-bound observation of the original native catalog operation.
    Requires current administrative permission on its qualified logical resource.
    It does not execute, rebind, retry or admit a mutation. A retained rejection
    includes its precise SQL diagnostic. Unknown or evicted outcomes remain
    non-retryable SQLSTATE 40003; reconcile administratively rather than replaying.

    Args:
        body (SQLCatalogRecoveryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[SQLCatalogOperationReceipt | SQLDiagnostic]
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
    body: SQLCatalogRecoveryRequest,
) -> SQLCatalogOperationReceipt | SQLDiagnostic | None:
    """Observe an exact retained SQL catalog operation

     Read-only, body-bound observation of the original native catalog operation.
    Requires current administrative permission on its qualified logical resource.
    It does not execute, rebind, retry or admit a mutation. A retained rejection
    includes its precise SQL diagnostic. Unknown or evicted outcomes remain
    non-retryable SQLSTATE 40003; reconcile administratively rather than replaying.

    Args:
        body (SQLCatalogRecoveryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        SQLCatalogOperationReceipt | SQLDiagnostic
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: SQLCatalogRecoveryRequest,
) -> Response[SQLCatalogOperationReceipt | SQLDiagnostic]:
    """Observe an exact retained SQL catalog operation

     Read-only, body-bound observation of the original native catalog operation.
    Requires current administrative permission on its qualified logical resource.
    It does not execute, rebind, retry or admit a mutation. A retained rejection
    includes its precise SQL diagnostic. Unknown or evicted outcomes remain
    non-retryable SQLSTATE 40003; reconcile administratively rather than replaying.

    Args:
        body (SQLCatalogRecoveryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[SQLCatalogOperationReceipt | SQLDiagnostic]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: SQLCatalogRecoveryRequest,
) -> SQLCatalogOperationReceipt | SQLDiagnostic | None:
    """Observe an exact retained SQL catalog operation

     Read-only, body-bound observation of the original native catalog operation.
    Requires current administrative permission on its qualified logical resource.
    It does not execute, rebind, retry or admit a mutation. A retained rejection
    includes its precise SQL diagnostic. Unknown or evicted outcomes remain
    non-retryable SQLSTATE 40003; reconcile administratively rather than replaying.

    Args:
        body (SQLCatalogRecoveryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        SQLCatalogOperationReceipt | SQLDiagnostic
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
