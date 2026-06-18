from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.batch_response import BatchResponse
from ...models.error import Error
from ...models.rows_batch_request import RowsBatchRequest
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: RowsBatchRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/rows/batch".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> BatchResponse | Error | None:
    if response.status_code == 201:
        response_201 = BatchResponse.from_dict(response.json())

        return response_201

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
) -> Response[BatchResponse | Error]:
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
    body: RowsBatchRequest,
) -> Response[BatchResponse | Error]:
    """Perform structured relational row writes by row identity

     Relational row batch endpoint for tables with a declared `primary_key`.
    Callers address rows by structured primary-key identity or declared
    unique-key identity instead of physical document keys. The server
    derives the storage-owned physical row key from the canonical typed
    primary-key tuple, or resolves a unique selector through durable
    unique-owner rows, then executes through the normal batch/2PC path.

    Args:
        table_name (str):
        body (RowsBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[BatchResponse | Error]
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
    body: RowsBatchRequest,
) -> BatchResponse | Error | None:
    """Perform structured relational row writes by row identity

     Relational row batch endpoint for tables with a declared `primary_key`.
    Callers address rows by structured primary-key identity or declared
    unique-key identity instead of physical document keys. The server
    derives the storage-owned physical row key from the canonical typed
    primary-key tuple, or resolves a unique selector through durable
    unique-owner rows, then executes through the normal batch/2PC path.

    Args:
        table_name (str):
        body (RowsBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        BatchResponse | Error
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
    body: RowsBatchRequest,
) -> Response[BatchResponse | Error]:
    """Perform structured relational row writes by row identity

     Relational row batch endpoint for tables with a declared `primary_key`.
    Callers address rows by structured primary-key identity or declared
    unique-key identity instead of physical document keys. The server
    derives the storage-owned physical row key from the canonical typed
    primary-key tuple, or resolves a unique selector through durable
    unique-owner rows, then executes through the normal batch/2PC path.

    Args:
        table_name (str):
        body (RowsBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[BatchResponse | Error]
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
    body: RowsBatchRequest,
) -> BatchResponse | Error | None:
    """Perform structured relational row writes by row identity

     Relational row batch endpoint for tables with a declared `primary_key`.
    Callers address rows by structured primary-key identity or declared
    unique-key identity instead of physical document keys. The server
    derives the storage-owned physical row key from the canonical typed
    primary-key tuple, or resolves a unique selector through durable
    unique-owner rows, then executes through the normal batch/2PC path.

    Args:
        table_name (str):
        body (RowsBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        BatchResponse | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
