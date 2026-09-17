from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_table_storage_migration_body import CreateTableStorageMigrationBody
from ...models.create_table_storage_migration_response_200 import CreateTableStorageMigrationResponse200
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: CreateTableStorageMigrationBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/storage/migrations".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | CreateTableStorageMigrationResponse200 | Error | None:
    if response.status_code == 200:
        response_200 = CreateTableStorageMigrationResponse200.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | CreateTableStorageMigrationResponse200 | Error]:
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
    body: CreateTableStorageMigrationBody,
) -> Response[Any | CreateTableStorageMigrationResponse200 | Error]:
    """Create or resume a table storage migration job

     Table-admin operation for local single-shard standalone tables. Target
    vector_store changes primary_lsm source ownership without changing models,
    dimensions, artifacts or logical indexes. Retry creation with the same
    job_id, target and budgets. The job is advanced explicitly through its
    job endpoint; the server does not schedule an unattended migration loop.
    Offline migration uses antfly storage migrate against a stopped server.

    Args:
        table_name (str):
        body (CreateTableStorageMigrationBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | CreateTableStorageMigrationResponse200 | Error]
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
    body: CreateTableStorageMigrationBody,
) -> Any | CreateTableStorageMigrationResponse200 | Error | None:
    """Create or resume a table storage migration job

     Table-admin operation for local single-shard standalone tables. Target
    vector_store changes primary_lsm source ownership without changing models,
    dimensions, artifacts or logical indexes. Retry creation with the same
    job_id, target and budgets. The job is advanced explicitly through its
    job endpoint; the server does not schedule an unattended migration loop.
    Offline migration uses antfly storage migrate against a stopped server.

    Args:
        table_name (str):
        body (CreateTableStorageMigrationBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | CreateTableStorageMigrationResponse200 | Error
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
    body: CreateTableStorageMigrationBody,
) -> Response[Any | CreateTableStorageMigrationResponse200 | Error]:
    """Create or resume a table storage migration job

     Table-admin operation for local single-shard standalone tables. Target
    vector_store changes primary_lsm source ownership without changing models,
    dimensions, artifacts or logical indexes. Retry creation with the same
    job_id, target and budgets. The job is advanced explicitly through its
    job endpoint; the server does not schedule an unattended migration loop.
    Offline migration uses antfly storage migrate against a stopped server.

    Args:
        table_name (str):
        body (CreateTableStorageMigrationBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | CreateTableStorageMigrationResponse200 | Error]
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
    body: CreateTableStorageMigrationBody,
) -> Any | CreateTableStorageMigrationResponse200 | Error | None:
    """Create or resume a table storage migration job

     Table-admin operation for local single-shard standalone tables. Target
    vector_store changes primary_lsm source ownership without changing models,
    dimensions, artifacts or logical indexes. Retry creation with the same
    job_id, target and budgets. The job is advanced explicitly through its
    job endpoint; the server does not schedule an unattended migration loop.
    Offline migration uses antfly storage migrate against a stopped server.

    Args:
        table_name (str):
        body (CreateTableStorageMigrationBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | CreateTableStorageMigrationResponse200 | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
