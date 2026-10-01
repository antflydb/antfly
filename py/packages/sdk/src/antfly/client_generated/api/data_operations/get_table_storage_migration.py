from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.get_table_storage_migration_response_200 import GetTableStorageMigrationResponse200
from ...types import Response


def _get_kwargs(
    table_name: str,
    job_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/storage/migrations/{job_id}".format(
            table_name=quote(str(table_name), safe=""),
            job_id=quote(str(job_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | GetTableStorageMigrationResponse200 | None:
    if response.status_code == 200:
        response_200 = GetTableStorageMigrationResponse200.from_dict(response.json())

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
) -> Response[Any | Error | GetTableStorageMigrationResponse200]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | Error | GetTableStorageMigrationResponse200]:
    """Read a table storage migration receipt

     Table-admin observation only. Does not admit, advance, or publish a job.
    A phase of admitted means catalog admission is durable but DB preparation
    has not begun; retry creation or send a job action to recover it. The
    receipt is retained until a later migration replaces it; this endpoint
    is not a permanent job history.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | GetTableStorageMigrationResponse200]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        job_id=job_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> Any | Error | GetTableStorageMigrationResponse200 | None:
    """Read a table storage migration receipt

     Table-admin observation only. Does not admit, advance, or publish a job.
    A phase of admitted means catalog admission is durable but DB preparation
    has not begun; retry creation or send a job action to recover it. The
    receipt is retained until a later migration replaces it; this endpoint
    is not a permanent job history.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | GetTableStorageMigrationResponse200
    """

    return sync_detailed(
        table_name=table_name,
        job_id=job_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | Error | GetTableStorageMigrationResponse200]:
    """Read a table storage migration receipt

     Table-admin observation only. Does not admit, advance, or publish a job.
    A phase of admitted means catalog admission is durable but DB preparation
    has not begun; retry creation or send a job action to recover it. The
    receipt is retained until a later migration replaces it; this endpoint
    is not a permanent job history.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | GetTableStorageMigrationResponse200]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        job_id=job_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> Any | Error | GetTableStorageMigrationResponse200 | None:
    """Read a table storage migration receipt

     Table-admin observation only. Does not admit, advance, or publish a job.
    A phase of admitted means catalog admission is durable but DB preparation
    has not begun; retry creation or send a job action to recover it. The
    receipt is retained until a later migration replaces it; this endpoint
    is not a permanent job history.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | GetTableStorageMigrationResponse200
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            job_id=job_id,
            client=client,
        )
    ).parsed
