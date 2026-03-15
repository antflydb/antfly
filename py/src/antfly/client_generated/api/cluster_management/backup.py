from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.cluster_backup_request import ClusterBackupRequest
from ...models.cluster_backup_response import ClusterBackupResponse
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    *,
    body: ClusterBackupRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/backup",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ClusterBackupResponse, Error]]:
    if response.status_code == 200:
        response_200 = ClusterBackupResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[ClusterBackupResponse, Error]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: ClusterBackupRequest,
) -> Response[Union[ClusterBackupResponse, Error]]:
    """Backup all tables or selected tables

     Creates a backup of all tables or specified tables. Each table's backup includes:
    - Table metadata (schema, indexes, shard configuration)
    - All shard data (compressed with zstd)

    The backup creates a cluster-level manifest that tracks all included tables
    and their individual backup locations.

    **Storage Locations:**
    - Local filesystem: `file:///path/to/backup`
    - Amazon S3: `s3://bucket-name/path/to/backup`

    **Backup Structure:**
    ```
    {location}/
    ├── {backup_id}-cluster-metadata.json   (cluster manifest)
    ├── {table1}-{backup_id}-metadata.json  (table metadata)
    ├── shard-1-{table1}-{backup_id}.tar.zst
    ├── shard-2-{table1}-{backup_id}.tar.zst
    ├── {table2}-{backup_id}-metadata.json
    └── ...
    ```

    Args:
        body (ClusterBackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ClusterBackupResponse, Error]]
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
    body: ClusterBackupRequest,
) -> Optional[Union[ClusterBackupResponse, Error]]:
    """Backup all tables or selected tables

     Creates a backup of all tables or specified tables. Each table's backup includes:
    - Table metadata (schema, indexes, shard configuration)
    - All shard data (compressed with zstd)

    The backup creates a cluster-level manifest that tracks all included tables
    and their individual backup locations.

    **Storage Locations:**
    - Local filesystem: `file:///path/to/backup`
    - Amazon S3: `s3://bucket-name/path/to/backup`

    **Backup Structure:**
    ```
    {location}/
    ├── {backup_id}-cluster-metadata.json   (cluster manifest)
    ├── {table1}-{backup_id}-metadata.json  (table metadata)
    ├── shard-1-{table1}-{backup_id}.tar.zst
    ├── shard-2-{table1}-{backup_id}.tar.zst
    ├── {table2}-{backup_id}-metadata.json
    └── ...
    ```

    Args:
        body (ClusterBackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ClusterBackupResponse, Error]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: ClusterBackupRequest,
) -> Response[Union[ClusterBackupResponse, Error]]:
    """Backup all tables or selected tables

     Creates a backup of all tables or specified tables. Each table's backup includes:
    - Table metadata (schema, indexes, shard configuration)
    - All shard data (compressed with zstd)

    The backup creates a cluster-level manifest that tracks all included tables
    and their individual backup locations.

    **Storage Locations:**
    - Local filesystem: `file:///path/to/backup`
    - Amazon S3: `s3://bucket-name/path/to/backup`

    **Backup Structure:**
    ```
    {location}/
    ├── {backup_id}-cluster-metadata.json   (cluster manifest)
    ├── {table1}-{backup_id}-metadata.json  (table metadata)
    ├── shard-1-{table1}-{backup_id}.tar.zst
    ├── shard-2-{table1}-{backup_id}.tar.zst
    ├── {table2}-{backup_id}-metadata.json
    └── ...
    ```

    Args:
        body (ClusterBackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ClusterBackupResponse, Error]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: ClusterBackupRequest,
) -> Optional[Union[ClusterBackupResponse, Error]]:
    """Backup all tables or selected tables

     Creates a backup of all tables or specified tables. Each table's backup includes:
    - Table metadata (schema, indexes, shard configuration)
    - All shard data (compressed with zstd)

    The backup creates a cluster-level manifest that tracks all included tables
    and their individual backup locations.

    **Storage Locations:**
    - Local filesystem: `file:///path/to/backup`
    - Amazon S3: `s3://bucket-name/path/to/backup`

    **Backup Structure:**
    ```
    {location}/
    ├── {backup_id}-cluster-metadata.json   (cluster manifest)
    ├── {table1}-{backup_id}-metadata.json  (table metadata)
    ├── shard-1-{table1}-{backup_id}.tar.zst
    ├── shard-2-{table1}-{backup_id}.tar.zst
    ├── {table2}-{backup_id}-metadata.json
    └── ...
    ```

    Args:
        body (ClusterBackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ClusterBackupResponse, Error]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
