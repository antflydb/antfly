from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.cluster_backup_request import ClusterBackupRequest
from ...models.cluster_backup_response import ClusterBackupResponse
from ...models.error import Error
from ...models.metadata_capability_unavailable_error import MetadataCapabilityUnavailableError
from ...models.metadata_leader_unavailable_error import MetadataLeaderUnavailableError
from ...types import Response


def _get_kwargs(
    *,
    body: ClusterBackupRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/backup",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ClusterBackupResponse | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError | None:
    if response.status_code == 200:
        response_200 = ClusterBackupResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 503:

        def _parse_response_503(data: object) -> MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_backup_metadata_unavailable_error_type_0 = (
                    MetadataCapabilityUnavailableError.from_dict(data)
                )

                return componentsschemas_backup_metadata_unavailable_error_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_backup_metadata_unavailable_error_type_1 = MetadataLeaderUnavailableError.from_dict(data)

            return componentsschemas_backup_metadata_unavailable_error_type_1

        response_503 = _parse_response_503(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ClusterBackupResponse | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError]:
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
) -> Response[ClusterBackupResponse | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError]:
    """Backup all tables or selected tables

     Creates a backup of all tables or specified tables. Each table's backup includes:
    - Table metadata (schema, indexes, shard configuration)
    - All shard data (compressed with zstd)

    A non-empty backup publishes a cluster-level manifest only after every
    requested table backup is durable. The manifest is the final commit
    point and records complete expected/completed table counts. A `partial`
    or `failed` attempt returns per-table diagnostics but does not publish a
    restorable aggregate manifest. A cluster with no selected tables returns
    `400` without writing a backup artifact.

    Backup IDs are immutable. Reusing an ID that already has a published
    cluster manifest returns `409` and leaves the existing backup unchanged.

    **Storage Locations:**
    - Local filesystem: `file:///path/to/backup`
    - Amazon S3: `s3://bucket-name/path/to/backup`

    **Backup Structure:**
    ```
    {location}/
    ├── {backup_id}-cluster-metadata.json   (cluster manifest)
    └── generation-scoped table manifests and payloads
    ```

    Args:
        body (ClusterBackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ClusterBackupResponse | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError]
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
) -> ClusterBackupResponse | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError | None:
    """Backup all tables or selected tables

     Creates a backup of all tables or specified tables. Each table's backup includes:
    - Table metadata (schema, indexes, shard configuration)
    - All shard data (compressed with zstd)

    A non-empty backup publishes a cluster-level manifest only after every
    requested table backup is durable. The manifest is the final commit
    point and records complete expected/completed table counts. A `partial`
    or `failed` attempt returns per-table diagnostics but does not publish a
    restorable aggregate manifest. A cluster with no selected tables returns
    `400` without writing a backup artifact.

    Backup IDs are immutable. Reusing an ID that already has a published
    cluster manifest returns `409` and leaves the existing backup unchanged.

    **Storage Locations:**
    - Local filesystem: `file:///path/to/backup`
    - Amazon S3: `s3://bucket-name/path/to/backup`

    **Backup Structure:**
    ```
    {location}/
    ├── {backup_id}-cluster-metadata.json   (cluster manifest)
    └── generation-scoped table manifests and payloads
    ```

    Args:
        body (ClusterBackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ClusterBackupResponse | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: ClusterBackupRequest,
) -> Response[ClusterBackupResponse | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError]:
    """Backup all tables or selected tables

     Creates a backup of all tables or specified tables. Each table's backup includes:
    - Table metadata (schema, indexes, shard configuration)
    - All shard data (compressed with zstd)

    A non-empty backup publishes a cluster-level manifest only after every
    requested table backup is durable. The manifest is the final commit
    point and records complete expected/completed table counts. A `partial`
    or `failed` attempt returns per-table diagnostics but does not publish a
    restorable aggregate manifest. A cluster with no selected tables returns
    `400` without writing a backup artifact.

    Backup IDs are immutable. Reusing an ID that already has a published
    cluster manifest returns `409` and leaves the existing backup unchanged.

    **Storage Locations:**
    - Local filesystem: `file:///path/to/backup`
    - Amazon S3: `s3://bucket-name/path/to/backup`

    **Backup Structure:**
    ```
    {location}/
    ├── {backup_id}-cluster-metadata.json   (cluster manifest)
    └── generation-scoped table manifests and payloads
    ```

    Args:
        body (ClusterBackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ClusterBackupResponse | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError]
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
) -> ClusterBackupResponse | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError | None:
    """Backup all tables or selected tables

     Creates a backup of all tables or specified tables. Each table's backup includes:
    - Table metadata (schema, indexes, shard configuration)
    - All shard data (compressed with zstd)

    A non-empty backup publishes a cluster-level manifest only after every
    requested table backup is durable. The manifest is the final commit
    point and records complete expected/completed table counts. A `partial`
    or `failed` attempt returns per-table diagnostics but does not publish a
    restorable aggregate manifest. A cluster with no selected tables returns
    `400` without writing a backup artifact.

    Backup IDs are immutable. Reusing an ID that already has a published
    cluster manifest returns `409` and leaves the existing backup unchanged.

    **Storage Locations:**
    - Local filesystem: `file:///path/to/backup`
    - Amazon S3: `s3://bucket-name/path/to/backup`

    **Backup Structure:**
    ```
    {location}/
    ├── {backup_id}-cluster-metadata.json   (cluster manifest)
    └── generation-scoped table manifests and payloads
    ```

    Args:
        body (ClusterBackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ClusterBackupResponse | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
