from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.backup_request import BackupRequest
from ...models.backup_table_response_201 import BackupTableResponse201
from ...models.error import Error
from ...models.metadata_capability_unavailable_error import MetadataCapabilityUnavailableError
from ...models.metadata_leader_unavailable_error import MetadataLeaderUnavailableError
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: BackupRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/backup".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> BackupTableResponse201 | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError | None:
    if response.status_code == 201:
        response_201 = BackupTableResponse201.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

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
) -> Response[BackupTableResponse201 | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError]:
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
    body: BackupRequest,
) -> Response[BackupTableResponse201 | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError]:
    """Backup a table

     Backup IDs are immutable. Reusing an already published ID returns `409` without changing the
    existing backup.

    Args:
        table_name (str):
        body (BackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[BackupTableResponse201 | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError]
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
    body: BackupRequest,
) -> BackupTableResponse201 | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError | None:
    """Backup a table

     Backup IDs are immutable. Reusing an already published ID returns `409` without changing the
    existing backup.

    Args:
        table_name (str):
        body (BackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        BackupTableResponse201 | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError
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
    body: BackupRequest,
) -> Response[BackupTableResponse201 | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError]:
    """Backup a table

     Backup IDs are immutable. Reusing an already published ID returns `409` without changing the
    existing backup.

    Args:
        table_name (str):
        body (BackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[BackupTableResponse201 | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError]
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
    body: BackupRequest,
) -> BackupTableResponse201 | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError | None:
    """Backup a table

     Backup IDs are immutable. Reusing an already published ID returns `409` without changing the
    existing backup.

    Args:
        table_name (str):
        body (BackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        BackupTableResponse201 | Error | MetadataCapabilityUnavailableError | MetadataLeaderUnavailableError
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
