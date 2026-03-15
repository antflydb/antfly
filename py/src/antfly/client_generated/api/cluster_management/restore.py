from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.cluster_restore_request import ClusterRestoreRequest
from ...models.cluster_restore_response import ClusterRestoreResponse
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    *,
    body: ClusterRestoreRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/restore",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ClusterRestoreResponse, Error]]:
    if response.status_code == 202:
        response_202 = ClusterRestoreResponse.from_dict(response.json())

        return response_202

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
) -> Response[Union[ClusterRestoreResponse, Error]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: ClusterRestoreRequest,
) -> Response[Union[ClusterRestoreResponse, Error]]:
    """Restore multiple tables from a backup

     Restores tables from a cluster backup. Can restore all tables or a subset.

    **Restore Modes:**
    - `fail_if_exists`: Abort if any target table already exists (default)
    - `skip_if_exists`: Skip existing tables and restore the rest
    - `overwrite`: Drop existing tables and restore from backup

    The restore is asynchronous - this endpoint triggers the restore process
    and returns immediately. The actual data restoration happens via the
    reconciliation loop as shards are started.

    Args:
        body (ClusterRestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ClusterRestoreResponse, Error]]
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
    body: ClusterRestoreRequest,
) -> Optional[Union[ClusterRestoreResponse, Error]]:
    """Restore multiple tables from a backup

     Restores tables from a cluster backup. Can restore all tables or a subset.

    **Restore Modes:**
    - `fail_if_exists`: Abort if any target table already exists (default)
    - `skip_if_exists`: Skip existing tables and restore the rest
    - `overwrite`: Drop existing tables and restore from backup

    The restore is asynchronous - this endpoint triggers the restore process
    and returns immediately. The actual data restoration happens via the
    reconciliation loop as shards are started.

    Args:
        body (ClusterRestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ClusterRestoreResponse, Error]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: ClusterRestoreRequest,
) -> Response[Union[ClusterRestoreResponse, Error]]:
    """Restore multiple tables from a backup

     Restores tables from a cluster backup. Can restore all tables or a subset.

    **Restore Modes:**
    - `fail_if_exists`: Abort if any target table already exists (default)
    - `skip_if_exists`: Skip existing tables and restore the rest
    - `overwrite`: Drop existing tables and restore from backup

    The restore is asynchronous - this endpoint triggers the restore process
    and returns immediately. The actual data restoration happens via the
    reconciliation loop as shards are started.

    Args:
        body (ClusterRestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ClusterRestoreResponse, Error]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: ClusterRestoreRequest,
) -> Optional[Union[ClusterRestoreResponse, Error]]:
    """Restore multiple tables from a backup

     Restores tables from a cluster backup. Can restore all tables or a subset.

    **Restore Modes:**
    - `fail_if_exists`: Abort if any target table already exists (default)
    - `skip_if_exists`: Skip existing tables and restore the rest
    - `overwrite`: Drop existing tables and restore from backup

    The restore is asynchronous - this endpoint triggers the restore process
    and returns immediately. The actual data restoration happens via the
    reconciliation loop as shards are started.

    Args:
        body (ClusterRestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ClusterRestoreResponse, Error]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
