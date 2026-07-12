from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.cluster_restore_request import ClusterRestoreRequest
from ...models.error import Error
from ...models.restore_job import RestoreJob
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: ClusterRestoreRequest,
    idempotency_key: str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(idempotency_key, Unset):
        headers["Idempotency-Key"] = idempotency_key

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/restore",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | RestoreJob | None:
    if response.status_code == 202:
        response_202 = RestoreJob.from_dict(response.json())

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


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Error | RestoreJob]:
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
    idempotency_key: str | Unset = UNSET,
) -> Response[Error | RestoreJob]:
    """Restore multiple tables from a backup

     Restores tables from a cluster backup. Can restore all tables or a subset.

    **Restore Modes:**
    - `fail_if_exists`: Abort if any target table already exists (default)
    - `skip_if_exists`: Skip existing tables and restore the rest
    - `overwrite`: Drop existing tables and restore from backup

    The restore is a durable asynchronous job. The request returns after the
    job record is persisted. Poll the restore job resource for progress.
    Interrupted running jobs are resumed after restart. Cancellation is
    cooperative between table and artifact publication boundaries.

    Args:
        idempotency_key (str | Unset):
        body (ClusterRestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RestoreJob]
    """

    kwargs = _get_kwargs(
        body=body,
        idempotency_key=idempotency_key,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    body: ClusterRestoreRequest,
    idempotency_key: str | Unset = UNSET,
) -> Error | RestoreJob | None:
    """Restore multiple tables from a backup

     Restores tables from a cluster backup. Can restore all tables or a subset.

    **Restore Modes:**
    - `fail_if_exists`: Abort if any target table already exists (default)
    - `skip_if_exists`: Skip existing tables and restore the rest
    - `overwrite`: Drop existing tables and restore from backup

    The restore is a durable asynchronous job. The request returns after the
    job record is persisted. Poll the restore job resource for progress.
    Interrupted running jobs are resumed after restart. Cancellation is
    cooperative between table and artifact publication boundaries.

    Args:
        idempotency_key (str | Unset):
        body (ClusterRestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RestoreJob
    """

    return sync_detailed(
        client=client,
        body=body,
        idempotency_key=idempotency_key,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: ClusterRestoreRequest,
    idempotency_key: str | Unset = UNSET,
) -> Response[Error | RestoreJob]:
    """Restore multiple tables from a backup

     Restores tables from a cluster backup. Can restore all tables or a subset.

    **Restore Modes:**
    - `fail_if_exists`: Abort if any target table already exists (default)
    - `skip_if_exists`: Skip existing tables and restore the rest
    - `overwrite`: Drop existing tables and restore from backup

    The restore is a durable asynchronous job. The request returns after the
    job record is persisted. Poll the restore job resource for progress.
    Interrupted running jobs are resumed after restart. Cancellation is
    cooperative between table and artifact publication boundaries.

    Args:
        idempotency_key (str | Unset):
        body (ClusterRestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RestoreJob]
    """

    kwargs = _get_kwargs(
        body=body,
        idempotency_key=idempotency_key,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: ClusterRestoreRequest,
    idempotency_key: str | Unset = UNSET,
) -> Error | RestoreJob | None:
    """Restore multiple tables from a backup

     Restores tables from a cluster backup. Can restore all tables or a subset.

    **Restore Modes:**
    - `fail_if_exists`: Abort if any target table already exists (default)
    - `skip_if_exists`: Skip existing tables and restore the rest
    - `overwrite`: Drop existing tables and restore from backup

    The restore is a durable asynchronous job. The request returns after the
    job record is persisted. Poll the restore job resource for progress.
    Interrupted running jobs are resumed after restart. Cancellation is
    cooperative between table and artifact publication boundaries.

    Args:
        idempotency_key (str | Unset):
        body (ClusterRestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RestoreJob
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            idempotency_key=idempotency_key,
        )
    ).parsed
