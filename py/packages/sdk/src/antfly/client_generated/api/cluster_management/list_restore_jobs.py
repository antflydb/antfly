from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.list_restore_jobs_phase import ListRestoreJobsPhase
from ...models.list_restore_jobs_scope import ListRestoreJobsScope
from ...models.restore_job_list import RestoreJobList
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    limit: int | Unset = 50,
    cursor: str | Unset = UNSET,
    phase: ListRestoreJobsPhase | Unset = UNSET,
    scope: ListRestoreJobsScope | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["limit"] = limit

    params["cursor"] = cursor

    json_phase: str | Unset = UNSET
    if not isinstance(phase, Unset):
        json_phase = phase.value

    params["phase"] = json_phase

    json_scope: str | Unset = UNSET
    if not isinstance(scope, Unset):
        json_scope = scope.value

    params["scope"] = json_scope

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/restore/jobs",
        "params": params,
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | RestoreJobList | None:
    if response.status_code == 200:
        response_200 = RestoreJobList.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 503:
        response_503 = Error.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | RestoreJobList]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    limit: int | Unset = 50,
    cursor: str | Unset = UNSET,
    phase: ListRestoreJobsPhase | Unset = UNSET,
    scope: ListRestoreJobsScope | Unset = UNSET,
) -> Response[Error | RestoreJobList]:
    """List durable restore jobs

     Returns a newest-first, authorization-filtered page of retained restore jobs from the metadata
    leader.

    Args:
        limit (int | Unset):  Default: 50.
        cursor (str | Unset):
        phase (ListRestoreJobsPhase | Unset):
        scope (ListRestoreJobsScope | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RestoreJobList]
    """

    kwargs = _get_kwargs(
        limit=limit,
        cursor=cursor,
        phase=phase,
        scope=scope,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    limit: int | Unset = 50,
    cursor: str | Unset = UNSET,
    phase: ListRestoreJobsPhase | Unset = UNSET,
    scope: ListRestoreJobsScope | Unset = UNSET,
) -> Error | RestoreJobList | None:
    """List durable restore jobs

     Returns a newest-first, authorization-filtered page of retained restore jobs from the metadata
    leader.

    Args:
        limit (int | Unset):  Default: 50.
        cursor (str | Unset):
        phase (ListRestoreJobsPhase | Unset):
        scope (ListRestoreJobsScope | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RestoreJobList
    """

    return sync_detailed(
        client=client,
        limit=limit,
        cursor=cursor,
        phase=phase,
        scope=scope,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    limit: int | Unset = 50,
    cursor: str | Unset = UNSET,
    phase: ListRestoreJobsPhase | Unset = UNSET,
    scope: ListRestoreJobsScope | Unset = UNSET,
) -> Response[Error | RestoreJobList]:
    """List durable restore jobs

     Returns a newest-first, authorization-filtered page of retained restore jobs from the metadata
    leader.

    Args:
        limit (int | Unset):  Default: 50.
        cursor (str | Unset):
        phase (ListRestoreJobsPhase | Unset):
        scope (ListRestoreJobsScope | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RestoreJobList]
    """

    kwargs = _get_kwargs(
        limit=limit,
        cursor=cursor,
        phase=phase,
        scope=scope,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    limit: int | Unset = 50,
    cursor: str | Unset = UNSET,
    phase: ListRestoreJobsPhase | Unset = UNSET,
    scope: ListRestoreJobsScope | Unset = UNSET,
) -> Error | RestoreJobList | None:
    """List durable restore jobs

     Returns a newest-first, authorization-filtered page of retained restore jobs from the metadata
    leader.

    Args:
        limit (int | Unset):  Default: 50.
        cursor (str | Unset):
        phase (ListRestoreJobsPhase | Unset):
        scope (ListRestoreJobsScope | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RestoreJobList
    """

    return (
        await asyncio_detailed(
            client=client,
            limit=limit,
            cursor=cursor,
            phase=phase,
            scope=scope,
        )
    ).parsed
