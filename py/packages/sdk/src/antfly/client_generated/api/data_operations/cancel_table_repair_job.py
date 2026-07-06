from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.table_repair_job import TableRepairJob
from ...types import Response


def _get_kwargs(
    table_name: str,
    job_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/repair/jobs/{job_id}/cancel".format(
            table_name=quote(str(table_name), safe=""),
            job_id=quote(str(job_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | TableRepairJob | None:
    if response.status_code == 200:
        response_200 = TableRepairJob.from_dict(response.json())

        return response_200

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
) -> Response[Error | TableRepairJob]:
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
) -> Response[Error | TableRepairJob]:
    """Cancel a table repair job

     Cancels a queued table repair job. If a repair pass is already running,
    the response returns the current running state; cancellation is applied
    only at pass boundaries so the API never reports a committed in-flight
    pass as cancelled.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TableRepairJob]
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
) -> Error | TableRepairJob | None:
    """Cancel a table repair job

     Cancels a queued table repair job. If a repair pass is already running,
    the response returns the current running state; cancellation is applied
    only at pass boundaries so the API never reports a committed in-flight
    pass as cancelled.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TableRepairJob
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
) -> Response[Error | TableRepairJob]:
    """Cancel a table repair job

     Cancels a queued table repair job. If a repair pass is already running,
    the response returns the current running state; cancellation is applied
    only at pass boundaries so the API never reports a committed in-flight
    pass as cancelled.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TableRepairJob]
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
) -> Error | TableRepairJob | None:
    """Cancel a table repair job

     Cancels a queued table repair job. If a repair pass is already running,
    the response returns the current running state; cancellation is applied
    only at pass boundaries so the API never reports a committed in-flight
    pass as cancelled.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TableRepairJob
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            job_id=job_id,
            client=client,
        )
    ).parsed
