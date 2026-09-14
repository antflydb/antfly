from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.table_repair_control_job_start_request import TableRepairControlJobStartRequest
from ...models.table_repair_job import TableRepairJob
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: TableRepairControlJobStartRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/repair/control-jobs".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | TableRepairJob | None:
    if response.status_code == 200:
        response_200 = TableRepairJob.from_dict(response.json())

        return response_200

    if response.status_code == 202:
        response_202 = TableRepairJob.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 405:
        response_405 = Error.from_dict(response.json())

        return response_405

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
    *,
    client: AuthenticatedClient,
    body: TableRepairControlJobStartRequest,
) -> Response[Error | TableRepairJob]:
    """Start a durable index control job

     Applies pause, resume, or attempt cancellation across all table groups.
    Traversal is durable and server-owned. The resulting repair job can be
    inspected or cancelled with the regular repair job endpoints. Cancelling
    a control job stops remaining passes without undoing applied controls.
    This distinct endpoint prevents older servers from interpreting a control
    request as ordinary repair work.

    Args:
        table_name (str):
        body (TableRepairControlJobStartRequest): Starts a durable named-index control traversal.
            The server advances every bounded pass, including after restart. Use the repair job status
            and cancellation endpoints to inspect or stop the traversal.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TableRepairJob]
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
    body: TableRepairControlJobStartRequest,
) -> Error | TableRepairJob | None:
    """Start a durable index control job

     Applies pause, resume, or attempt cancellation across all table groups.
    Traversal is durable and server-owned. The resulting repair job can be
    inspected or cancelled with the regular repair job endpoints. Cancelling
    a control job stops remaining passes without undoing applied controls.
    This distinct endpoint prevents older servers from interpreting a control
    request as ordinary repair work.

    Args:
        table_name (str):
        body (TableRepairControlJobStartRequest): Starts a durable named-index control traversal.
            The server advances every bounded pass, including after restart. Use the repair job status
            and cancellation endpoints to inspect or stop the traversal.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TableRepairJob
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
    body: TableRepairControlJobStartRequest,
) -> Response[Error | TableRepairJob]:
    """Start a durable index control job

     Applies pause, resume, or attempt cancellation across all table groups.
    Traversal is durable and server-owned. The resulting repair job can be
    inspected or cancelled with the regular repair job endpoints. Cancelling
    a control job stops remaining passes without undoing applied controls.
    This distinct endpoint prevents older servers from interpreting a control
    request as ordinary repair work.

    Args:
        table_name (str):
        body (TableRepairControlJobStartRequest): Starts a durable named-index control traversal.
            The server advances every bounded pass, including after restart. Use the repair job status
            and cancellation endpoints to inspect or stop the traversal.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TableRepairJob]
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
    body: TableRepairControlJobStartRequest,
) -> Error | TableRepairJob | None:
    """Start a durable index control job

     Applies pause, resume, or attempt cancellation across all table groups.
    Traversal is durable and server-owned. The resulting repair job can be
    inspected or cancelled with the regular repair job endpoints. Cancelling
    a control job stops remaining passes without undoing applied controls.
    This distinct endpoint prevents older servers from interpreting a control
    request as ordinary repair work.

    Args:
        table_name (str):
        body (TableRepairControlJobStartRequest): Starts a durable named-index control traversal.
            The server advances every bounded pass, including after restart. Use the repair job status
            and cancellation endpoints to inspect or stop the traversal.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TableRepairJob
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
