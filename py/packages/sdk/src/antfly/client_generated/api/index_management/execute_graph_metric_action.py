from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.execute_graph_metric_action_action import ExecuteGraphMetricActionAction
from ...models.graph_metric_action_response import GraphMetricActionResponse
from ...types import Response


def _get_kwargs(
    table_name: str,
    index_name: str,
    metric_name: str,
    action: ExecuteGraphMetricActionAction,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/indexes/{index_name}/graph-metrics/{metric_name}:{action}".format(
            table_name=quote(str(table_name), safe=""),
            index_name=quote(str(index_name), safe=""),
            metric_name=quote(str(metric_name), safe=""),
            action=quote(str(action), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | GraphMetricActionResponse | str | None:
    if response.status_code == 200:
        response_200 = GraphMetricActionResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 405:
        response_405 = Error.from_dict(response.json())

        return response_405

    if response.status_code == 409:
        response_409 = response.text
        return response_409

    if response.status_code == 429:
        response_429 = Error.from_dict(response.json())

        return response_429

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | GraphMetricActionResponse | str]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    index_name: str,
    metric_name: str,
    action: ExecuteGraphMetricActionAction,
    *,
    client: AuthenticatedClient,
) -> Response[Error | GraphMetricActionResponse | str]:
    """Execute a graph metric operational action

     Refresh, rebuild, delete, pause, or resume maintenance for a configured
    graph metric. The metric configuration remains owned by the graph index.
    Refresh and rebuild durably enqueue bounded, resumable maintenance and
    return the aggregate shard status without waiting for graph-sized work.
    `delete` clears materialized metric state and durably disables automatic
    maintenance. A later refresh, rebuild, or resume action re-enables the
    metric and can publish a new generation.

    Args:
        table_name (str):
        index_name (str):
        metric_name (str):
        action (ExecuteGraphMetricActionAction):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GraphMetricActionResponse | str]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        index_name=index_name,
        metric_name=metric_name,
        action=action,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    index_name: str,
    metric_name: str,
    action: ExecuteGraphMetricActionAction,
    *,
    client: AuthenticatedClient,
) -> Error | GraphMetricActionResponse | str | None:
    """Execute a graph metric operational action

     Refresh, rebuild, delete, pause, or resume maintenance for a configured
    graph metric. The metric configuration remains owned by the graph index.
    Refresh and rebuild durably enqueue bounded, resumable maintenance and
    return the aggregate shard status without waiting for graph-sized work.
    `delete` clears materialized metric state and durably disables automatic
    maintenance. A later refresh, rebuild, or resume action re-enables the
    metric and can publish a new generation.

    Args:
        table_name (str):
        index_name (str):
        metric_name (str):
        action (ExecuteGraphMetricActionAction):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GraphMetricActionResponse | str
    """

    return sync_detailed(
        table_name=table_name,
        index_name=index_name,
        metric_name=metric_name,
        action=action,
        client=client,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    index_name: str,
    metric_name: str,
    action: ExecuteGraphMetricActionAction,
    *,
    client: AuthenticatedClient,
) -> Response[Error | GraphMetricActionResponse | str]:
    """Execute a graph metric operational action

     Refresh, rebuild, delete, pause, or resume maintenance for a configured
    graph metric. The metric configuration remains owned by the graph index.
    Refresh and rebuild durably enqueue bounded, resumable maintenance and
    return the aggregate shard status without waiting for graph-sized work.
    `delete` clears materialized metric state and durably disables automatic
    maintenance. A later refresh, rebuild, or resume action re-enables the
    metric and can publish a new generation.

    Args:
        table_name (str):
        index_name (str):
        metric_name (str):
        action (ExecuteGraphMetricActionAction):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GraphMetricActionResponse | str]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        index_name=index_name,
        metric_name=metric_name,
        action=action,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    index_name: str,
    metric_name: str,
    action: ExecuteGraphMetricActionAction,
    *,
    client: AuthenticatedClient,
) -> Error | GraphMetricActionResponse | str | None:
    """Execute a graph metric operational action

     Refresh, rebuild, delete, pause, or resume maintenance for a configured
    graph metric. The metric configuration remains owned by the graph index.
    Refresh and rebuild durably enqueue bounded, resumable maintenance and
    return the aggregate shard status without waiting for graph-sized work.
    `delete` clears materialized metric state and durably disables automatic
    maintenance. A later refresh, rebuild, or resume action re-enables the
    metric and can publish a new generation.

    Args:
        table_name (str):
        index_name (str):
        metric_name (str):
        action (ExecuteGraphMetricActionAction):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GraphMetricActionResponse | str
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            index_name=index_name,
            metric_name=metric_name,
            action=action,
            client=client,
        )
    ).parsed
