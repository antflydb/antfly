from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.graph_query_request import GraphQueryRequest
from ...models.graph_query_result import GraphQueryResult
from ...types import Response


def _get_kwargs(
    graph_name: str,
    *,
    body: GraphQueryRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/graphs/{graph_name}/query".format(
            graph_name=quote(str(graph_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | GraphQueryResult | None:
    if response.status_code == 200:
        response_200 = GraphQueryResult.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 429:
        response_429 = cast(Any, None)
        return response_429

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | GraphQueryResult]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    graph_name: str,
    *,
    client: AuthenticatedClient,
    body: GraphQueryRequest,
) -> Response[Any | Error | GraphQueryResult]:
    """Execute Cypher against a named graph

     Executes an authenticated statement under the graph definition and each underlying table's
    permissions and row policies. Resource budgets fail explicitly. EXPLAIN binds without reading rows.
    Results use the lossless version 1 graph wire format.

    Args:
        graph_name (str):
        body (GraphQueryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | GraphQueryResult]
    """

    kwargs = _get_kwargs(
        graph_name=graph_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    graph_name: str,
    *,
    client: AuthenticatedClient,
    body: GraphQueryRequest,
) -> Any | Error | GraphQueryResult | None:
    """Execute Cypher against a named graph

     Executes an authenticated statement under the graph definition and each underlying table's
    permissions and row policies. Resource budgets fail explicitly. EXPLAIN binds without reading rows.
    Results use the lossless version 1 graph wire format.

    Args:
        graph_name (str):
        body (GraphQueryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | GraphQueryResult
    """

    return sync_detailed(
        graph_name=graph_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    graph_name: str,
    *,
    client: AuthenticatedClient,
    body: GraphQueryRequest,
) -> Response[Any | Error | GraphQueryResult]:
    """Execute Cypher against a named graph

     Executes an authenticated statement under the graph definition and each underlying table's
    permissions and row policies. Resource budgets fail explicitly. EXPLAIN binds without reading rows.
    Results use the lossless version 1 graph wire format.

    Args:
        graph_name (str):
        body (GraphQueryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | GraphQueryResult]
    """

    kwargs = _get_kwargs(
        graph_name=graph_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    graph_name: str,
    *,
    client: AuthenticatedClient,
    body: GraphQueryRequest,
) -> Any | Error | GraphQueryResult | None:
    """Execute Cypher against a named graph

     Executes an authenticated statement under the graph definition and each underlying table's
    permissions and row policies. Resource budgets fail explicitly. EXPLAIN binds without reading rows.
    Results use the lossless version 1 graph wire format.

    Args:
        graph_name (str):
        body (GraphQueryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | GraphQueryResult
    """

    return (
        await asyncio_detailed(
            graph_name=graph_name,
            client=client,
            body=body,
        )
    ).parsed
