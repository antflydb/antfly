from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.catalog_mutation_visibility_pending import CatalogMutationVisibilityPending
from ...models.error import Error
from ...models.graph_catalog_entry import GraphCatalogEntry
from ...models.graph_definition import GraphDefinition
from ...types import Response


def _get_kwargs(
    graph_name: str,
    *,
    body: GraphDefinition,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/graphs/{graph_name}".format(
            graph_name=quote(str(graph_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | CatalogMutationVisibilityPending | Error | GraphCatalogEntry | None:
    if response.status_code == 201:
        response_201 = GraphCatalogEntry.from_dict(response.json())

        return response_201

    if response.status_code == 202:
        response_202 = CatalogMutationVisibilityPending.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | CatalogMutationVisibilityPending | Error | GraphCatalogEntry]:
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
    body: GraphDefinition,
) -> Response[Any | CatalogMutationVisibilityPending | Error | GraphCatalogEntry]:
    """Create graph

     Creates a graph mapping over existing tables and graph indexes. Reads require permission on the
    graph and every input table. Definition updates use conditional revisions; existing cursors retain
    their captured definition.

    Args:
        graph_name (str):
        body (GraphDefinition):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | CatalogMutationVisibilityPending | Error | GraphCatalogEntry]
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
    body: GraphDefinition,
) -> Any | CatalogMutationVisibilityPending | Error | GraphCatalogEntry | None:
    """Create graph

     Creates a graph mapping over existing tables and graph indexes. Reads require permission on the
    graph and every input table. Definition updates use conditional revisions; existing cursors retain
    their captured definition.

    Args:
        graph_name (str):
        body (GraphDefinition):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | CatalogMutationVisibilityPending | Error | GraphCatalogEntry
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
    body: GraphDefinition,
) -> Response[Any | CatalogMutationVisibilityPending | Error | GraphCatalogEntry]:
    """Create graph

     Creates a graph mapping over existing tables and graph indexes. Reads require permission on the
    graph and every input table. Definition updates use conditional revisions; existing cursors retain
    their captured definition.

    Args:
        graph_name (str):
        body (GraphDefinition):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | CatalogMutationVisibilityPending | Error | GraphCatalogEntry]
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
    body: GraphDefinition,
) -> Any | CatalogMutationVisibilityPending | Error | GraphCatalogEntry | None:
    """Create graph

     Creates a graph mapping over existing tables and graph indexes. Reads require permission on the
    graph and every input table. Definition updates use conditional revisions; existing cursors retain
    their captured definition.

    Args:
        graph_name (str):
        body (GraphDefinition):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | CatalogMutationVisibilityPending | Error | GraphCatalogEntry
    """

    return (
        await asyncio_detailed(
            graph_name=graph_name,
            client=client,
            body=body,
        )
    ).parsed
