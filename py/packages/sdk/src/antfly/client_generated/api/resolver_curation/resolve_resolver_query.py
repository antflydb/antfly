from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.resolver_resolve_request import ResolverResolveRequest
from ...models.resolver_resolve_response import ResolverResolveResponse
from ...types import Response


def _get_kwargs(
    table_name: str,
    resolver_name: str,
    *,
    body: ResolverResolveRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/resolvers/{resolver_name}:resolve".format(
            table_name=quote(str(table_name), safe=""),
            resolver_name=quote(str(resolver_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | ResolverResolveResponse | None:
    if response.status_code == 200:
        response_200 = ResolverResolveResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 429:
        response_429 = cast(Any, None)
        return response_429

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | ResolverResolveResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    resolver_name: str,
    *,
    client: AuthenticatedClient,
    body: ResolverResolveRequest,
) -> Response[Any | Error | ResolverResolveResponse]:
    """resolve resolver query

     Runtime curation is owned by the source table and resolver identity, independent of graph index
    versions. Accepted changes replay through bounded resolution and promotion journals. Repeating a
    request is safe. Entity operations report each affected resolver; atomicity is per source table, and
    partial outcomes must be retried. Render and resolve are reads.

    Args:
        table_name (str):
        resolver_name (str):
        body (ResolverResolveRequest): Supply exactly one of text or extraction. Text uses this
            resolver's configured extractor. Resolution is ephemeral and uses the serving policy,
            candidate namespace and typed adjudicator without publishing artifacts or entities.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | ResolverResolveResponse]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        resolver_name=resolver_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    resolver_name: str,
    *,
    client: AuthenticatedClient,
    body: ResolverResolveRequest,
) -> Any | Error | ResolverResolveResponse | None:
    """resolve resolver query

     Runtime curation is owned by the source table and resolver identity, independent of graph index
    versions. Accepted changes replay through bounded resolution and promotion journals. Repeating a
    request is safe. Entity operations report each affected resolver; atomicity is per source table, and
    partial outcomes must be retried. Render and resolve are reads.

    Args:
        table_name (str):
        resolver_name (str):
        body (ResolverResolveRequest): Supply exactly one of text or extraction. Text uses this
            resolver's configured extractor. Resolution is ephemeral and uses the serving policy,
            candidate namespace and typed adjudicator without publishing artifacts or entities.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | ResolverResolveResponse
    """

    return sync_detailed(
        table_name=table_name,
        resolver_name=resolver_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    resolver_name: str,
    *,
    client: AuthenticatedClient,
    body: ResolverResolveRequest,
) -> Response[Any | Error | ResolverResolveResponse]:
    """resolve resolver query

     Runtime curation is owned by the source table and resolver identity, independent of graph index
    versions. Accepted changes replay through bounded resolution and promotion journals. Repeating a
    request is safe. Entity operations report each affected resolver; atomicity is per source table, and
    partial outcomes must be retried. Render and resolve are reads.

    Args:
        table_name (str):
        resolver_name (str):
        body (ResolverResolveRequest): Supply exactly one of text or extraction. Text uses this
            resolver's configured extractor. Resolution is ephemeral and uses the serving policy,
            candidate namespace and typed adjudicator without publishing artifacts or entities.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | ResolverResolveResponse]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        resolver_name=resolver_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    resolver_name: str,
    *,
    client: AuthenticatedClient,
    body: ResolverResolveRequest,
) -> Any | Error | ResolverResolveResponse | None:
    """resolve resolver query

     Runtime curation is owned by the source table and resolver identity, independent of graph index
    versions. Accepted changes replay through bounded resolution and promotion journals. Repeating a
    request is safe. Entity operations report each affected resolver; atomicity is per source table, and
    partial outcomes must be retried. Render and resolve are reads.

    Args:
        table_name (str):
        resolver_name (str):
        body (ResolverResolveRequest): Supply exactly one of text or extraction. Text uses this
            resolver's configured extractor. Resolution is ephemeral and uses the serving policy,
            candidate namespace and typed adjudicator without publishing artifacts or entities.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | ResolverResolveResponse
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            resolver_name=resolver_name,
            client=client,
            body=body,
        )
    ).parsed
