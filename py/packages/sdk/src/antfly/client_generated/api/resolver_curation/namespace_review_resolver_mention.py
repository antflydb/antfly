from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.resolver_mutation_result import ResolverMutationResult
from ...models.resolver_review_decision import ResolverReviewDecision
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    resolver_name: str,
    review_id: str,
    *,
    body: ResolverReviewDecision,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/resolvers/{resolver_name}/reviews/{review_id}".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            table_name=quote(str(table_name), safe=""),
            resolver_name=quote(str(resolver_name), safe=""),
            review_id=quote(str(review_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | ResolverMutationResult | None:
    if response.status_code == 200:
        response_200 = ResolverMutationResult.from_dict(response.json())

        return response_200

    if response.status_code == 202:
        response_202 = ResolverMutationResult.from_dict(response.json())

        return response_202

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
) -> Response[Any | Error | ResolverMutationResult]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    resolver_name: str,
    review_id: str,
    *,
    client: AuthenticatedClient,
    body: ResolverReviewDecision,
) -> Response[Any | Error | ResolverMutationResult]:
    """review resolver mention

     Runtime curation is owned by the source table and resolver identity, independent of graph index
    versions. Accepted changes replay through bounded resolution and promotion journals. Repeating a
    request is safe. Entity operations report each affected resolver; atomicity is per source table, and
    partial outcomes must be retried. Render and resolve are reads.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        resolver_name (str):
        review_id (str):
        body (ResolverReviewDecision):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | ResolverMutationResult]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        resolver_name=resolver_name,
        review_id=review_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    table_name: str,
    resolver_name: str,
    review_id: str,
    *,
    client: AuthenticatedClient,
    body: ResolverReviewDecision,
) -> Any | Error | ResolverMutationResult | None:
    """review resolver mention

     Runtime curation is owned by the source table and resolver identity, independent of graph index
    versions. Accepted changes replay through bounded resolution and promotion journals. Repeating a
    request is safe. Entity operations report each affected resolver; atomicity is per source table, and
    partial outcomes must be retried. Render and resolve are reads.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        resolver_name (str):
        review_id (str):
        body (ResolverReviewDecision):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | ResolverMutationResult
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        resolver_name=resolver_name,
        review_id=review_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    resolver_name: str,
    review_id: str,
    *,
    client: AuthenticatedClient,
    body: ResolverReviewDecision,
) -> Response[Any | Error | ResolverMutationResult]:
    """review resolver mention

     Runtime curation is owned by the source table and resolver identity, independent of graph index
    versions. Accepted changes replay through bounded resolution and promotion journals. Repeating a
    request is safe. Entity operations report each affected resolver; atomicity is per source table, and
    partial outcomes must be retried. Render and resolve are reads.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        resolver_name (str):
        review_id (str):
        body (ResolverReviewDecision):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | ResolverMutationResult]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        resolver_name=resolver_name,
        review_id=review_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    table_name: str,
    resolver_name: str,
    review_id: str,
    *,
    client: AuthenticatedClient,
    body: ResolverReviewDecision,
) -> Any | Error | ResolverMutationResult | None:
    """review resolver mention

     Runtime curation is owned by the source table and resolver identity, independent of graph index
    versions. Accepted changes replay through bounded resolution and promotion journals. Repeating a
    request is safe. Entity operations report each affected resolver; atomicity is per source table, and
    partial outcomes must be retried. Render and resolve are reads.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        resolver_name (str):
        review_id (str):
        body (ResolverReviewDecision):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | ResolverMutationResult
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            resolver_name=resolver_name,
            review_id=review_id,
            client=client,
            body=body,
        )
    ).parsed
