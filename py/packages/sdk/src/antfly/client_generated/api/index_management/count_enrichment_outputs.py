from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.enrichment_counts import EnrichmentCounts
from ...models.enrichment_counts_request import EnrichmentCountsRequest
from ...models.error import Error
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    *,
    body: EnrichmentCountsRequest | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/enrichments/counts".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> EnrichmentCounts | Error | None:
    if response.status_code == 200:
        response_200 = EnrichmentCounts.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[EnrichmentCounts | Error]:
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
    body: EnrichmentCountsRequest | Unset = UNSET,
) -> Response[EnrichmentCounts | Error]:
    """Count enrichment outputs by origin

     Counts each enrichment's outputs by origin (seeded, generated,
    external, pending) over one page of documents in key order, for
    migration and readiness tracking (#1791). Page with `next`. Range
    owners count their own documents; counts are exact per page rather
    than maintained counters. Relational tables are not supported.

    Args:
        table_name (str):
        body (EnrichmentCountsRequest | Unset): One page of an enrichment output count walk.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EnrichmentCounts | Error]
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
    body: EnrichmentCountsRequest | Unset = UNSET,
) -> EnrichmentCounts | Error | None:
    """Count enrichment outputs by origin

     Counts each enrichment's outputs by origin (seeded, generated,
    external, pending) over one page of documents in key order, for
    migration and readiness tracking (#1791). Page with `next`. Range
    owners count their own documents; counts are exact per page rather
    than maintained counters. Relational tables are not supported.

    Args:
        table_name (str):
        body (EnrichmentCountsRequest | Unset): One page of an enrichment output count walk.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EnrichmentCounts | Error
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
    body: EnrichmentCountsRequest | Unset = UNSET,
) -> Response[EnrichmentCounts | Error]:
    """Count enrichment outputs by origin

     Counts each enrichment's outputs by origin (seeded, generated,
    external, pending) over one page of documents in key order, for
    migration and readiness tracking (#1791). Page with `next`. Range
    owners count their own documents; counts are exact per page rather
    than maintained counters. Relational tables are not supported.

    Args:
        table_name (str):
        body (EnrichmentCountsRequest | Unset): One page of an enrichment output count walk.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EnrichmentCounts | Error]
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
    body: EnrichmentCountsRequest | Unset = UNSET,
) -> EnrichmentCounts | Error | None:
    """Count enrichment outputs by origin

     Counts each enrichment's outputs by origin (seeded, generated,
    external, pending) over one page of documents in key order, for
    migration and readiness tracking (#1791). Page with `next`. Range
    owners count their own documents; counts are exact per page rather
    than maintained counters. Relational tables are not supported.

    Args:
        table_name (str):
        body (EnrichmentCountsRequest | Unset): One page of an enrichment output count walk.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EnrichmentCounts | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
