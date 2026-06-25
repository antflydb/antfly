from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.table_artifact_enrichment_list import TableArtifactEnrichmentList
from ...types import Response


def _get_kwargs(
    table_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/artifacts".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | TableArtifactEnrichmentList | None:
    if response.status_code == 200:
        response_200 = TableArtifactEnrichmentList.from_dict(response.json())

        return response_200

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
) -> Response[Error | TableArtifactEnrichmentList]:
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
) -> Response[Error | TableArtifactEnrichmentList]:
    """List table artifact enrichments

     Returns table-level generated artifact enrichments configured for a
    table, including enrichments declared inline by indexes. This is the
    artifact control-plane view; use the document artifact routes to inspect
    materialized manifests for a specific source document.

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TableArtifactEnrichmentList]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> Error | TableArtifactEnrichmentList | None:
    """List table artifact enrichments

     Returns table-level generated artifact enrichments configured for a
    table, including enrichments declared inline by indexes. This is the
    artifact control-plane view; use the document artifact routes to inspect
    materialized manifests for a specific source document.

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TableArtifactEnrichmentList
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | TableArtifactEnrichmentList]:
    """List table artifact enrichments

     Returns table-level generated artifact enrichments configured for a
    table, including enrichments declared inline by indexes. This is the
    artifact control-plane view; use the document artifact routes to inspect
    materialized manifests for a specific source document.

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TableArtifactEnrichmentList]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> Error | TableArtifactEnrichmentList | None:
    """List table artifact enrichments

     Returns table-level generated artifact enrichments configured for a
    table, including enrichments declared inline by indexes. This is the
    artifact control-plane view; use the document artifact routes to inspect
    materialized manifests for a specific source document.

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TableArtifactEnrichmentList
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
        )
    ).parsed
