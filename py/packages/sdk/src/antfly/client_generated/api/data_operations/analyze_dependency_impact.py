from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.dependency_impact_request import DependencyImpactRequest
from ...models.dependency_impact_response import DependencyImpactResponse
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: DependencyImpactRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/dependencies/impact".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> DependencyImpactResponse | Error | None:
    if response.status_code == 200:
        response_200 = DependencyImpactResponse.from_dict(response.json())

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

    if response.status_code == 503:
        response_503 = Error.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[DependencyImpactResponse | Error]:
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
    body: DependencyImpactRequest,
) -> Response[DependencyImpactResponse | Error]:
    """Find generated artifacts that consumed a source document

     Reverse lineage for one source document key: retained dependent artifact
    versions confirmed against their lineage manifests, from every table
    whose enrichments read this table, and optionally their same-document
    downstream consumers. Rows from tables the caller may not read are
    omitted and make the response incomplete.

    Args:
        table_name (str):
        body (DependencyImpactRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DependencyImpactResponse | Error]
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
    body: DependencyImpactRequest,
) -> DependencyImpactResponse | Error | None:
    """Find generated artifacts that consumed a source document

     Reverse lineage for one source document key: retained dependent artifact
    versions confirmed against their lineage manifests, from every table
    whose enrichments read this table, and optionally their same-document
    downstream consumers. Rows from tables the caller may not read are
    omitted and make the response incomplete.

    Args:
        table_name (str):
        body (DependencyImpactRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DependencyImpactResponse | Error
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
    body: DependencyImpactRequest,
) -> Response[DependencyImpactResponse | Error]:
    """Find generated artifacts that consumed a source document

     Reverse lineage for one source document key: retained dependent artifact
    versions confirmed against their lineage manifests, from every table
    whose enrichments read this table, and optionally their same-document
    downstream consumers. Rows from tables the caller may not read are
    omitted and make the response incomplete.

    Args:
        table_name (str):
        body (DependencyImpactRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DependencyImpactResponse | Error]
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
    body: DependencyImpactRequest,
) -> DependencyImpactResponse | Error | None:
    """Find generated artifacts that consumed a source document

     Reverse lineage for one source document key: retained dependent artifact
    versions confirmed against their lineage manifests, from every table
    whose enrichments read this table, and optionally their same-document
    downstream consumers. Rows from tables the caller may not read are
    omitted and make the response incomplete.

    Args:
        table_name (str):
        body (DependencyImpactRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DependencyImpactResponse | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
