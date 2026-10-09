from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.lake_catalog_commit_request import LakeCatalogCommitRequest
from ...models.lake_catalog_response import LakeCatalogResponse
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: LakeCatalogCommitRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/lake/commits".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | LakeCatalogResponse | None:
    if response.status_code == 200:
        response_200 = LakeCatalogResponse.from_dict(response.json())

        return response_200

    if response.status_code == 202:
        response_202 = LakeCatalogResponse.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if response.status_code == 504:
        response_504 = cast(Any, None)
        return response_504

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | LakeCatalogResponse]:
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
    body: LakeCatalogCommitRequest,
) -> Response[Any | LakeCatalogResponse]:
    """commitLakeCatalog

     Native Iceberg catalog operation for managed or external REST authority. Catalog mutations require
    table admin permission and iceberg_writer policy. These endpoints commit already prepared lake
    files; they do not perform ordinary row mutations or imply index publication.

    Args:
        table_name (str):
        body (LakeCatalogCommitRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | LakeCatalogResponse]
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
    body: LakeCatalogCommitRequest,
) -> Any | LakeCatalogResponse | None:
    """commitLakeCatalog

     Native Iceberg catalog operation for managed or external REST authority. Catalog mutations require
    table admin permission and iceberg_writer policy. These endpoints commit already prepared lake
    files; they do not perform ordinary row mutations or imply index publication.

    Args:
        table_name (str):
        body (LakeCatalogCommitRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | LakeCatalogResponse
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
    body: LakeCatalogCommitRequest,
) -> Response[Any | LakeCatalogResponse]:
    """commitLakeCatalog

     Native Iceberg catalog operation for managed or external REST authority. Catalog mutations require
    table admin permission and iceberg_writer policy. These endpoints commit already prepared lake
    files; they do not perform ordinary row mutations or imply index publication.

    Args:
        table_name (str):
        body (LakeCatalogCommitRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | LakeCatalogResponse]
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
    body: LakeCatalogCommitRequest,
) -> Any | LakeCatalogResponse | None:
    """commitLakeCatalog

     Native Iceberg catalog operation for managed or external REST authority. Catalog mutations require
    table admin permission and iceberg_writer policy. These endpoints commit already prepared lake
    files; they do not perform ordinary row mutations or imply index publication.

    Args:
        table_name (str):
        body (LakeCatalogCommitRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | LakeCatalogResponse
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
