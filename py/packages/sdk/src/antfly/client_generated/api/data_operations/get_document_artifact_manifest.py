from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.document_artifact_manifest import DocumentArtifactManifest
from ...models.error import Error
from ...models.get_document_artifact_manifest_detail import GetDocumentArtifactManifestDetail
from ...models.get_document_artifact_manifest_response_413 import GetDocumentArtifactManifestResponse413
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    detail: GetDocumentArtifactManifestDetail | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_detail: str | Unset = UNSET
    if not isinstance(detail, Unset):
        json_detail = detail.value

    params["detail"] = json_detail

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/documents/{key}/artifacts/{artifact_name}".format(
            table_name=quote(str(table_name), safe=""),
            key=quote(str(key), safe=""),
            artifact_name=quote(str(artifact_name), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> DocumentArtifactManifest | Error | GetDocumentArtifactManifestResponse413 | None:
    if response.status_code == 200:
        response_200 = DocumentArtifactManifest.from_dict(response.json())

        return response_200

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 405:
        response_405 = Error.from_dict(response.json())

        return response_405

    if response.status_code == 413:
        response_413 = GetDocumentArtifactManifestResponse413.from_dict(response.json())

        return response_413

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
) -> Response[DocumentArtifactManifest | Error | GetDocumentArtifactManifestResponse413]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    detail: GetDocumentArtifactManifestDetail | Unset = UNSET,
) -> Response[DocumentArtifactManifest | Error | GetDocumentArtifactManifestResponse413]:
    """Inspect a derived document artifact manifest

     Returns manifest and processing state for a derived document artifact,
    such as the document-unit hierarchy extracted from a PDF, HTML page, or
    text field. The route is shard-aware; hosted deployments route the
    request to the data group that owns the source document key.

    On a privacy-protected table, a dense index name without a same-named
    asset also selects its stored text-chunk vectors for administrator
    inspection. The response uses route_type `dense_index_inspection` and
    manifest_version 1; raw manifest_json carries the versioned
    `antfly.dense-index-inspection.v1` snapshot. Its current flag, policy,
    index recipe, sanitized source and vector publication evidence belong
    to one publication-fenced storage cut. The top-level artifact_id is empty;
    member artifact IDs are included in the raw snapshot. This bounded
    inspection does not re-embed missing vectors and does not alter normal
    protected search projection permissions. See the table privacy guide
    for the raw payload and supported recipe limits.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):
        detail (GetDocumentArtifactManifestDetail | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DocumentArtifactManifest | Error | GetDocumentArtifactManifestResponse413]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        artifact_name=artifact_name,
        detail=detail,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    detail: GetDocumentArtifactManifestDetail | Unset = UNSET,
) -> DocumentArtifactManifest | Error | GetDocumentArtifactManifestResponse413 | None:
    """Inspect a derived document artifact manifest

     Returns manifest and processing state for a derived document artifact,
    such as the document-unit hierarchy extracted from a PDF, HTML page, or
    text field. The route is shard-aware; hosted deployments route the
    request to the data group that owns the source document key.

    On a privacy-protected table, a dense index name without a same-named
    asset also selects its stored text-chunk vectors for administrator
    inspection. The response uses route_type `dense_index_inspection` and
    manifest_version 1; raw manifest_json carries the versioned
    `antfly.dense-index-inspection.v1` snapshot. Its current flag, policy,
    index recipe, sanitized source and vector publication evidence belong
    to one publication-fenced storage cut. The top-level artifact_id is empty;
    member artifact IDs are included in the raw snapshot. This bounded
    inspection does not re-embed missing vectors and does not alter normal
    protected search projection permissions. See the table privacy guide
    for the raw payload and supported recipe limits.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):
        detail (GetDocumentArtifactManifestDetail | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DocumentArtifactManifest | Error | GetDocumentArtifactManifestResponse413
    """

    return sync_detailed(
        table_name=table_name,
        key=key,
        artifact_name=artifact_name,
        client=client,
        detail=detail,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    detail: GetDocumentArtifactManifestDetail | Unset = UNSET,
) -> Response[DocumentArtifactManifest | Error | GetDocumentArtifactManifestResponse413]:
    """Inspect a derived document artifact manifest

     Returns manifest and processing state for a derived document artifact,
    such as the document-unit hierarchy extracted from a PDF, HTML page, or
    text field. The route is shard-aware; hosted deployments route the
    request to the data group that owns the source document key.

    On a privacy-protected table, a dense index name without a same-named
    asset also selects its stored text-chunk vectors for administrator
    inspection. The response uses route_type `dense_index_inspection` and
    manifest_version 1; raw manifest_json carries the versioned
    `antfly.dense-index-inspection.v1` snapshot. Its current flag, policy,
    index recipe, sanitized source and vector publication evidence belong
    to one publication-fenced storage cut. The top-level artifact_id is empty;
    member artifact IDs are included in the raw snapshot. This bounded
    inspection does not re-embed missing vectors and does not alter normal
    protected search projection permissions. See the table privacy guide
    for the raw payload and supported recipe limits.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):
        detail (GetDocumentArtifactManifestDetail | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DocumentArtifactManifest | Error | GetDocumentArtifactManifestResponse413]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        artifact_name=artifact_name,
        detail=detail,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    detail: GetDocumentArtifactManifestDetail | Unset = UNSET,
) -> DocumentArtifactManifest | Error | GetDocumentArtifactManifestResponse413 | None:
    """Inspect a derived document artifact manifest

     Returns manifest and processing state for a derived document artifact,
    such as the document-unit hierarchy extracted from a PDF, HTML page, or
    text field. The route is shard-aware; hosted deployments route the
    request to the data group that owns the source document key.

    On a privacy-protected table, a dense index name without a same-named
    asset also selects its stored text-chunk vectors for administrator
    inspection. The response uses route_type `dense_index_inspection` and
    manifest_version 1; raw manifest_json carries the versioned
    `antfly.dense-index-inspection.v1` snapshot. Its current flag, policy,
    index recipe, sanitized source and vector publication evidence belong
    to one publication-fenced storage cut. The top-level artifact_id is empty;
    member artifact IDs are included in the raw snapshot. This bounded
    inspection does not re-embed missing vectors and does not alter normal
    protected search projection permissions. See the table privacy guide
    for the raw payload and supported recipe limits.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):
        detail (GetDocumentArtifactManifestDetail | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DocumentArtifactManifest | Error | GetDocumentArtifactManifestResponse413
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            key=key,
            artifact_name=artifact_name,
            client=client,
            detail=detail,
        )
    ).parsed
