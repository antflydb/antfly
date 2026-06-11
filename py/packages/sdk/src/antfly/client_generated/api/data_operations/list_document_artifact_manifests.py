from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.document_artifact_manifest_list import DocumentArtifactManifestList
from ...models.error import Error
from ...models.list_document_artifact_manifests_detail import ListDocumentArtifactManifestsDetail
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    key: str,
    *,
    detail: ListDocumentArtifactManifestsDetail | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_detail: str | Unset = UNSET
    if not isinstance(detail, Unset):
        json_detail = detail.value

    params["detail"] = json_detail

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/documents/{key}/artifacts".format(
            table_name=quote(str(table_name), safe=""),
            key=quote(str(key), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> DocumentArtifactManifestList | Error | None:
    if response.status_code == 200:
        response_200 = DocumentArtifactManifestList.from_dict(response.json())

        return response_200

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 405:
        response_405 = Error.from_dict(response.json())

        return response_405

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
) -> Response[DocumentArtifactManifestList | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    detail: ListDocumentArtifactManifestsDetail | Unset = UNSET,
) -> Response[DocumentArtifactManifestList | Error]:
    """List derived document artifact manifests

     Returns the derived document artifact manifests currently available for
    a source document. This lets clients discover artifact names before
    inspecting a single manifest or triggering reprocessing.

    Args:
        table_name (str):
        key (str):
        detail (ListDocumentArtifactManifestsDetail | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DocumentArtifactManifestList | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        detail=detail,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    detail: ListDocumentArtifactManifestsDetail | Unset = UNSET,
) -> DocumentArtifactManifestList | Error | None:
    """List derived document artifact manifests

     Returns the derived document artifact manifests currently available for
    a source document. This lets clients discover artifact names before
    inspecting a single manifest or triggering reprocessing.

    Args:
        table_name (str):
        key (str):
        detail (ListDocumentArtifactManifestsDetail | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DocumentArtifactManifestList | Error
    """

    return sync_detailed(
        table_name=table_name,
        key=key,
        client=client,
        detail=detail,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    detail: ListDocumentArtifactManifestsDetail | Unset = UNSET,
) -> Response[DocumentArtifactManifestList | Error]:
    """List derived document artifact manifests

     Returns the derived document artifact manifests currently available for
    a source document. This lets clients discover artifact names before
    inspecting a single manifest or triggering reprocessing.

    Args:
        table_name (str):
        key (str):
        detail (ListDocumentArtifactManifestsDetail | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DocumentArtifactManifestList | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        detail=detail,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    detail: ListDocumentArtifactManifestsDetail | Unset = UNSET,
) -> DocumentArtifactManifestList | Error | None:
    """List derived document artifact manifests

     Returns the derived document artifact manifests currently available for
    a source document. This lets clients discover artifact names before
    inspecting a single manifest or triggering reprocessing.

    Args:
        table_name (str):
        key (str):
        detail (ListDocumentArtifactManifestsDetail | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DocumentArtifactManifestList | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            key=key,
            client=client,
            detail=detail,
        )
    ).parsed
