from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.document_artifact_reprocess_response import DocumentArtifactReprocessResponse
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    table_name: str,
    key: str,
    artifact_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/documents/{key}/artifacts/{artifact_name}/reprocess".format(
            table_name=quote(str(table_name), safe=""),
            key=quote(str(key), safe=""),
            artifact_name=quote(str(artifact_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> DocumentArtifactReprocessResponse | Error | None:
    if response.status_code == 202:
        response_202 = DocumentArtifactReprocessResponse.from_dict(response.json())

        return response_202

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
) -> Response[DocumentArtifactReprocessResponse | Error]:
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
) -> Response[DocumentArtifactReprocessResponse | Error]:
    """Reprocess a derived asset

     Invalidates the current artifact state and requests the producer to
    rebuild the derived asset for the source document. Copy, generator,
    reader, transcriber, extractor, and document-extraction producers are
    all supported.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DocumentArtifactReprocessResponse | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        artifact_name=artifact_name,
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
) -> DocumentArtifactReprocessResponse | Error | None:
    """Reprocess a derived asset

     Invalidates the current artifact state and requests the producer to
    rebuild the derived asset for the source document. Copy, generator,
    reader, transcriber, extractor, and document-extraction producers are
    all supported.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DocumentArtifactReprocessResponse | Error
    """

    return sync_detailed(
        table_name=table_name,
        key=key,
        artifact_name=artifact_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[DocumentArtifactReprocessResponse | Error]:
    """Reprocess a derived asset

     Invalidates the current artifact state and requests the producer to
    rebuild the derived asset for the source document. Copy, generator,
    reader, transcriber, extractor, and document-extraction producers are
    all supported.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DocumentArtifactReprocessResponse | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        artifact_name=artifact_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
) -> DocumentArtifactReprocessResponse | Error | None:
    """Reprocess a derived asset

     Invalidates the current artifact state and requests the producer to
    rebuild the derived asset for the source document. Copy, generator,
    reader, transcriber, extractor, and document-extraction producers are
    all supported.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DocumentArtifactReprocessResponse | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            key=key,
            artifact_name=artifact_name,
            client=client,
        )
    ).parsed
