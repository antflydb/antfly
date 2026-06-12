from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.document_artifact_reprocess_job import DocumentArtifactReprocessJob
from ...models.document_artifact_reprocess_job_start_request import DocumentArtifactReprocessJobStartRequest
from ...models.error import Error
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    artifact_name: str,
    *,
    body: DocumentArtifactReprocessJobStartRequest | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/artifacts/{artifact_name}/reprocess-jobs".format(
            table_name=quote(str(table_name), safe=""),
            artifact_name=quote(str(artifact_name), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> DocumentArtifactReprocessJob | Error | None:
    if response.status_code == 202:
        response_202 = DocumentArtifactReprocessJob.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

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
) -> Response[DocumentArtifactReprocessJob | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    body: DocumentArtifactReprocessJobStartRequest | Unset = UNSET,
) -> Response[DocumentArtifactReprocessJob | Error]:
    """Create a derived document artifact reprocess job

     Creates a durable user-facing repair job for a derived document
    artifact. The job advances through the same bounded per-shard repair
    primitive used by `/reprocess`, and stores returned continuation
    cursors so hosted controllers can resume large sharded table repairs
    without collapsing progress into a single global key cursor.

    Args:
        table_name (str):
        artifact_name (str):
        body (DocumentArtifactReprocessJobStartRequest | Unset): Request to create a durable table
            artifact reprocess job.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DocumentArtifactReprocessJob | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        artifact_name=artifact_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    body: DocumentArtifactReprocessJobStartRequest | Unset = UNSET,
) -> DocumentArtifactReprocessJob | Error | None:
    """Create a derived document artifact reprocess job

     Creates a durable user-facing repair job for a derived document
    artifact. The job advances through the same bounded per-shard repair
    primitive used by `/reprocess`, and stores returned continuation
    cursors so hosted controllers can resume large sharded table repairs
    without collapsing progress into a single global key cursor.

    Args:
        table_name (str):
        artifact_name (str):
        body (DocumentArtifactReprocessJobStartRequest | Unset): Request to create a durable table
            artifact reprocess job.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DocumentArtifactReprocessJob | Error
    """

    return sync_detailed(
        table_name=table_name,
        artifact_name=artifact_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    body: DocumentArtifactReprocessJobStartRequest | Unset = UNSET,
) -> Response[DocumentArtifactReprocessJob | Error]:
    """Create a derived document artifact reprocess job

     Creates a durable user-facing repair job for a derived document
    artifact. The job advances through the same bounded per-shard repair
    primitive used by `/reprocess`, and stores returned continuation
    cursors so hosted controllers can resume large sharded table repairs
    without collapsing progress into a single global key cursor.

    Args:
        table_name (str):
        artifact_name (str):
        body (DocumentArtifactReprocessJobStartRequest | Unset): Request to create a durable table
            artifact reprocess job.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DocumentArtifactReprocessJob | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        artifact_name=artifact_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    body: DocumentArtifactReprocessJobStartRequest | Unset = UNSET,
) -> DocumentArtifactReprocessJob | Error | None:
    """Create a derived document artifact reprocess job

     Creates a durable user-facing repair job for a derived document
    artifact. The job advances through the same bounded per-shard repair
    primitive used by `/reprocess`, and stores returned continuation
    cursors so hosted controllers can resume large sharded table repairs
    without collapsing progress into a single global key cursor.

    Args:
        table_name (str):
        artifact_name (str):
        body (DocumentArtifactReprocessJobStartRequest | Unset): Request to create a durable table
            artifact reprocess job.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DocumentArtifactReprocessJob | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            artifact_name=artifact_name,
            client=client,
            body=body,
        )
    ).parsed
