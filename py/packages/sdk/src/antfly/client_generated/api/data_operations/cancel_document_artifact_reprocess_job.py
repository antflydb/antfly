from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.document_artifact_reprocess_job import DocumentArtifactReprocessJob
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    table_name: str,
    artifact_name: str,
    job_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/artifacts/{artifact_name}/reprocess-jobs/{job_id}/cancel".format(
            table_name=quote(str(table_name), safe=""),
            artifact_name=quote(str(artifact_name), safe=""),
            job_id=quote(str(job_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> DocumentArtifactReprocessJob | Error | None:
    if response.status_code == 200:
        response_200 = DocumentArtifactReprocessJob.from_dict(response.json())

        return response_200

    if response.status_code == 202:
        response_202 = DocumentArtifactReprocessJob.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

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
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[DocumentArtifactReprocessJob | Error]:
    """Cancel a derived document artifact reprocess job

     Cancels a queued document artifact reprocess job. If a reprocess pass is
    already running, the response returns the current running state;
    cancellation is applied only at pass boundaries so the API never reports
    a committed in-flight pass as cancelled.

    Args:
        table_name (str):
        artifact_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DocumentArtifactReprocessJob | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        artifact_name=artifact_name,
        job_id=job_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    artifact_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> DocumentArtifactReprocessJob | Error | None:
    """Cancel a derived document artifact reprocess job

     Cancels a queued document artifact reprocess job. If a reprocess pass is
    already running, the response returns the current running state;
    cancellation is applied only at pass boundaries so the API never reports
    a committed in-flight pass as cancelled.

    Args:
        table_name (str):
        artifact_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DocumentArtifactReprocessJob | Error
    """

    return sync_detailed(
        table_name=table_name,
        artifact_name=artifact_name,
        job_id=job_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    artifact_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[DocumentArtifactReprocessJob | Error]:
    """Cancel a derived document artifact reprocess job

     Cancels a queued document artifact reprocess job. If a reprocess pass is
    already running, the response returns the current running state;
    cancellation is applied only at pass boundaries so the API never reports
    a committed in-flight pass as cancelled.

    Args:
        table_name (str):
        artifact_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DocumentArtifactReprocessJob | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        artifact_name=artifact_name,
        job_id=job_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    artifact_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> DocumentArtifactReprocessJob | Error | None:
    """Cancel a derived document artifact reprocess job

     Cancels a queued document artifact reprocess job. If a reprocess pass is
    already running, the response returns the current running state;
    cancellation is applied only at pass boundaries so the API never reports
    a committed in-flight pass as cancelled.

    Args:
        table_name (str):
        artifact_name (str):
        job_id (str):

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
            job_id=job_id,
            client=client,
        )
    ).parsed
