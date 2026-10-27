from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_document_job import InferenceDocumentJob
from ...models.inference_document_job_request import InferenceDocumentJobRequest
from ...models.inference_error import InferenceError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceDocumentJobRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/read/jobs",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceDocumentJob | InferenceError | None:
    if response.status_code == 202:
        response_202 = InferenceDocumentJob.from_dict(response.json())

        return response_202

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 503:
        response_503 = InferenceError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceDocumentJob | InferenceError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDocumentJobRequest,
) -> Response[InferenceDocumentJob | InferenceError]:
    """Submit a durable read job

     Unified Antfly API only. Persists the request before returning. Jobs continue after the submitting
    client disconnects. Sources and accepted stages are immutable snapshots; ambiguous calls remain
    charged on retry. Retention is absolute and is not extended by polling or resuming.

    Args:
        body (InferenceDocumentJobRequest): Durable document execution on the unified Antfly API.
            Requires configured durable job storage and local inference orchestration. The request
            uses the ordinary read or extract schema. Credentials must use server connections,
            environment variables, or secret references.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceDocumentJob | InferenceError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDocumentJobRequest,
) -> InferenceDocumentJob | InferenceError | None:
    """Submit a durable read job

     Unified Antfly API only. Persists the request before returning. Jobs continue after the submitting
    client disconnects. Sources and accepted stages are immutable snapshots; ambiguous calls remain
    charged on retry. Retention is absolute and is not extended by polling or resuming.

    Args:
        body (InferenceDocumentJobRequest): Durable document execution on the unified Antfly API.
            Requires configured durable job storage and local inference orchestration. The request
            uses the ordinary read or extract schema. Credentials must use server connections,
            environment variables, or secret references.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceDocumentJob | InferenceError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDocumentJobRequest,
) -> Response[InferenceDocumentJob | InferenceError]:
    """Submit a durable read job

     Unified Antfly API only. Persists the request before returning. Jobs continue after the submitting
    client disconnects. Sources and accepted stages are immutable snapshots; ambiguous calls remain
    charged on retry. Retention is absolute and is not extended by polling or resuming.

    Args:
        body (InferenceDocumentJobRequest): Durable document execution on the unified Antfly API.
            Requires configured durable job storage and local inference orchestration. The request
            uses the ordinary read or extract schema. Credentials must use server connections,
            environment variables, or secret references.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceDocumentJob | InferenceError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDocumentJobRequest,
) -> InferenceDocumentJob | InferenceError | None:
    """Submit a durable read job

     Unified Antfly API only. Persists the request before returning. Jobs continue after the submitting
    client disconnects. Sources and accepted stages are immutable snapshots; ambiguous calls remain
    charged on retry. Retention is absolute and is not extended by polling or resuming.

    Args:
        body (InferenceDocumentJobRequest): Durable document execution on the unified Antfly API.
            Requires configured durable job storage and local inference orchestration. The request
            uses the ordinary read or extract schema. Credentials must use server connections,
            environment variables, or secret references.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceDocumentJob | InferenceError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
