from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.query_dependency_error import QueryDependencyError
from ...models.research_job import ResearchJob
from ...models.research_job_start_request import ResearchJobStartRequest
from ...types import Response


def _get_kwargs(
    *,
    body: ResearchJobStartRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/agents/research/jobs",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | QueryDependencyError | ResearchJob | None:
    if response.status_code == 202:
        response_202 = ResearchJob.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 429:
        response_429 = QueryDependencyError.from_dict(response.json())

        return response_429

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
) -> Response[Any | Error | QueryDependencyError | ResearchJob]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: ResearchJobStartRequest,
) -> Response[Any | Error | QueryDependencyError | ResearchJob]:
    """Start a durable research job

     Persists a research request as a durable job that advances one bounded
    phase at a time. Each phase checkpoints its research_state, so a job
    survives server restarts and can be resumed by any caller holding the
    same identity. Jobs are scoped to the authenticated principal.

    Args:
        body (ResearchJobStartRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | QueryDependencyError | ResearchJob]
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
    client: AuthenticatedClient,
    body: ResearchJobStartRequest,
) -> Any | Error | QueryDependencyError | ResearchJob | None:
    """Start a durable research job

     Persists a research request as a durable job that advances one bounded
    phase at a time. Each phase checkpoints its research_state, so a job
    survives server restarts and can be resumed by any caller holding the
    same identity. Jobs are scoped to the authenticated principal.

    Args:
        body (ResearchJobStartRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | QueryDependencyError | ResearchJob
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: ResearchJobStartRequest,
) -> Response[Any | Error | QueryDependencyError | ResearchJob]:
    """Start a durable research job

     Persists a research request as a durable job that advances one bounded
    phase at a time. Each phase checkpoints its research_state, so a job
    survives server restarts and can be resumed by any caller holding the
    same identity. Jobs are scoped to the authenticated principal.

    Args:
        body (ResearchJobStartRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | QueryDependencyError | ResearchJob]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: ResearchJobStartRequest,
) -> Any | Error | QueryDependencyError | ResearchJob | None:
    """Start a durable research job

     Persists a research request as a durable job that advances one bounded
    phase at a time. Each phase checkpoints its research_state, so a job
    survives server restarts and can be resumed by any caller holding the
    same identity. Jobs are scoped to the authenticated principal.

    Args:
        body (ResearchJobStartRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | QueryDependencyError | ResearchJob
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
