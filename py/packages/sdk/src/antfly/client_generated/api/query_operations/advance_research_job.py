from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.inference_capacity_error import InferenceCapacityError
from ...models.query_dependency_error import QueryDependencyError
from ...models.query_temporarily_unavailable_error import QueryTemporarilyUnavailableError
from ...models.research_job import ResearchJob
from ...models.research_job_advance_request import ResearchJobAdvanceRequest
from ...types import UNSET, Response, Unset


def _get_kwargs(
    job_id: str,
    *,
    body: ResearchJobAdvanceRequest | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/agents/research/jobs/{job_id}/advance".format(
            job_id=quote(str(job_id), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | ResearchJob | None:
    if response.status_code == 200:
        response_200 = ResearchJob.from_dict(response.json())

        return response_200

    if response.status_code == 202:
        response_202 = ResearchJob.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 429:
        response_429 = QueryDependencyError.from_dict(response.json())

        return response_429

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 502:
        response_502 = QueryDependencyError.from_dict(response.json())

        return response_502

    if response.status_code == 503:

        def _parse_response_503(data: object) -> InferenceCapacityError | QueryTemporarilyUnavailableError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_503_type_0 = QueryTemporarilyUnavailableError.from_dict(data)

                return response_503_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_503_type_1 = InferenceCapacityError.from_dict(data)

            return response_503_type_1

        response_503 = _parse_response_503(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | ResearchJob]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    job_id: str,
    *,
    client: AuthenticatedClient,
    body: ResearchJobAdvanceRequest | Unset = UNSET,
) -> Response[Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | ResearchJob]:
    """Advance a durable research job

     Runs up to `max_phases` bounded research phases and persists the
    checkpoint after each one. Concurrent advances of the same job are
    rejected with 409.

    Args:
        job_id (str):
        body (ResearchJobAdvanceRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | ResearchJob]
    """

    kwargs = _get_kwargs(
        job_id=job_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    job_id: str,
    *,
    client: AuthenticatedClient,
    body: ResearchJobAdvanceRequest | Unset = UNSET,
) -> Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | ResearchJob | None:
    """Advance a durable research job

     Runs up to `max_phases` bounded research phases and persists the
    checkpoint after each one. Concurrent advances of the same job are
    rejected with 409.

    Args:
        job_id (str):
        body (ResearchJobAdvanceRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | ResearchJob
    """

    return sync_detailed(
        job_id=job_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    job_id: str,
    *,
    client: AuthenticatedClient,
    body: ResearchJobAdvanceRequest | Unset = UNSET,
) -> Response[Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | ResearchJob]:
    """Advance a durable research job

     Runs up to `max_phases` bounded research phases and persists the
    checkpoint after each one. Concurrent advances of the same job are
    rejected with 409.

    Args:
        job_id (str):
        body (ResearchJobAdvanceRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | ResearchJob]
    """

    kwargs = _get_kwargs(
        job_id=job_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    job_id: str,
    *,
    client: AuthenticatedClient,
    body: ResearchJobAdvanceRequest | Unset = UNSET,
) -> Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | ResearchJob | None:
    """Advance a durable research job

     Runs up to `max_phases` bounded research phases and persists the
    checkpoint after each one. Concurrent advances of the same job are
    rejected with 409.

    Args:
        job_id (str):
        body (ResearchJobAdvanceRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | ResearchJob
    """

    return (
        await asyncio_detailed(
            job_id=job_id,
            client=client,
            body=body,
        )
    ).parsed
