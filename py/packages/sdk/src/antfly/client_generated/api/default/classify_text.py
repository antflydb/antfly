from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_classify_request import InferenceClassifyRequest
from ...models.inference_classify_response import InferenceClassifyResponse
from ...models.inference_error import InferenceError
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceClassifyRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/classify",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceClassifyResponse | InferenceError | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceClassifyResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 404:
        response_404 = InferenceError.from_dict(response.json())

        return response_404

    if response.status_code == 500:
        response_500 = InferenceError.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = InferenceTransientCapacityError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceClassifyResponse | InferenceError | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceClassifyRequest,
) -> Response[InferenceClassifyResponse | InferenceError | InferenceTransientCapacityError]:
    """Classify text against candidate labels

     Classifies one or more texts with either a zero-shot NLI classifier or
    a classification-capable extractor model. Candidate labels are supplied
    per request, and `multi_label` controls whether their scores are
    independent or normalized across labels.

    Args:
        body (InferenceClassifyRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceClassifyResponse | InferenceError | InferenceTransientCapacityError]
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
    body: InferenceClassifyRequest,
) -> InferenceClassifyResponse | InferenceError | InferenceTransientCapacityError | None:
    """Classify text against candidate labels

     Classifies one or more texts with either a zero-shot NLI classifier or
    a classification-capable extractor model. Candidate labels are supplied
    per request, and `multi_label` controls whether their scores are
    independent or normalized across labels.

    Args:
        body (InferenceClassifyRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceClassifyResponse | InferenceError | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceClassifyRequest,
) -> Response[InferenceClassifyResponse | InferenceError | InferenceTransientCapacityError]:
    """Classify text against candidate labels

     Classifies one or more texts with either a zero-shot NLI classifier or
    a classification-capable extractor model. Candidate labels are supplied
    per request, and `multi_label` controls whether their scores are
    independent or normalized across labels.

    Args:
        body (InferenceClassifyRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceClassifyResponse | InferenceError | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceClassifyRequest,
) -> InferenceClassifyResponse | InferenceError | InferenceTransientCapacityError | None:
    """Classify text against candidate labels

     Classifies one or more texts with either a zero-shot NLI classifier or
    a classification-capable extractor model. Candidate labels are supplied
    per request, and `multi_label` controls whether their scores are
    independent or normalized across labels.

    Args:
        body (InferenceClassifyRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceClassifyResponse | InferenceError | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
