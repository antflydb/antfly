from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_context_prepare_request import InferenceContextPrepareRequest
from ...models.inference_context_prepare_response import InferenceContextPrepareResponse
from ...models.inference_error import InferenceError
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceContextPrepareRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/chat/context/prepare",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceContextPrepareResponse | InferenceError | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceContextPrepareResponse.from_dict(response.json())

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

    if response.status_code == 503:
        response_503 = InferenceTransientCapacityError.from_dict(response.json())

        return response_503

    if response.status_code == 507:
        response_507 = InferenceError.from_dict(response.json())

        return response_507

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceContextPrepareResponse | InferenceError | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceContextPrepareRequest,
) -> Response[InferenceContextPrepareResponse | InferenceError | InferenceTransientCapacityError]:
    """Inspect or prepare bounded chat context

     Plans a chat transcript against the selected model's input budget. `inspect`
    reports the plan without changing the supplied messages. `window` may drop old
    turns while retaining recent turns. `compact` may also return a portable
    structured-memory checkpoint when that capability is available. The response
    diagnostics describe the action actually taken; clients must not infer semantic
    equivalence from a successful response. This route budgets the target model only;
    speculative draft-model preparation is not supported.

    Args:
        body (InferenceContextPrepareRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceContextPrepareResponse | InferenceError | InferenceTransientCapacityError]
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
    body: InferenceContextPrepareRequest,
) -> InferenceContextPrepareResponse | InferenceError | InferenceTransientCapacityError | None:
    """Inspect or prepare bounded chat context

     Plans a chat transcript against the selected model's input budget. `inspect`
    reports the plan without changing the supplied messages. `window` may drop old
    turns while retaining recent turns. `compact` may also return a portable
    structured-memory checkpoint when that capability is available. The response
    diagnostics describe the action actually taken; clients must not infer semantic
    equivalence from a successful response. This route budgets the target model only;
    speculative draft-model preparation is not supported.

    Args:
        body (InferenceContextPrepareRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceContextPrepareResponse | InferenceError | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceContextPrepareRequest,
) -> Response[InferenceContextPrepareResponse | InferenceError | InferenceTransientCapacityError]:
    """Inspect or prepare bounded chat context

     Plans a chat transcript against the selected model's input budget. `inspect`
    reports the plan without changing the supplied messages. `window` may drop old
    turns while retaining recent turns. `compact` may also return a portable
    structured-memory checkpoint when that capability is available. The response
    diagnostics describe the action actually taken; clients must not infer semantic
    equivalence from a successful response. This route budgets the target model only;
    speculative draft-model preparation is not supported.

    Args:
        body (InferenceContextPrepareRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceContextPrepareResponse | InferenceError | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceContextPrepareRequest,
) -> InferenceContextPrepareResponse | InferenceError | InferenceTransientCapacityError | None:
    """Inspect or prepare bounded chat context

     Plans a chat transcript against the selected model's input budget. `inspect`
    reports the plan without changing the supplied messages. `window` may drop old
    turns while retaining recent turns. `compact` may also return a portable
    structured-memory checkpoint when that capability is available. The response
    diagnostics describe the action actually taken; clients must not infer semantic
    equivalence from a successful response. This route budgets the target model only;
    speculative draft-model preparation is not supported.

    Args:
        body (InferenceContextPrepareRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceContextPrepareResponse | InferenceError | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
