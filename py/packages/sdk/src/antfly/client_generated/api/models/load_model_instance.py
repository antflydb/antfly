from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_model_instance import InferenceModelInstance
from ...models.inference_model_instance_load_request import InferenceModelInstanceLoadRequest
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceModelInstanceLoadRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/models/instances",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceModelInstance | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceModelInstance.from_dict(response.json())

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferenceModelInstance | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceModelInstanceLoadRequest,
) -> Response[InferenceError | InferenceModelInstance | InferenceTransientCapacityError]:
    """Load a model instance

     Prepare the instance used by the requested task. Reuses an existing compatible instance. Normal idle
    eviction still applies; loading does not pin weights or guarantee device residency.

    Args:
        body (InferenceModelInstanceLoadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceModelInstance | InferenceTransientCapacityError]
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
    body: InferenceModelInstanceLoadRequest,
) -> InferenceError | InferenceModelInstance | InferenceTransientCapacityError | None:
    """Load a model instance

     Prepare the instance used by the requested task. Reuses an existing compatible instance. Normal idle
    eviction still applies; loading does not pin weights or guarantee device residency.

    Args:
        body (InferenceModelInstanceLoadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceModelInstance | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceModelInstanceLoadRequest,
) -> Response[InferenceError | InferenceModelInstance | InferenceTransientCapacityError]:
    """Load a model instance

     Prepare the instance used by the requested task. Reuses an existing compatible instance. Normal idle
    eviction still applies; loading does not pin weights or guarantee device residency.

    Args:
        body (InferenceModelInstanceLoadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceModelInstance | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceModelInstanceLoadRequest,
) -> InferenceError | InferenceModelInstance | InferenceTransientCapacityError | None:
    """Load a model instance

     Prepare the instance used by the requested task. Reuses an existing compatible instance. Normal idle
    eviction still applies; loading does not pin weights or guarantee device residency.

    Args:
        body (InferenceModelInstanceLoadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceModelInstance | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
