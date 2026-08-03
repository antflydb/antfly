from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_embed_request import InferenceEmbedRequest
from ...models.inference_embed_response import InferenceEmbedResponse
from ...models.inference_error import InferenceError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceEmbedRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/embed",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceEmbedResponse | InferenceError | None:
    if response.status_code == 200:
        response_200 = InferenceEmbedResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = InferenceError.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = InferenceError.from_dict(response.json())

        return response_404

    if response.status_code == 413:
        response_413 = InferenceError.from_dict(response.json())

        return response_413

    if response.status_code == 500:
        response_500 = InferenceError.from_dict(response.json())

        return response_500

    if response.status_code == 502:
        response_502 = InferenceError.from_dict(response.json())

        return response_502

    if response.status_code == 503:
        response_503 = InferenceError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceEmbedResponse | InferenceError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceEmbedRequest,
) -> Response[InferenceEmbedResponse | InferenceError]:
    """Create embeddings (alias of `/embeddings`)

     Alias of `/ai/v1/embeddings`.

    Accepts the OpenAI embeddings request shape and returns the same OpenAI-compatible
    response envelope. For sparse-capable models, `data[i].embedding` is a sparse
    vector object instead of a dense float array. Dense image inputs are header-validated
    and admitted against the aggregate decoded-pixel budget before model loading. Remote
    URL byte potential is reserved before fetch; inline sources use their actual encoded size.

    Args:
        body (InferenceEmbedRequest): OpenAI-compatible embedding request with inference
            multimodal content-part extension

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceEmbedResponse | InferenceError]
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
    body: InferenceEmbedRequest,
) -> InferenceEmbedResponse | InferenceError | None:
    """Create embeddings (alias of `/embeddings`)

     Alias of `/ai/v1/embeddings`.

    Accepts the OpenAI embeddings request shape and returns the same OpenAI-compatible
    response envelope. For sparse-capable models, `data[i].embedding` is a sparse
    vector object instead of a dense float array. Dense image inputs are header-validated
    and admitted against the aggregate decoded-pixel budget before model loading. Remote
    URL byte potential is reserved before fetch; inline sources use their actual encoded size.

    Args:
        body (InferenceEmbedRequest): OpenAI-compatible embedding request with inference
            multimodal content-part extension

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceEmbedResponse | InferenceError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceEmbedRequest,
) -> Response[InferenceEmbedResponse | InferenceError]:
    """Create embeddings (alias of `/embeddings`)

     Alias of `/ai/v1/embeddings`.

    Accepts the OpenAI embeddings request shape and returns the same OpenAI-compatible
    response envelope. For sparse-capable models, `data[i].embedding` is a sparse
    vector object instead of a dense float array. Dense image inputs are header-validated
    and admitted against the aggregate decoded-pixel budget before model loading. Remote
    URL byte potential is reserved before fetch; inline sources use their actual encoded size.

    Args:
        body (InferenceEmbedRequest): OpenAI-compatible embedding request with inference
            multimodal content-part extension

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceEmbedResponse | InferenceError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceEmbedRequest,
) -> InferenceEmbedResponse | InferenceError | None:
    """Create embeddings (alias of `/embeddings`)

     Alias of `/ai/v1/embeddings`.

    Accepts the OpenAI embeddings request shape and returns the same OpenAI-compatible
    response envelope. For sparse-capable models, `data[i].embedding` is a sparse
    vector object instead of a dense float array. Dense image inputs are header-validated
    and admitted against the aggregate decoded-pixel budget before model loading. Remote
    URL byte potential is reserved before fetch; inline sources use their actual encoded size.

    Args:
        body (InferenceEmbedRequest): OpenAI-compatible embedding request with inference
            multimodal content-part extension

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceEmbedResponse | InferenceError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
