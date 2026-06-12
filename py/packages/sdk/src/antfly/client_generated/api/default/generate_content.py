from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_generate_request import InferenceGenerateRequest
from ...models.inference_generate_response import InferenceGenerateResponse
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceGenerateRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/generate",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceGenerateResponse | None:
    if response.status_code == 200:
        response_200 = InferenceGenerateResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = InferenceError.from_dict(response.json())

        return response_404

    if response.status_code == 500:
        response_500 = InferenceError.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = InferenceError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferenceGenerateResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceGenerateRequest,
) -> Response[InferenceError | InferenceGenerateResponse]:
    """Generate text using LLM (OpenAI-compatible)

     Generates text using local LLM models (e.g., Gemma 3).
    Fully compatible with the OpenAI Chat Completions API.

    ## Models

    Models are auto-discovered from `models_dir/generators/` at startup.
    Use the `/ai/v1/models` endpoint to list available models.

    ## Streaming

    Set `stream: true` to receive Server-Sent Events (SSE) with incremental
    token deltas. Each event contains a `ChatCompletionChunk` object.
    The stream ends with `data: [DONE]`.

    ## Input Format

    Uses OpenAI-compatible chat format with messages array containing role
    (system, user, assistant) and content. Set `stream: true` for streaming responses.

    Args:
        body (InferenceGenerateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceGenerateResponse]
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
    body: InferenceGenerateRequest,
) -> InferenceError | InferenceGenerateResponse | None:
    """Generate text using LLM (OpenAI-compatible)

     Generates text using local LLM models (e.g., Gemma 3).
    Fully compatible with the OpenAI Chat Completions API.

    ## Models

    Models are auto-discovered from `models_dir/generators/` at startup.
    Use the `/ai/v1/models` endpoint to list available models.

    ## Streaming

    Set `stream: true` to receive Server-Sent Events (SSE) with incremental
    token deltas. Each event contains a `ChatCompletionChunk` object.
    The stream ends with `data: [DONE]`.

    ## Input Format

    Uses OpenAI-compatible chat format with messages array containing role
    (system, user, assistant) and content. Set `stream: true` for streaming responses.

    Args:
        body (InferenceGenerateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceGenerateResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceGenerateRequest,
) -> Response[InferenceError | InferenceGenerateResponse]:
    """Generate text using LLM (OpenAI-compatible)

     Generates text using local LLM models (e.g., Gemma 3).
    Fully compatible with the OpenAI Chat Completions API.

    ## Models

    Models are auto-discovered from `models_dir/generators/` at startup.
    Use the `/ai/v1/models` endpoint to list available models.

    ## Streaming

    Set `stream: true` to receive Server-Sent Events (SSE) with incremental
    token deltas. Each event contains a `ChatCompletionChunk` object.
    The stream ends with `data: [DONE]`.

    ## Input Format

    Uses OpenAI-compatible chat format with messages array containing role
    (system, user, assistant) and content. Set `stream: true` for streaming responses.

    Args:
        body (InferenceGenerateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceGenerateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceGenerateRequest,
) -> InferenceError | InferenceGenerateResponse | None:
    """Generate text using LLM (OpenAI-compatible)

     Generates text using local LLM models (e.g., Gemma 3).
    Fully compatible with the OpenAI Chat Completions API.

    ## Models

    Models are auto-discovered from `models_dir/generators/` at startup.
    Use the `/ai/v1/models` endpoint to list available models.

    ## Streaming

    Set `stream: true` to receive Server-Sent Events (SSE) with incremental
    token deltas. Each event contains a `ChatCompletionChunk` object.
    The stream ends with `data: [DONE]`.

    ## Input Format

    Uses OpenAI-compatible chat format with messages array containing role
    (system, user, assistant) and content. Set `stream: true` for streaming responses.

    Args:
        body (InferenceGenerateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceGenerateResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
