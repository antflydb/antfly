from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.termite_error import TermiteError
from ...models.termite_rerank_request import TermiteRerankRequest
from ...models.termite_rerank_response import TermiteRerankResponse
from ...types import Response


def _get_kwargs(
    *,
    body: TermiteRerankRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ml/v1/rerank",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> TermiteError | TermiteRerankResponse | None:
    if response.status_code == 200:
        response_200 = TermiteRerankResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = TermiteError.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = TermiteError.from_dict(response.json())

        return response_404

    if response.status_code == 500:
        response_500 = TermiteError.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = TermiteError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[TermiteError | TermiteRerankResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteRerankRequest,
) -> Response[TermiteError | TermiteRerankResponse]:
    """Rerank prompts by relevance

     Re-scores pre-rendered text prompts based on relevance to a query using native or ONNX reranking
    models.

    ## Client Responsibilities

    The client must:
    1. Extract relevant fields from documents
    2. Render any templates
    3. Send pre-rendered text strings as `prompts`

    This design keeps Termite stateless and allows clients to customize rendering logic.

    ## Models

    - Models are auto-discovered from `models_dir/rerankers/`
    - Cross-encoder rerankers are supported through the existing text scorer
    - Late-interaction text rerankers such as ColBERT can opt in with `model_manifest.json` capability
    `late_interaction` or `colbert`
    - Supports quantized models (`model_quantized.onnx`)
    - Automatically prefers quantized variants if available

    This endpoint is still text-only. Real ColQwen-style multimodal reranking requires a future request
    shape that carries page images or image-derived embeddings.

    For document-based reranking with field extraction, use the client-side
    `lib/reranking` package which handles rendering before calling this endpoint.

    Args:
        body (TermiteRerankRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteError | TermiteRerankResponse]
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
    body: TermiteRerankRequest,
) -> TermiteError | TermiteRerankResponse | None:
    """Rerank prompts by relevance

     Re-scores pre-rendered text prompts based on relevance to a query using native or ONNX reranking
    models.

    ## Client Responsibilities

    The client must:
    1. Extract relevant fields from documents
    2. Render any templates
    3. Send pre-rendered text strings as `prompts`

    This design keeps Termite stateless and allows clients to customize rendering logic.

    ## Models

    - Models are auto-discovered from `models_dir/rerankers/`
    - Cross-encoder rerankers are supported through the existing text scorer
    - Late-interaction text rerankers such as ColBERT can opt in with `model_manifest.json` capability
    `late_interaction` or `colbert`
    - Supports quantized models (`model_quantized.onnx`)
    - Automatically prefers quantized variants if available

    This endpoint is still text-only. Real ColQwen-style multimodal reranking requires a future request
    shape that carries page images or image-derived embeddings.

    For document-based reranking with field extraction, use the client-side
    `lib/reranking` package which handles rendering before calling this endpoint.

    Args:
        body (TermiteRerankRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteError | TermiteRerankResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteRerankRequest,
) -> Response[TermiteError | TermiteRerankResponse]:
    """Rerank prompts by relevance

     Re-scores pre-rendered text prompts based on relevance to a query using native or ONNX reranking
    models.

    ## Client Responsibilities

    The client must:
    1. Extract relevant fields from documents
    2. Render any templates
    3. Send pre-rendered text strings as `prompts`

    This design keeps Termite stateless and allows clients to customize rendering logic.

    ## Models

    - Models are auto-discovered from `models_dir/rerankers/`
    - Cross-encoder rerankers are supported through the existing text scorer
    - Late-interaction text rerankers such as ColBERT can opt in with `model_manifest.json` capability
    `late_interaction` or `colbert`
    - Supports quantized models (`model_quantized.onnx`)
    - Automatically prefers quantized variants if available

    This endpoint is still text-only. Real ColQwen-style multimodal reranking requires a future request
    shape that carries page images or image-derived embeddings.

    For document-based reranking with field extraction, use the client-side
    `lib/reranking` package which handles rendering before calling this endpoint.

    Args:
        body (TermiteRerankRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteError | TermiteRerankResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteRerankRequest,
) -> TermiteError | TermiteRerankResponse | None:
    """Rerank prompts by relevance

     Re-scores pre-rendered text prompts based on relevance to a query using native or ONNX reranking
    models.

    ## Client Responsibilities

    The client must:
    1. Extract relevant fields from documents
    2. Render any templates
    3. Send pre-rendered text strings as `prompts`

    This design keeps Termite stateless and allows clients to customize rendering logic.

    ## Models

    - Models are auto-discovered from `models_dir/rerankers/`
    - Cross-encoder rerankers are supported through the existing text scorer
    - Late-interaction text rerankers such as ColBERT can opt in with `model_manifest.json` capability
    `late_interaction` or `colbert`
    - Supports quantized models (`model_quantized.onnx`)
    - Automatically prefers quantized variants if available

    This endpoint is still text-only. Real ColQwen-style multimodal reranking requires a future request
    shape that carries page images or image-derived embeddings.

    For document-based reranking with field extraction, use the client-side
    `lib/reranking` package which handles rendering before calling this endpoint.

    Args:
        body (TermiteRerankRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteError | TermiteRerankResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
