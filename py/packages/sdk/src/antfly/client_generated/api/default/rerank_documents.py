from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_rerank_request import InferenceRerankRequest
from ...models.inference_rerank_response import InferenceRerankResponse
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: InferenceRerankRequest,
    accept: str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(accept, Unset):
        headers["Accept"] = accept

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/rerank",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceRerankResponse | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceRerankResponse.from_dict(response.json())

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
        response_503 = InferenceTransientCapacityError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferenceRerankResponse | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRerankRequest,
    accept: str | Unset = UNSET,
) -> Response[InferenceError | InferenceRerankResponse | InferenceTransientCapacityError]:
    """Rerank documents by relevance

     Re-scores documents by relevance to a text query. Returns one score per
    document, in request order.

    Each entry in `documents` is either a string or an array of content parts, in
    the same format that generation and embedding use: `text` parts, `image_url`
    parts, and inline `media` parts with an `image/*` MIME type. The client renders
    document fields or templates to text before calling this endpoint.

    Text-only documents work with any reranker. Documents with images require a
    model that supports them: a ColQwen-style late-interaction reranker (manifest
    capability `colqwen` or `multimodal_late_interaction`) or a Qwen3-VL reranker
    bundled with its GGUF vision projector. Otherwise the request is rejected with
    a `400`. Within a request that contains images, documents without images are
    scored by the model's text scorer.

    ## Models

    - Models are auto-discovered from `models_dir/rerankers/`
    - Cross-encoder rerankers are supported through the text scorer
    - Late-interaction text rerankers such as ColBERT can opt in with `model_manifest.json` capability
    `late_interaction` or `colbert`
    - Automatically prefers quantized variants if available

    Remote image URLs are fetched subject to the configured content security
    policy. Image headers and aggregate decoded pixels are admitted before the
    model loads.

    Args:
        accept (str | Unset):
        body (InferenceRerankRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceRerankResponse | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
        accept=accept,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRerankRequest,
    accept: str | Unset = UNSET,
) -> InferenceError | InferenceRerankResponse | InferenceTransientCapacityError | None:
    """Rerank documents by relevance

     Re-scores documents by relevance to a text query. Returns one score per
    document, in request order.

    Each entry in `documents` is either a string or an array of content parts, in
    the same format that generation and embedding use: `text` parts, `image_url`
    parts, and inline `media` parts with an `image/*` MIME type. The client renders
    document fields or templates to text before calling this endpoint.

    Text-only documents work with any reranker. Documents with images require a
    model that supports them: a ColQwen-style late-interaction reranker (manifest
    capability `colqwen` or `multimodal_late_interaction`) or a Qwen3-VL reranker
    bundled with its GGUF vision projector. Otherwise the request is rejected with
    a `400`. Within a request that contains images, documents without images are
    scored by the model's text scorer.

    ## Models

    - Models are auto-discovered from `models_dir/rerankers/`
    - Cross-encoder rerankers are supported through the text scorer
    - Late-interaction text rerankers such as ColBERT can opt in with `model_manifest.json` capability
    `late_interaction` or `colbert`
    - Automatically prefers quantized variants if available

    Remote image URLs are fetched subject to the configured content security
    policy. Image headers and aggregate decoded pixels are admitted before the
    model loads.

    Args:
        accept (str | Unset):
        body (InferenceRerankRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceRerankResponse | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
        accept=accept,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRerankRequest,
    accept: str | Unset = UNSET,
) -> Response[InferenceError | InferenceRerankResponse | InferenceTransientCapacityError]:
    """Rerank documents by relevance

     Re-scores documents by relevance to a text query. Returns one score per
    document, in request order.

    Each entry in `documents` is either a string or an array of content parts, in
    the same format that generation and embedding use: `text` parts, `image_url`
    parts, and inline `media` parts with an `image/*` MIME type. The client renders
    document fields or templates to text before calling this endpoint.

    Text-only documents work with any reranker. Documents with images require a
    model that supports them: a ColQwen-style late-interaction reranker (manifest
    capability `colqwen` or `multimodal_late_interaction`) or a Qwen3-VL reranker
    bundled with its GGUF vision projector. Otherwise the request is rejected with
    a `400`. Within a request that contains images, documents without images are
    scored by the model's text scorer.

    ## Models

    - Models are auto-discovered from `models_dir/rerankers/`
    - Cross-encoder rerankers are supported through the text scorer
    - Late-interaction text rerankers such as ColBERT can opt in with `model_manifest.json` capability
    `late_interaction` or `colbert`
    - Automatically prefers quantized variants if available

    Remote image URLs are fetched subject to the configured content security
    policy. Image headers and aggregate decoded pixels are admitted before the
    model loads.

    Args:
        accept (str | Unset):
        body (InferenceRerankRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceRerankResponse | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
        accept=accept,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRerankRequest,
    accept: str | Unset = UNSET,
) -> InferenceError | InferenceRerankResponse | InferenceTransientCapacityError | None:
    """Rerank documents by relevance

     Re-scores documents by relevance to a text query. Returns one score per
    document, in request order.

    Each entry in `documents` is either a string or an array of content parts, in
    the same format that generation and embedding use: `text` parts, `image_url`
    parts, and inline `media` parts with an `image/*` MIME type. The client renders
    document fields or templates to text before calling this endpoint.

    Text-only documents work with any reranker. Documents with images require a
    model that supports them: a ColQwen-style late-interaction reranker (manifest
    capability `colqwen` or `multimodal_late_interaction`) or a Qwen3-VL reranker
    bundled with its GGUF vision projector. Otherwise the request is rejected with
    a `400`. Within a request that contains images, documents without images are
    scored by the model's text scorer.

    ## Models

    - Models are auto-discovered from `models_dir/rerankers/`
    - Cross-encoder rerankers are supported through the text scorer
    - Late-interaction text rerankers such as ColBERT can opt in with `model_manifest.json` capability
    `late_interaction` or `colbert`
    - Automatically prefers quantized variants if available

    Remote image URLs are fetched subject to the configured content security
    policy. Image headers and aggregate decoded pixels are admitted before the
    model loads.

    Args:
        accept (str | Unset):
        body (InferenceRerankRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceRerankResponse | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            accept=accept,
        )
    ).parsed
