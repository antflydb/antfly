from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_rerank_multimodal_request import InferenceRerankMultimodalRequest
from ...models.inference_rerank_response import InferenceRerankResponse
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceRerankMultimodalRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/rerank_multimodal",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceRerankResponse | None:
    if response.status_code == 200:
        response_200 = InferenceRerankResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = InferenceError.from_dict(response.json())

        return response_404

    if response.status_code == 501:
        response_501 = InferenceError.from_dict(response.json())

        return response_501

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferenceRerankResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRerankMultimodalRequest,
) -> Response[InferenceError | InferenceRerankResponse]:
    """Rerank multimodal documents by relevance

     Re-scores multimodal documents based on relevance to a text query.

    This endpoint accepts the same content-part image conventions as generation and embedding.
    Text-only requests can be served immediately. Image-bearing requests reserve the stable
    contract for native ColQwen-style late-interaction reranking as that encoder lands.
    Image-bearing requests already run native Zig image preprocessing and grid preparation.

    Args:
        body (InferenceRerankMultimodalRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceRerankResponse]
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
    body: InferenceRerankMultimodalRequest,
) -> InferenceError | InferenceRerankResponse | None:
    """Rerank multimodal documents by relevance

     Re-scores multimodal documents based on relevance to a text query.

    This endpoint accepts the same content-part image conventions as generation and embedding.
    Text-only requests can be served immediately. Image-bearing requests reserve the stable
    contract for native ColQwen-style late-interaction reranking as that encoder lands.
    Image-bearing requests already run native Zig image preprocessing and grid preparation.

    Args:
        body (InferenceRerankMultimodalRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceRerankResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRerankMultimodalRequest,
) -> Response[InferenceError | InferenceRerankResponse]:
    """Rerank multimodal documents by relevance

     Re-scores multimodal documents based on relevance to a text query.

    This endpoint accepts the same content-part image conventions as generation and embedding.
    Text-only requests can be served immediately. Image-bearing requests reserve the stable
    contract for native ColQwen-style late-interaction reranking as that encoder lands.
    Image-bearing requests already run native Zig image preprocessing and grid preparation.

    Args:
        body (InferenceRerankMultimodalRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceRerankResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRerankMultimodalRequest,
) -> InferenceError | InferenceRerankResponse | None:
    """Rerank multimodal documents by relevance

     Re-scores multimodal documents based on relevance to a text query.

    This endpoint accepts the same content-part image conventions as generation and embedding.
    Text-only requests can be served immediately. Image-bearing requests reserve the stable
    contract for native ColQwen-style late-interaction reranking as that encoder lands.
    Image-bearing requests already run native Zig image preprocessing and grid preparation.

    Args:
        body (InferenceRerankMultimodalRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceRerankResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
