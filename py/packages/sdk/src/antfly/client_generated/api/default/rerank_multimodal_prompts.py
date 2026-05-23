from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.termite_error import TermiteError
from ...models.termite_rerank_multimodal_request import TermiteRerankMultimodalRequest
from ...models.termite_rerank_response import TermiteRerankResponse
from ...types import Response


def _get_kwargs(
    *,
    body: TermiteRerankMultimodalRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ml/v1/rerank_multimodal",
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

    if response.status_code == 501:
        response_501 = TermiteError.from_dict(response.json())

        return response_501

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
    body: TermiteRerankMultimodalRequest,
) -> Response[TermiteError | TermiteRerankResponse]:
    """Rerank multimodal documents by relevance

     Re-scores multimodal documents based on relevance to a text query.

    This endpoint accepts the same content-part image conventions as generation and embedding.
    Text-only requests can be served immediately. Image-bearing requests reserve the stable
    contract for native ColQwen-style late-interaction reranking as that encoder lands.
    Image-bearing requests already run native Zig image preprocessing and grid preparation.

    Args:
        body (TermiteRerankMultimodalRequest):

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
    body: TermiteRerankMultimodalRequest,
) -> TermiteError | TermiteRerankResponse | None:
    """Rerank multimodal documents by relevance

     Re-scores multimodal documents based on relevance to a text query.

    This endpoint accepts the same content-part image conventions as generation and embedding.
    Text-only requests can be served immediately. Image-bearing requests reserve the stable
    contract for native ColQwen-style late-interaction reranking as that encoder lands.
    Image-bearing requests already run native Zig image preprocessing and grid preparation.

    Args:
        body (TermiteRerankMultimodalRequest):

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
    body: TermiteRerankMultimodalRequest,
) -> Response[TermiteError | TermiteRerankResponse]:
    """Rerank multimodal documents by relevance

     Re-scores multimodal documents based on relevance to a text query.

    This endpoint accepts the same content-part image conventions as generation and embedding.
    Text-only requests can be served immediately. Image-bearing requests reserve the stable
    contract for native ColQwen-style late-interaction reranking as that encoder lands.
    Image-bearing requests already run native Zig image preprocessing and grid preparation.

    Args:
        body (TermiteRerankMultimodalRequest):

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
    body: TermiteRerankMultimodalRequest,
) -> TermiteError | TermiteRerankResponse | None:
    """Rerank multimodal documents by relevance

     Re-scores multimodal documents based on relevance to a text query.

    This endpoint accepts the same content-part image conventions as generation and embedding.
    Text-only requests can be served immediately. Image-bearing requests reserve the stable
    contract for native ColQwen-style late-interaction reranking as that encoder lands.
    Image-bearing requests already run native Zig image preprocessing and grid preparation.

    Args:
        body (TermiteRerankMultimodalRequest):

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
