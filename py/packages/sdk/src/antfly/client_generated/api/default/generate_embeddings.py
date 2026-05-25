from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.termite_embed_request import TermiteEmbedRequest
from ...models.termite_embed_response import TermiteEmbedResponse
from ...models.termite_error import TermiteError
from ...types import Response


def _get_kwargs(
    *,
    body: TermiteEmbedRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ml/v1/embed",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> TermiteEmbedResponse | TermiteError | None:
    if response.status_code == 200:
        response_200 = TermiteEmbedResponse.from_dict(response.json())

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[TermiteEmbedResponse | TermiteError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteEmbedRequest,
) -> Response[TermiteEmbedResponse | TermiteError]:
    """Create embeddings (alias of `/embeddings`)

     Alias of `/ml/v1/embeddings`.

    Accepts the OpenAI embeddings request shape and returns the same OpenAI-compatible
    response envelope. For sparse-capable models, `data[i].embedding` is a sparse
    vector object instead of a dense float array.

    Args:
        body (TermiteEmbedRequest): OpenAI-compatible embedding request with Termite multimodal
            content-part extension

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteEmbedResponse | TermiteError]
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
    body: TermiteEmbedRequest,
) -> TermiteEmbedResponse | TermiteError | None:
    """Create embeddings (alias of `/embeddings`)

     Alias of `/ml/v1/embeddings`.

    Accepts the OpenAI embeddings request shape and returns the same OpenAI-compatible
    response envelope. For sparse-capable models, `data[i].embedding` is a sparse
    vector object instead of a dense float array.

    Args:
        body (TermiteEmbedRequest): OpenAI-compatible embedding request with Termite multimodal
            content-part extension

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteEmbedResponse | TermiteError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteEmbedRequest,
) -> Response[TermiteEmbedResponse | TermiteError]:
    """Create embeddings (alias of `/embeddings`)

     Alias of `/ml/v1/embeddings`.

    Accepts the OpenAI embeddings request shape and returns the same OpenAI-compatible
    response envelope. For sparse-capable models, `data[i].embedding` is a sparse
    vector object instead of a dense float array.

    Args:
        body (TermiteEmbedRequest): OpenAI-compatible embedding request with Termite multimodal
            content-part extension

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteEmbedResponse | TermiteError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteEmbedRequest,
) -> TermiteEmbedResponse | TermiteError | None:
    """Create embeddings (alias of `/embeddings`)

     Alias of `/ml/v1/embeddings`.

    Accepts the OpenAI embeddings request shape and returns the same OpenAI-compatible
    response envelope. For sparse-capable models, `data[i].embedding` is a sparse
    vector object instead of a dense float array.

    Args:
        body (TermiteEmbedRequest): OpenAI-compatible embedding request with Termite multimodal
            content-part extension

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteEmbedResponse | TermiteError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
