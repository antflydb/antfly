from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.termite_error import TermiteError
from ...models.termite_rewrite_request import TermiteRewriteRequest
from ...models.termite_rewrite_response import TermiteRewriteResponse
from ...types import Response


def _get_kwargs(
    *,
    body: TermiteRewriteRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ml/v1/rewrite",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> TermiteError | TermiteRewriteResponse | None:
    if response.status_code == 200:
        response_200 = TermiteRewriteResponse.from_dict(response.json())

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
) -> Response[TermiteError | TermiteRewriteResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteRewriteRequest,
) -> Response[TermiteError | TermiteRewriteResponse]:
    """Rewrite text using Seq2Seq models

     Rewrite/transform text using Seq2Seq models (T5, FLAN-T5, BART, etc.).

    ## Models

    - Models are auto-discovered from `models_dir/rewriters/`
    - Seq2Seq models have encoder.onnx, decoder-init.onnx, and decoder.onnx files
    - Compatible with LMQG question generation models

    ## Use Cases

    - **Question Generation**: Generate questions from answer-context pairs
    - **Query Generation**: Generate search queries from documents
    - **Paraphrasing**: Rewrite text in different words
    - **Translation**: Translate text between languages

    Args:
        body (TermiteRewriteRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteError | TermiteRewriteResponse]
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
    body: TermiteRewriteRequest,
) -> TermiteError | TermiteRewriteResponse | None:
    """Rewrite text using Seq2Seq models

     Rewrite/transform text using Seq2Seq models (T5, FLAN-T5, BART, etc.).

    ## Models

    - Models are auto-discovered from `models_dir/rewriters/`
    - Seq2Seq models have encoder.onnx, decoder-init.onnx, and decoder.onnx files
    - Compatible with LMQG question generation models

    ## Use Cases

    - **Question Generation**: Generate questions from answer-context pairs
    - **Query Generation**: Generate search queries from documents
    - **Paraphrasing**: Rewrite text in different words
    - **Translation**: Translate text between languages

    Args:
        body (TermiteRewriteRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteError | TermiteRewriteResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteRewriteRequest,
) -> Response[TermiteError | TermiteRewriteResponse]:
    """Rewrite text using Seq2Seq models

     Rewrite/transform text using Seq2Seq models (T5, FLAN-T5, BART, etc.).

    ## Models

    - Models are auto-discovered from `models_dir/rewriters/`
    - Seq2Seq models have encoder.onnx, decoder-init.onnx, and decoder.onnx files
    - Compatible with LMQG question generation models

    ## Use Cases

    - **Question Generation**: Generate questions from answer-context pairs
    - **Query Generation**: Generate search queries from documents
    - **Paraphrasing**: Rewrite text in different words
    - **Translation**: Translate text between languages

    Args:
        body (TermiteRewriteRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteError | TermiteRewriteResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteRewriteRequest,
) -> TermiteError | TermiteRewriteResponse | None:
    """Rewrite text using Seq2Seq models

     Rewrite/transform text using Seq2Seq models (T5, FLAN-T5, BART, etc.).

    ## Models

    - Models are auto-discovered from `models_dir/rewriters/`
    - Seq2Seq models have encoder.onnx, decoder-init.onnx, and decoder.onnx files
    - Compatible with LMQG question generation models

    ## Use Cases

    - **Question Generation**: Generate questions from answer-context pairs
    - **Query Generation**: Generate search queries from documents
    - **Paraphrasing**: Rewrite text in different words
    - **Translation**: Translate text between languages

    Args:
        body (TermiteRewriteRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteError | TermiteRewriteResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
