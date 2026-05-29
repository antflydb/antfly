from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_rewrite_request import InferenceRewriteRequest
from ...models.inference_rewrite_response import InferenceRewriteResponse
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceRewriteRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/rewrite",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceRewriteResponse | None:
    if response.status_code == 200:
        response_200 = InferenceRewriteResponse.from_dict(response.json())

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
) -> Response[InferenceError | InferenceRewriteResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRewriteRequest,
) -> Response[InferenceError | InferenceRewriteResponse]:
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
        body (InferenceRewriteRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceRewriteResponse]
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
    body: InferenceRewriteRequest,
) -> InferenceError | InferenceRewriteResponse | None:
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
        body (InferenceRewriteRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceRewriteResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRewriteRequest,
) -> Response[InferenceError | InferenceRewriteResponse]:
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
        body (InferenceRewriteRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceRewriteResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRewriteRequest,
) -> InferenceError | InferenceRewriteResponse | None:
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
        body (InferenceRewriteRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceRewriteResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
