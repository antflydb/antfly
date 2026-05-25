from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.termite_error import TermiteError
from ...models.termite_models_response import TermiteModelsResponse
from ...types import Response


def _get_kwargs() -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/ml/v1/models",
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> TermiteError | TermiteModelsResponse | None:
    if response.status_code == 200:
        response_200 = TermiteModelsResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = TermiteError.from_dict(response.json())

        return response_400

    if response.status_code == 500:
        response_500 = TermiteError.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[TermiteError | TermiteModelsResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[TermiteError | TermiteModelsResponse]:
    r"""List available models

     Returns lists of available embedding, chunking, reranking, generator, NER, rewriter, reader, and
    transcriber models.

    ## Embedders

    - ONNX models from `models_dir/embedders/`
    - Quantized variants have `-i8` suffix

    ## Chunkers

    - Always includes \"fixed\" (built-in)
    - Plus any ONNX models from `models_dir/chunkers/`

    ## Rerankers

    - Native or ONNX rerankers from `models_dir/rerankers/`
    - `model_manifest.json` capabilities can mark late-interaction text rerankers (`late_interaction`,
    `colbert`)
    - Empty if no models configured

    ## Generators

    - LLM models from `models_dir/generators/`
    - Empty if no models configured

    ## Recognizers

    - ONNX models from `models_dir/recognizers/`
    - Includes GLiNER models for zero-shot recognition

    ## Rewriters

    - Seq2Seq models from `models_dir/rewriters/`
    - T5, FLAN-T5, BART, and LMQG question generation models

    ## Readers

    - Vision2Seq models from `models_dir/readers/`
    - TrOCR, Donut, Florence-2 for OCR and document understanding

    ## Transcribers

    - Speech2Seq models from `models_dir/transcribers/`
    - Whisper, Wav2Vec2, HuBERT for speech-to-text

    Models are discovered at service startup and cached.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteError | TermiteModelsResponse]
    """

    kwargs = _get_kwargs()

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
) -> TermiteError | TermiteModelsResponse | None:
    r"""List available models

     Returns lists of available embedding, chunking, reranking, generator, NER, rewriter, reader, and
    transcriber models.

    ## Embedders

    - ONNX models from `models_dir/embedders/`
    - Quantized variants have `-i8` suffix

    ## Chunkers

    - Always includes \"fixed\" (built-in)
    - Plus any ONNX models from `models_dir/chunkers/`

    ## Rerankers

    - Native or ONNX rerankers from `models_dir/rerankers/`
    - `model_manifest.json` capabilities can mark late-interaction text rerankers (`late_interaction`,
    `colbert`)
    - Empty if no models configured

    ## Generators

    - LLM models from `models_dir/generators/`
    - Empty if no models configured

    ## Recognizers

    - ONNX models from `models_dir/recognizers/`
    - Includes GLiNER models for zero-shot recognition

    ## Rewriters

    - Seq2Seq models from `models_dir/rewriters/`
    - T5, FLAN-T5, BART, and LMQG question generation models

    ## Readers

    - Vision2Seq models from `models_dir/readers/`
    - TrOCR, Donut, Florence-2 for OCR and document understanding

    ## Transcribers

    - Speech2Seq models from `models_dir/transcribers/`
    - Whisper, Wav2Vec2, HuBERT for speech-to-text

    Models are discovered at service startup and cached.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteError | TermiteModelsResponse
    """

    return sync_detailed(
        client=client,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[TermiteError | TermiteModelsResponse]:
    r"""List available models

     Returns lists of available embedding, chunking, reranking, generator, NER, rewriter, reader, and
    transcriber models.

    ## Embedders

    - ONNX models from `models_dir/embedders/`
    - Quantized variants have `-i8` suffix

    ## Chunkers

    - Always includes \"fixed\" (built-in)
    - Plus any ONNX models from `models_dir/chunkers/`

    ## Rerankers

    - Native or ONNX rerankers from `models_dir/rerankers/`
    - `model_manifest.json` capabilities can mark late-interaction text rerankers (`late_interaction`,
    `colbert`)
    - Empty if no models configured

    ## Generators

    - LLM models from `models_dir/generators/`
    - Empty if no models configured

    ## Recognizers

    - ONNX models from `models_dir/recognizers/`
    - Includes GLiNER models for zero-shot recognition

    ## Rewriters

    - Seq2Seq models from `models_dir/rewriters/`
    - T5, FLAN-T5, BART, and LMQG question generation models

    ## Readers

    - Vision2Seq models from `models_dir/readers/`
    - TrOCR, Donut, Florence-2 for OCR and document understanding

    ## Transcribers

    - Speech2Seq models from `models_dir/transcribers/`
    - Whisper, Wav2Vec2, HuBERT for speech-to-text

    Models are discovered at service startup and cached.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteError | TermiteModelsResponse]
    """

    kwargs = _get_kwargs()

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
) -> TermiteError | TermiteModelsResponse | None:
    r"""List available models

     Returns lists of available embedding, chunking, reranking, generator, NER, rewriter, reader, and
    transcriber models.

    ## Embedders

    - ONNX models from `models_dir/embedders/`
    - Quantized variants have `-i8` suffix

    ## Chunkers

    - Always includes \"fixed\" (built-in)
    - Plus any ONNX models from `models_dir/chunkers/`

    ## Rerankers

    - Native or ONNX rerankers from `models_dir/rerankers/`
    - `model_manifest.json` capabilities can mark late-interaction text rerankers (`late_interaction`,
    `colbert`)
    - Empty if no models configured

    ## Generators

    - LLM models from `models_dir/generators/`
    - Empty if no models configured

    ## Recognizers

    - ONNX models from `models_dir/recognizers/`
    - Includes GLiNER models for zero-shot recognition

    ## Rewriters

    - Seq2Seq models from `models_dir/rewriters/`
    - T5, FLAN-T5, BART, and LMQG question generation models

    ## Readers

    - Vision2Seq models from `models_dir/readers/`
    - TrOCR, Donut, Florence-2 for OCR and document understanding

    ## Transcribers

    - Speech2Seq models from `models_dir/transcribers/`
    - Whisper, Wav2Vec2, HuBERT for speech-to-text

    Models are discovered at service startup and cached.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteError | TermiteModelsResponse
    """

    return (
        await asyncio_detailed(
            client=client,
        )
    ).parsed
