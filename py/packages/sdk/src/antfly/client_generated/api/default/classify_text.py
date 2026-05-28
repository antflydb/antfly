from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_classify_request import InferenceClassifyRequest
from ...models.inference_classify_response import InferenceClassifyResponse
from ...models.inference_error import InferenceError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceClassifyRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/classify",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceClassifyResponse | InferenceError | None:
    if response.status_code == 200:
        response_200 = InferenceClassifyResponse.from_dict(response.json())

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceClassifyResponse | InferenceError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceClassifyRequest,
) -> Response[InferenceClassifyResponse | InferenceError]:
    r"""Zero-shot text classification

     Classifies text into arbitrary categories using NLI-based zero-shot classification models.

    ## How It Works

    Zero-shot classification uses Natural Language Inference (NLI) to classify text
    without requiring training data for the specific categories. The model determines
    how well a text \"entails\" each candidate label.

    ## Models

    - Models are auto-discovered from `models_dir/classifiers/`
    - Supports multilingual models like mDeBERTa-mnli-xnli
    - Compatible with HuggingFace NLI/MNLI models exported to ONNX

    ## Use Cases

    - **Sentiment Analysis**: Classify as positive/negative/neutral
    - **Topic Classification**: Categorize by topic without training
    - **Intent Detection**: Identify user intents from text
    - **Content Moderation**: Detect inappropriate content types

    ## Multilingual Support

    The mDeBERTa-mnli-xnli model supports 100+ languages. You can classify text
    in any supported language using labels in that language.

    Args:
        body (InferenceClassifyRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceClassifyResponse | InferenceError]
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
    body: InferenceClassifyRequest,
) -> InferenceClassifyResponse | InferenceError | None:
    r"""Zero-shot text classification

     Classifies text into arbitrary categories using NLI-based zero-shot classification models.

    ## How It Works

    Zero-shot classification uses Natural Language Inference (NLI) to classify text
    without requiring training data for the specific categories. The model determines
    how well a text \"entails\" each candidate label.

    ## Models

    - Models are auto-discovered from `models_dir/classifiers/`
    - Supports multilingual models like mDeBERTa-mnli-xnli
    - Compatible with HuggingFace NLI/MNLI models exported to ONNX

    ## Use Cases

    - **Sentiment Analysis**: Classify as positive/negative/neutral
    - **Topic Classification**: Categorize by topic without training
    - **Intent Detection**: Identify user intents from text
    - **Content Moderation**: Detect inappropriate content types

    ## Multilingual Support

    The mDeBERTa-mnli-xnli model supports 100+ languages. You can classify text
    in any supported language using labels in that language.

    Args:
        body (InferenceClassifyRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceClassifyResponse | InferenceError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceClassifyRequest,
) -> Response[InferenceClassifyResponse | InferenceError]:
    r"""Zero-shot text classification

     Classifies text into arbitrary categories using NLI-based zero-shot classification models.

    ## How It Works

    Zero-shot classification uses Natural Language Inference (NLI) to classify text
    without requiring training data for the specific categories. The model determines
    how well a text \"entails\" each candidate label.

    ## Models

    - Models are auto-discovered from `models_dir/classifiers/`
    - Supports multilingual models like mDeBERTa-mnli-xnli
    - Compatible with HuggingFace NLI/MNLI models exported to ONNX

    ## Use Cases

    - **Sentiment Analysis**: Classify as positive/negative/neutral
    - **Topic Classification**: Categorize by topic without training
    - **Intent Detection**: Identify user intents from text
    - **Content Moderation**: Detect inappropriate content types

    ## Multilingual Support

    The mDeBERTa-mnli-xnli model supports 100+ languages. You can classify text
    in any supported language using labels in that language.

    Args:
        body (InferenceClassifyRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceClassifyResponse | InferenceError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceClassifyRequest,
) -> InferenceClassifyResponse | InferenceError | None:
    r"""Zero-shot text classification

     Classifies text into arbitrary categories using NLI-based zero-shot classification models.

    ## How It Works

    Zero-shot classification uses Natural Language Inference (NLI) to classify text
    without requiring training data for the specific categories. The model determines
    how well a text \"entails\" each candidate label.

    ## Models

    - Models are auto-discovered from `models_dir/classifiers/`
    - Supports multilingual models like mDeBERTa-mnli-xnli
    - Compatible with HuggingFace NLI/MNLI models exported to ONNX

    ## Use Cases

    - **Sentiment Analysis**: Classify as positive/negative/neutral
    - **Topic Classification**: Categorize by topic without training
    - **Intent Detection**: Identify user intents from text
    - **Content Moderation**: Detect inappropriate content types

    ## Multilingual Support

    The mDeBERTa-mnli-xnli model supports 100+ languages. You can classify text
    in any supported language using labels in that language.

    Args:
        body (InferenceClassifyRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceClassifyResponse | InferenceError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
