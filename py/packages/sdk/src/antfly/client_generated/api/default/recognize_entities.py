from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_recognize_request import InferenceRecognizeRequest
from ...models.inference_recognize_response import InferenceRecognizeResponse
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceRecognizeRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/recognize",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceRecognizeResponse | None:
    if response.status_code == 200:
        response_200 = InferenceRecognizeResponse.from_dict(response.json())

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
) -> Response[InferenceError | InferenceRecognizeResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRecognizeRequest,
) -> Response[InferenceError | InferenceRecognizeResponse]:
    r"""Recognize named entities

     Recognizes named entities (persons, organizations, locations, etc.) from text using ONNX recognition
    models.

    ## Entity Types

    Standard CoNLL entity types:
    - **PER**: Person names (e.g., \"John Smith\")
    - **ORG**: Organizations (e.g., \"Google\", \"Apple Inc.\")
    - **LOC**: Locations (e.g., \"New York\", \"France\")
    - **MISC**: Miscellaneous entities

    ## Models

    - Models are auto-discovered from `models_dir/recognizers/`
    - Supports quantized variants (model_i8.onnx)
    - Compatible with HuggingFace BERT-based recognition models
    - GLiNER models support custom entity labels via the `labels` parameter

    Args:
        body (InferenceRecognizeRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceRecognizeResponse]
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
    body: InferenceRecognizeRequest,
) -> InferenceError | InferenceRecognizeResponse | None:
    r"""Recognize named entities

     Recognizes named entities (persons, organizations, locations, etc.) from text using ONNX recognition
    models.

    ## Entity Types

    Standard CoNLL entity types:
    - **PER**: Person names (e.g., \"John Smith\")
    - **ORG**: Organizations (e.g., \"Google\", \"Apple Inc.\")
    - **LOC**: Locations (e.g., \"New York\", \"France\")
    - **MISC**: Miscellaneous entities

    ## Models

    - Models are auto-discovered from `models_dir/recognizers/`
    - Supports quantized variants (model_i8.onnx)
    - Compatible with HuggingFace BERT-based recognition models
    - GLiNER models support custom entity labels via the `labels` parameter

    Args:
        body (InferenceRecognizeRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceRecognizeResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRecognizeRequest,
) -> Response[InferenceError | InferenceRecognizeResponse]:
    r"""Recognize named entities

     Recognizes named entities (persons, organizations, locations, etc.) from text using ONNX recognition
    models.

    ## Entity Types

    Standard CoNLL entity types:
    - **PER**: Person names (e.g., \"John Smith\")
    - **ORG**: Organizations (e.g., \"Google\", \"Apple Inc.\")
    - **LOC**: Locations (e.g., \"New York\", \"France\")
    - **MISC**: Miscellaneous entities

    ## Models

    - Models are auto-discovered from `models_dir/recognizers/`
    - Supports quantized variants (model_i8.onnx)
    - Compatible with HuggingFace BERT-based recognition models
    - GLiNER models support custom entity labels via the `labels` parameter

    Args:
        body (InferenceRecognizeRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceRecognizeResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceRecognizeRequest,
) -> InferenceError | InferenceRecognizeResponse | None:
    r"""Recognize named entities

     Recognizes named entities (persons, organizations, locations, etc.) from text using ONNX recognition
    models.

    ## Entity Types

    Standard CoNLL entity types:
    - **PER**: Person names (e.g., \"John Smith\")
    - **ORG**: Organizations (e.g., \"Google\", \"Apple Inc.\")
    - **LOC**: Locations (e.g., \"New York\", \"France\")
    - **MISC**: Miscellaneous entities

    ## Models

    - Models are auto-discovered from `models_dir/recognizers/`
    - Supports quantized variants (model_i8.onnx)
    - Compatible with HuggingFace BERT-based recognition models
    - GLiNER models support custom entity labels via the `labels` parameter

    Args:
        body (InferenceRecognizeRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceRecognizeResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
