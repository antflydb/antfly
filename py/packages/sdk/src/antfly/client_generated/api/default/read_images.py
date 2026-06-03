from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_read_request import InferenceReadRequest
from ...models.inference_read_response import InferenceReadResponse
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceReadRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/read",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceReadResponse | None:
    if response.status_code == 200:
        response_200 = InferenceReadResponse.from_dict(response.json())

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
) -> Response[InferenceError | InferenceReadResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceReadRequest,
) -> Response[InferenceError | InferenceReadResponse]:
    """Read text from images (OCR/document understanding)

     Extracts text from images using Vision2Seq models like TrOCR, Donut, Florence-2, or Pix2Struct.

    ## Models

    Models are auto-discovered from `models_dir/readers/` at startup.
    Use the `/ai/v1/models` endpoint to list available models.

    - **TrOCR**: Pure OCR for printed/handwritten text
    - **Donut**: Document understanding with structured output (receipts, forms)
    - **Florence-2**: Multi-task vision model (OCR, captioning, VQA)
    - **Pix2Struct**: Visual question answering with natural-language prompts
    - **Moondream**: Decoder-only vision-language reader that can return text plus optional flattened
    fields

    ## Task Prompts

    Some models support task prompts for different extraction modes:

    - **Donut CORD**: `<s_cord-v2>` for receipt parsing
    - **Donut DocVQA**: `<s_docvqa><s_question>...</s_question><s_answer>` for visual QA
    - **Florence-2 OCR**: `<OCR>` for text extraction
    - **Florence-2 Caption**: `<CAPTION>` for image description
    - **Pix2Struct**: natural-language questions like `What type of document is this?`
    - **Moondream**: natural-language prompts like `Describe this image.`

    Args:
        body (InferenceReadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceReadResponse]
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
    body: InferenceReadRequest,
) -> InferenceError | InferenceReadResponse | None:
    """Read text from images (OCR/document understanding)

     Extracts text from images using Vision2Seq models like TrOCR, Donut, Florence-2, or Pix2Struct.

    ## Models

    Models are auto-discovered from `models_dir/readers/` at startup.
    Use the `/ai/v1/models` endpoint to list available models.

    - **TrOCR**: Pure OCR for printed/handwritten text
    - **Donut**: Document understanding with structured output (receipts, forms)
    - **Florence-2**: Multi-task vision model (OCR, captioning, VQA)
    - **Pix2Struct**: Visual question answering with natural-language prompts
    - **Moondream**: Decoder-only vision-language reader that can return text plus optional flattened
    fields

    ## Task Prompts

    Some models support task prompts for different extraction modes:

    - **Donut CORD**: `<s_cord-v2>` for receipt parsing
    - **Donut DocVQA**: `<s_docvqa><s_question>...</s_question><s_answer>` for visual QA
    - **Florence-2 OCR**: `<OCR>` for text extraction
    - **Florence-2 Caption**: `<CAPTION>` for image description
    - **Pix2Struct**: natural-language questions like `What type of document is this?`
    - **Moondream**: natural-language prompts like `Describe this image.`

    Args:
        body (InferenceReadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceReadResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceReadRequest,
) -> Response[InferenceError | InferenceReadResponse]:
    """Read text from images (OCR/document understanding)

     Extracts text from images using Vision2Seq models like TrOCR, Donut, Florence-2, or Pix2Struct.

    ## Models

    Models are auto-discovered from `models_dir/readers/` at startup.
    Use the `/ai/v1/models` endpoint to list available models.

    - **TrOCR**: Pure OCR for printed/handwritten text
    - **Donut**: Document understanding with structured output (receipts, forms)
    - **Florence-2**: Multi-task vision model (OCR, captioning, VQA)
    - **Pix2Struct**: Visual question answering with natural-language prompts
    - **Moondream**: Decoder-only vision-language reader that can return text plus optional flattened
    fields

    ## Task Prompts

    Some models support task prompts for different extraction modes:

    - **Donut CORD**: `<s_cord-v2>` for receipt parsing
    - **Donut DocVQA**: `<s_docvqa><s_question>...</s_question><s_answer>` for visual QA
    - **Florence-2 OCR**: `<OCR>` for text extraction
    - **Florence-2 Caption**: `<CAPTION>` for image description
    - **Pix2Struct**: natural-language questions like `What type of document is this?`
    - **Moondream**: natural-language prompts like `Describe this image.`

    Args:
        body (InferenceReadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceReadResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceReadRequest,
) -> InferenceError | InferenceReadResponse | None:
    """Read text from images (OCR/document understanding)

     Extracts text from images using Vision2Seq models like TrOCR, Donut, Florence-2, or Pix2Struct.

    ## Models

    Models are auto-discovered from `models_dir/readers/` at startup.
    Use the `/ai/v1/models` endpoint to list available models.

    - **TrOCR**: Pure OCR for printed/handwritten text
    - **Donut**: Document understanding with structured output (receipts, forms)
    - **Florence-2**: Multi-task vision model (OCR, captioning, VQA)
    - **Pix2Struct**: Visual question answering with natural-language prompts
    - **Moondream**: Decoder-only vision-language reader that can return text plus optional flattened
    fields

    ## Task Prompts

    Some models support task prompts for different extraction modes:

    - **Donut CORD**: `<s_cord-v2>` for receipt parsing
    - **Donut DocVQA**: `<s_docvqa><s_question>...</s_question><s_answer>` for visual QA
    - **Florence-2 OCR**: `<OCR>` for text extraction
    - **Florence-2 Caption**: `<CAPTION>` for image description
    - **Pix2Struct**: natural-language questions like `What type of document is this?`
    - **Moondream**: natural-language prompts like `Describe this image.`

    Args:
        body (InferenceReadRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceReadResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
