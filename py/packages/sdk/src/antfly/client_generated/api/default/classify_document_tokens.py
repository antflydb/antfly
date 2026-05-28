from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_document_token_classification_request import InferenceDocumentTokenClassificationRequest
from ...models.inference_document_token_classification_response import InferenceDocumentTokenClassificationResponse
from ...models.inference_error import InferenceError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceDocumentTokenClassificationRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/classify/document_tokens",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceDocumentTokenClassificationResponse | InferenceError | None:
    if response.status_code == 200:
        response_200 = InferenceDocumentTokenClassificationResponse.from_dict(response.json())

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
) -> Response[InferenceDocumentTokenClassificationResponse | InferenceError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDocumentTokenClassificationRequest,
) -> Response[InferenceDocumentTokenClassificationResponse | InferenceError]:
    """Document token classification

     Runs a native document token classification head against caller-provided OCR tokens and bounding
    boxes.

    This endpoint serves `layoutdoc_token_head.safetensors` artifacts produced by the
    Zig finetuning stack. It reconstructs the same compact 6-dimensional token feature
    vector used by the training code for each OCR token.

    ## Current Scope

    - Native `layoutdoc_token_head` checkpoints only
    - Caller-provided OCR token text and bboxes
    - Caller must supply labels in model output order

    ## Not Yet Included

    - OCR extraction inside this endpoint
    - Sequence-head image features
    - Label vocab discovery from checkpoint artifacts

    Args:
        body (InferenceDocumentTokenClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceDocumentTokenClassificationResponse | InferenceError]
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
    body: InferenceDocumentTokenClassificationRequest,
) -> InferenceDocumentTokenClassificationResponse | InferenceError | None:
    """Document token classification

     Runs a native document token classification head against caller-provided OCR tokens and bounding
    boxes.

    This endpoint serves `layoutdoc_token_head.safetensors` artifacts produced by the
    Zig finetuning stack. It reconstructs the same compact 6-dimensional token feature
    vector used by the training code for each OCR token.

    ## Current Scope

    - Native `layoutdoc_token_head` checkpoints only
    - Caller-provided OCR token text and bboxes
    - Caller must supply labels in model output order

    ## Not Yet Included

    - OCR extraction inside this endpoint
    - Sequence-head image features
    - Label vocab discovery from checkpoint artifacts

    Args:
        body (InferenceDocumentTokenClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceDocumentTokenClassificationResponse | InferenceError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDocumentTokenClassificationRequest,
) -> Response[InferenceDocumentTokenClassificationResponse | InferenceError]:
    """Document token classification

     Runs a native document token classification head against caller-provided OCR tokens and bounding
    boxes.

    This endpoint serves `layoutdoc_token_head.safetensors` artifacts produced by the
    Zig finetuning stack. It reconstructs the same compact 6-dimensional token feature
    vector used by the training code for each OCR token.

    ## Current Scope

    - Native `layoutdoc_token_head` checkpoints only
    - Caller-provided OCR token text and bboxes
    - Caller must supply labels in model output order

    ## Not Yet Included

    - OCR extraction inside this endpoint
    - Sequence-head image features
    - Label vocab discovery from checkpoint artifacts

    Args:
        body (InferenceDocumentTokenClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceDocumentTokenClassificationResponse | InferenceError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDocumentTokenClassificationRequest,
) -> InferenceDocumentTokenClassificationResponse | InferenceError | None:
    """Document token classification

     Runs a native document token classification head against caller-provided OCR tokens and bounding
    boxes.

    This endpoint serves `layoutdoc_token_head.safetensors` artifacts produced by the
    Zig finetuning stack. It reconstructs the same compact 6-dimensional token feature
    vector used by the training code for each OCR token.

    ## Current Scope

    - Native `layoutdoc_token_head` checkpoints only
    - Caller-provided OCR token text and bboxes
    - Caller must supply labels in model output order

    ## Not Yet Included

    - OCR extraction inside this endpoint
    - Sequence-head image features
    - Label vocab discovery from checkpoint artifacts

    Args:
        body (InferenceDocumentTokenClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceDocumentTokenClassificationResponse | InferenceError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
