from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_document_classification_request import InferenceDocumentClassificationRequest
from ...models.inference_document_classification_response import InferenceDocumentClassificationResponse
from ...models.inference_error import InferenceError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceDocumentClassificationRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/classify/document",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceDocumentClassificationResponse | InferenceError | None:
    if response.status_code == 200:
        response_200 = InferenceDocumentClassificationResponse.from_dict(response.json())

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
) -> Response[InferenceDocumentClassificationResponse | InferenceError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDocumentClassificationRequest,
) -> Response[InferenceDocumentClassificationResponse | InferenceError]:
    """Document classification

     Runs a native document classification head against a document image.

    This endpoint serves `layoutdoc_sequence_head.safetensors` artifacts produced by
    the Zig finetuning stack. It currently reconstructs the same compact visual/layout
    feature vector used by the training code and applies the saved sequence head.

    ## Current Scope

    - Native `layoutdoc_sequence_head` checkpoints only
    - Local image path input only
    - Caller must supply labels in model output order
    - Supports JPEG, PNG, and JPEG2000 image files

    ## Not Yet Included

    - Served `layoutdoc_token_head`
    - OCR token / bbox request shapes
    - Label vocab discovery from checkpoint artifacts

    Args:
        body (InferenceDocumentClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceDocumentClassificationResponse | InferenceError]
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
    body: InferenceDocumentClassificationRequest,
) -> InferenceDocumentClassificationResponse | InferenceError | None:
    """Document classification

     Runs a native document classification head against a document image.

    This endpoint serves `layoutdoc_sequence_head.safetensors` artifacts produced by
    the Zig finetuning stack. It currently reconstructs the same compact visual/layout
    feature vector used by the training code and applies the saved sequence head.

    ## Current Scope

    - Native `layoutdoc_sequence_head` checkpoints only
    - Local image path input only
    - Caller must supply labels in model output order
    - Supports JPEG, PNG, and JPEG2000 image files

    ## Not Yet Included

    - Served `layoutdoc_token_head`
    - OCR token / bbox request shapes
    - Label vocab discovery from checkpoint artifacts

    Args:
        body (InferenceDocumentClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceDocumentClassificationResponse | InferenceError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDocumentClassificationRequest,
) -> Response[InferenceDocumentClassificationResponse | InferenceError]:
    """Document classification

     Runs a native document classification head against a document image.

    This endpoint serves `layoutdoc_sequence_head.safetensors` artifacts produced by
    the Zig finetuning stack. It currently reconstructs the same compact visual/layout
    feature vector used by the training code and applies the saved sequence head.

    ## Current Scope

    - Native `layoutdoc_sequence_head` checkpoints only
    - Local image path input only
    - Caller must supply labels in model output order
    - Supports JPEG, PNG, and JPEG2000 image files

    ## Not Yet Included

    - Served `layoutdoc_token_head`
    - OCR token / bbox request shapes
    - Label vocab discovery from checkpoint artifacts

    Args:
        body (InferenceDocumentClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceDocumentClassificationResponse | InferenceError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDocumentClassificationRequest,
) -> InferenceDocumentClassificationResponse | InferenceError | None:
    """Document classification

     Runs a native document classification head against a document image.

    This endpoint serves `layoutdoc_sequence_head.safetensors` artifacts produced by
    the Zig finetuning stack. It currently reconstructs the same compact visual/layout
    feature vector used by the training code and applies the saved sequence head.

    ## Current Scope

    - Native `layoutdoc_sequence_head` checkpoints only
    - Local image path input only
    - Caller must supply labels in model output order
    - Supports JPEG, PNG, and JPEG2000 image files

    ## Not Yet Included

    - Served `layoutdoc_token_head`
    - OCR token / bbox request shapes
    - Label vocab discovery from checkpoint artifacts

    Args:
        body (InferenceDocumentClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceDocumentClassificationResponse | InferenceError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
