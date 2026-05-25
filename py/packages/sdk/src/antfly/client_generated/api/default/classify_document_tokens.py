from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.termite_document_token_classification_request import TermiteDocumentTokenClassificationRequest
from ...models.termite_document_token_classification_response import TermiteDocumentTokenClassificationResponse
from ...models.termite_error import TermiteError
from ...types import Response


def _get_kwargs(
    *,
    body: TermiteDocumentTokenClassificationRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ml/v1/classify/document_tokens",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> TermiteDocumentTokenClassificationResponse | TermiteError | None:
    if response.status_code == 200:
        response_200 = TermiteDocumentTokenClassificationResponse.from_dict(response.json())

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
) -> Response[TermiteDocumentTokenClassificationResponse | TermiteError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteDocumentTokenClassificationRequest,
) -> Response[TermiteDocumentTokenClassificationResponse | TermiteError]:
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
        body (TermiteDocumentTokenClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteDocumentTokenClassificationResponse | TermiteError]
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
    body: TermiteDocumentTokenClassificationRequest,
) -> TermiteDocumentTokenClassificationResponse | TermiteError | None:
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
        body (TermiteDocumentTokenClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteDocumentTokenClassificationResponse | TermiteError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteDocumentTokenClassificationRequest,
) -> Response[TermiteDocumentTokenClassificationResponse | TermiteError]:
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
        body (TermiteDocumentTokenClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteDocumentTokenClassificationResponse | TermiteError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteDocumentTokenClassificationRequest,
) -> TermiteDocumentTokenClassificationResponse | TermiteError | None:
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
        body (TermiteDocumentTokenClassificationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteDocumentTokenClassificationResponse | TermiteError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
