from http import HTTPStatus
from io import BytesIO
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_generate_request import InferenceGenerateRequest
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...models.prepare_native_kv_checkpoint_x_antfly_kv_encoding import PrepareNativeKvCheckpointXAntflyKVEncoding
from ...types import File, Response, Unset


def _get_kwargs(
    *,
    body: InferenceGenerateRequest,
    x_antfly_kv_encoding: PrepareNativeKvCheckpointXAntflyKVEncoding
    | Unset = PrepareNativeKvCheckpointXAntflyKVEncoding.F16,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(x_antfly_kv_encoding, Unset):
        headers["X-Antfly-KV-Encoding"] = str(x_antfly_kv_encoding)

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/generate/context/checkpoint",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> File | InferenceError | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = File(payload=BytesIO(response.content))

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 413:
        response_413 = InferenceError.from_dict(response.json())

        return response_413

    if response.status_code == 503:
        response_503 = InferenceTransientCapacityError.from_dict(response.json())

        return response_503

    if response.status_code == 507:
        response_507 = InferenceError.from_dict(response.json())

        return response_507

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[File | InferenceError | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceGenerateRequest,
    x_antfly_kv_encoding: PrepareNativeKvCheckpointXAntflyKVEncoding
    | Unset = PrepareNativeKvCheckpointXAntflyKVEncoding.F16,
) -> Response[File | InferenceError | InferenceTransientCapacityError]:
    """Prepare a portable native KV checkpoint

     Prefills text and tool messages and fits the native attention-matching KV
    cache on the qualified Gemma 4 E4B CUDA f16 route. The request uses the
    ordinary generation prompt/template and reserves one continuation position
    within the 32768-token native checkpoint window. It does not generate
    an answer; max_tokens does not change checkpoint preparation.
    stream must be false. Native checkpoint operations default cache_dtype
    to f16 and reject other cache precisions. Draft models, media, prompt caches, compiled/graph
    execution and other context/cache policies are unsupported.

    The response is an AFATT001 envelope: JSON metadata followed by exactly
    one application/vnd.antfly.kv-checkpoint attachment. The checkpoint is
    a V3 binary file by default (V2 for f32, V4 for rotated4), containing original token positions,
    fitted global KV,
    exact local KV, saved next-token logits, loaded-model fingerprints and
    an integrity digest. It is usable only with the same model revision,
    tokenizer, chat template and KV layout. Metadata reports the actual
    global-cache savings. If safe fitting cannot meet 1.5x savings, outcome
    is unchanged_insufficient_savings and the attachment preserves the
    original exact state. This fallback is not a successful compaction.

    Consume and validate the entire envelope before publishing a checkpoint.
    A partial streamed response is never a valid checkpoint. Metadata is
    limited to 2 MiB and the checkpoint to 2 GiB. Capacity admission bounds
    concurrent native checkpoint work and returns a retryable response when busy.

    Args:
        x_antfly_kv_encoding (PrepareNativeKvCheckpointXAntflyKVEncoding | Unset):  Default:
            PrepareNativeKvCheckpointXAntflyKVEncoding.F16.
        body (InferenceGenerateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[File | InferenceError | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
        x_antfly_kv_encoding=x_antfly_kv_encoding,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceGenerateRequest,
    x_antfly_kv_encoding: PrepareNativeKvCheckpointXAntflyKVEncoding
    | Unset = PrepareNativeKvCheckpointXAntflyKVEncoding.F16,
) -> File | InferenceError | InferenceTransientCapacityError | None:
    """Prepare a portable native KV checkpoint

     Prefills text and tool messages and fits the native attention-matching KV
    cache on the qualified Gemma 4 E4B CUDA f16 route. The request uses the
    ordinary generation prompt/template and reserves one continuation position
    within the 32768-token native checkpoint window. It does not generate
    an answer; max_tokens does not change checkpoint preparation.
    stream must be false. Native checkpoint operations default cache_dtype
    to f16 and reject other cache precisions. Draft models, media, prompt caches, compiled/graph
    execution and other context/cache policies are unsupported.

    The response is an AFATT001 envelope: JSON metadata followed by exactly
    one application/vnd.antfly.kv-checkpoint attachment. The checkpoint is
    a V3 binary file by default (V2 for f32, V4 for rotated4), containing original token positions,
    fitted global KV,
    exact local KV, saved next-token logits, loaded-model fingerprints and
    an integrity digest. It is usable only with the same model revision,
    tokenizer, chat template and KV layout. Metadata reports the actual
    global-cache savings. If safe fitting cannot meet 1.5x savings, outcome
    is unchanged_insufficient_savings and the attachment preserves the
    original exact state. This fallback is not a successful compaction.

    Consume and validate the entire envelope before publishing a checkpoint.
    A partial streamed response is never a valid checkpoint. Metadata is
    limited to 2 MiB and the checkpoint to 2 GiB. Capacity admission bounds
    concurrent native checkpoint work and returns a retryable response when busy.

    Args:
        x_antfly_kv_encoding (PrepareNativeKvCheckpointXAntflyKVEncoding | Unset):  Default:
            PrepareNativeKvCheckpointXAntflyKVEncoding.F16.
        body (InferenceGenerateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        File | InferenceError | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
        x_antfly_kv_encoding=x_antfly_kv_encoding,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceGenerateRequest,
    x_antfly_kv_encoding: PrepareNativeKvCheckpointXAntflyKVEncoding
    | Unset = PrepareNativeKvCheckpointXAntflyKVEncoding.F16,
) -> Response[File | InferenceError | InferenceTransientCapacityError]:
    """Prepare a portable native KV checkpoint

     Prefills text and tool messages and fits the native attention-matching KV
    cache on the qualified Gemma 4 E4B CUDA f16 route. The request uses the
    ordinary generation prompt/template and reserves one continuation position
    within the 32768-token native checkpoint window. It does not generate
    an answer; max_tokens does not change checkpoint preparation.
    stream must be false. Native checkpoint operations default cache_dtype
    to f16 and reject other cache precisions. Draft models, media, prompt caches, compiled/graph
    execution and other context/cache policies are unsupported.

    The response is an AFATT001 envelope: JSON metadata followed by exactly
    one application/vnd.antfly.kv-checkpoint attachment. The checkpoint is
    a V3 binary file by default (V2 for f32, V4 for rotated4), containing original token positions,
    fitted global KV,
    exact local KV, saved next-token logits, loaded-model fingerprints and
    an integrity digest. It is usable only with the same model revision,
    tokenizer, chat template and KV layout. Metadata reports the actual
    global-cache savings. If safe fitting cannot meet 1.5x savings, outcome
    is unchanged_insufficient_savings and the attachment preserves the
    original exact state. This fallback is not a successful compaction.

    Consume and validate the entire envelope before publishing a checkpoint.
    A partial streamed response is never a valid checkpoint. Metadata is
    limited to 2 MiB and the checkpoint to 2 GiB. Capacity admission bounds
    concurrent native checkpoint work and returns a retryable response when busy.

    Args:
        x_antfly_kv_encoding (PrepareNativeKvCheckpointXAntflyKVEncoding | Unset):  Default:
            PrepareNativeKvCheckpointXAntflyKVEncoding.F16.
        body (InferenceGenerateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[File | InferenceError | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
        x_antfly_kv_encoding=x_antfly_kv_encoding,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceGenerateRequest,
    x_antfly_kv_encoding: PrepareNativeKvCheckpointXAntflyKVEncoding
    | Unset = PrepareNativeKvCheckpointXAntflyKVEncoding.F16,
) -> File | InferenceError | InferenceTransientCapacityError | None:
    """Prepare a portable native KV checkpoint

     Prefills text and tool messages and fits the native attention-matching KV
    cache on the qualified Gemma 4 E4B CUDA f16 route. The request uses the
    ordinary generation prompt/template and reserves one continuation position
    within the 32768-token native checkpoint window. It does not generate
    an answer; max_tokens does not change checkpoint preparation.
    stream must be false. Native checkpoint operations default cache_dtype
    to f16 and reject other cache precisions. Draft models, media, prompt caches, compiled/graph
    execution and other context/cache policies are unsupported.

    The response is an AFATT001 envelope: JSON metadata followed by exactly
    one application/vnd.antfly.kv-checkpoint attachment. The checkpoint is
    a V3 binary file by default (V2 for f32, V4 for rotated4), containing original token positions,
    fitted global KV,
    exact local KV, saved next-token logits, loaded-model fingerprints and
    an integrity digest. It is usable only with the same model revision,
    tokenizer, chat template and KV layout. Metadata reports the actual
    global-cache savings. If safe fitting cannot meet 1.5x savings, outcome
    is unchanged_insufficient_savings and the attachment preserves the
    original exact state. This fallback is not a successful compaction.

    Consume and validate the entire envelope before publishing a checkpoint.
    A partial streamed response is never a valid checkpoint. Metadata is
    limited to 2 MiB and the checkpoint to 2 GiB. Capacity admission bounds
    concurrent native checkpoint work and returns a retryable response when busy.

    Args:
        x_antfly_kv_encoding (PrepareNativeKvCheckpointXAntflyKVEncoding | Unset):  Default:
            PrepareNativeKvCheckpointXAntflyKVEncoding.F16.
        body (InferenceGenerateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        File | InferenceError | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            x_antfly_kv_encoding=x_antfly_kv_encoding,
        )
    ).parsed
