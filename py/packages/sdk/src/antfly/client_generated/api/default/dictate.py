from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_dictate_request import InferenceDictateRequest
from ...models.inference_dictate_response import InferenceDictateResponse
from ...models.inference_error import InferenceError
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceDictateRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/dictate",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceDictateResponse | InferenceError | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceDictateResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 404:
        response_404 = InferenceError.from_dict(response.json())

        return response_404

    if response.status_code == 413:
        response_413 = InferenceError.from_dict(response.json())

        return response_413

    if response.status_code == 500:
        response_500 = InferenceError.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = InferenceTransientCapacityError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceDictateResponse | InferenceError | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDictateRequest,
) -> Response[InferenceDictateResponse | InferenceError | InferenceTransientCapacityError]:
    r"""Dictate speech into clean written text

     Push-to-talk dictation. Transcribes one recorded clip with a Whisper
    transcriber, then rewrites the transcript as clean written text with a
    generator model: fillers, false starts, and repeated words are removed,
    punctuation and paragraphing are added, and preferred spellings from
    `dictionary` are applied. Clips longer than the 30 s Whisper window
    are transcribed in windows cut at the quietest pause near the boundary.

    Set `cleanup_model` to the generator that rewrites the transcript.
    Without it, or with `style: verbatim`, the response carries the raw
    transcript and no generation runs.

    The framed attachment transport is accepted: send the JSON as the
    envelope metadata with `\"audio\": \"attachment:0\"` and the clip as the
    single attachment.

    With `stream: true` the response is Server-Sent Events. The stream
    emits one `dictation.transcript` event as soon as transcription
    finishes, then `dictation.delta` events with cleaned-text tokens,
    then `dictation.completed` with the full cleaned text, then `[DONE]`.

    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"cleanup_model\": \"ggml-org/gemma-4-E4B-it-GGUF\",
      \"audio\": \"UklGRi...\",
      \"dictionary\": [\"Antfly\", \"Colony\"],
      \"context\": \"reply in a Slack thread\",
      \"stream\": true
    }
    ```

    Args:
        body (InferenceDictateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceDictateResponse | InferenceError | InferenceTransientCapacityError]
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
    body: InferenceDictateRequest,
) -> InferenceDictateResponse | InferenceError | InferenceTransientCapacityError | None:
    r"""Dictate speech into clean written text

     Push-to-talk dictation. Transcribes one recorded clip with a Whisper
    transcriber, then rewrites the transcript as clean written text with a
    generator model: fillers, false starts, and repeated words are removed,
    punctuation and paragraphing are added, and preferred spellings from
    `dictionary` are applied. Clips longer than the 30 s Whisper window
    are transcribed in windows cut at the quietest pause near the boundary.

    Set `cleanup_model` to the generator that rewrites the transcript.
    Without it, or with `style: verbatim`, the response carries the raw
    transcript and no generation runs.

    The framed attachment transport is accepted: send the JSON as the
    envelope metadata with `\"audio\": \"attachment:0\"` and the clip as the
    single attachment.

    With `stream: true` the response is Server-Sent Events. The stream
    emits one `dictation.transcript` event as soon as transcription
    finishes, then `dictation.delta` events with cleaned-text tokens,
    then `dictation.completed` with the full cleaned text, then `[DONE]`.

    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"cleanup_model\": \"ggml-org/gemma-4-E4B-it-GGUF\",
      \"audio\": \"UklGRi...\",
      \"dictionary\": [\"Antfly\", \"Colony\"],
      \"context\": \"reply in a Slack thread\",
      \"stream\": true
    }
    ```

    Args:
        body (InferenceDictateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceDictateResponse | InferenceError | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDictateRequest,
) -> Response[InferenceDictateResponse | InferenceError | InferenceTransientCapacityError]:
    r"""Dictate speech into clean written text

     Push-to-talk dictation. Transcribes one recorded clip with a Whisper
    transcriber, then rewrites the transcript as clean written text with a
    generator model: fillers, false starts, and repeated words are removed,
    punctuation and paragraphing are added, and preferred spellings from
    `dictionary` are applied. Clips longer than the 30 s Whisper window
    are transcribed in windows cut at the quietest pause near the boundary.

    Set `cleanup_model` to the generator that rewrites the transcript.
    Without it, or with `style: verbatim`, the response carries the raw
    transcript and no generation runs.

    The framed attachment transport is accepted: send the JSON as the
    envelope metadata with `\"audio\": \"attachment:0\"` and the clip as the
    single attachment.

    With `stream: true` the response is Server-Sent Events. The stream
    emits one `dictation.transcript` event as soon as transcription
    finishes, then `dictation.delta` events with cleaned-text tokens,
    then `dictation.completed` with the full cleaned text, then `[DONE]`.

    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"cleanup_model\": \"ggml-org/gemma-4-E4B-it-GGUF\",
      \"audio\": \"UklGRi...\",
      \"dictionary\": [\"Antfly\", \"Colony\"],
      \"context\": \"reply in a Slack thread\",
      \"stream\": true
    }
    ```

    Args:
        body (InferenceDictateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceDictateResponse | InferenceError | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceDictateRequest,
) -> InferenceDictateResponse | InferenceError | InferenceTransientCapacityError | None:
    r"""Dictate speech into clean written text

     Push-to-talk dictation. Transcribes one recorded clip with a Whisper
    transcriber, then rewrites the transcript as clean written text with a
    generator model: fillers, false starts, and repeated words are removed,
    punctuation and paragraphing are added, and preferred spellings from
    `dictionary` are applied. Clips longer than the 30 s Whisper window
    are transcribed in windows cut at the quietest pause near the boundary.

    Set `cleanup_model` to the generator that rewrites the transcript.
    Without it, or with `style: verbatim`, the response carries the raw
    transcript and no generation runs.

    The framed attachment transport is accepted: send the JSON as the
    envelope metadata with `\"audio\": \"attachment:0\"` and the clip as the
    single attachment.

    With `stream: true` the response is Server-Sent Events. The stream
    emits one `dictation.transcript` event as soon as transcription
    finishes, then `dictation.delta` events with cleaned-text tokens,
    then `dictation.completed` with the full cleaned text, then `[DONE]`.

    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"cleanup_model\": \"ggml-org/gemma-4-E4B-it-GGUF\",
      \"audio\": \"UklGRi...\",
      \"dictionary\": [\"Antfly\", \"Colony\"],
      \"context\": \"reply in a Slack thread\",
      \"stream\": true
    }
    ```

    Args:
        body (InferenceDictateRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceDictateResponse | InferenceError | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
