from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_transcription_audio_append import InferenceTranscriptionAudioAppend
from ...models.inference_transcription_event_list import InferenceTranscriptionEventList
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import Response


def _get_kwargs(
    session_id: str,
    *,
    body: InferenceTranscriptionAudioAppend,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/transcription/sessions/{session_id}/audio".format(
            session_id=quote(str(session_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceTranscriptionEventList | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceTranscriptionEventList.from_dict(response.json())

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

    if response.status_code == 409:
        response_409 = InferenceError.from_dict(response.json())

        return response_409

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
) -> Response[InferenceError | InferenceTranscriptionEventList | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: InferenceTranscriptionAudioAppend,
) -> Response[InferenceError | InferenceTranscriptionEventList | InferenceTransientCapacityError]:
    r"""Append audio to a streaming transcription session

     Appends one chunk of audio and runs endpointing and decoding over the
    session buffer. The response lists the events produced by this
    append, in order. Appends to one session must be sequential; a
    concurrent append is rejected with 409.

    `audio` is base64. With `format: auto` (default) the bytes are a
    container the runtime can decode (WAV, Opus, MP3, FLAC, ...). With
    `format: pcm16` or `pcm_f32` the bytes are raw little-endian mono
    samples at `sample_rate`, which lets a client send microphone frames
    without re-encoding. Chunks of 250 ms to 1 s balance latency and
    decoder work.

    `commit: true` finalizes buffered speech even without trailing
    silence. It may be sent without `audio` to flush at the end of a
    recording.

    The framed attachment transport is accepted: send the JSON as the
    envelope metadata with `\"audio\": \"attachment:0\"` and the bytes as the
    single attachment.

    Args:
        session_id (str):
        body (InferenceTranscriptionAudioAppend):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceTranscriptionEventList | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        session_id=session_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: InferenceTranscriptionAudioAppend,
) -> InferenceError | InferenceTranscriptionEventList | InferenceTransientCapacityError | None:
    r"""Append audio to a streaming transcription session

     Appends one chunk of audio and runs endpointing and decoding over the
    session buffer. The response lists the events produced by this
    append, in order. Appends to one session must be sequential; a
    concurrent append is rejected with 409.

    `audio` is base64. With `format: auto` (default) the bytes are a
    container the runtime can decode (WAV, Opus, MP3, FLAC, ...). With
    `format: pcm16` or `pcm_f32` the bytes are raw little-endian mono
    samples at `sample_rate`, which lets a client send microphone frames
    without re-encoding. Chunks of 250 ms to 1 s balance latency and
    decoder work.

    `commit: true` finalizes buffered speech even without trailing
    silence. It may be sent without `audio` to flush at the end of a
    recording.

    The framed attachment transport is accepted: send the JSON as the
    envelope metadata with `\"audio\": \"attachment:0\"` and the bytes as the
    single attachment.

    Args:
        session_id (str):
        body (InferenceTranscriptionAudioAppend):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceTranscriptionEventList | InferenceTransientCapacityError
    """

    return sync_detailed(
        session_id=session_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: InferenceTranscriptionAudioAppend,
) -> Response[InferenceError | InferenceTranscriptionEventList | InferenceTransientCapacityError]:
    r"""Append audio to a streaming transcription session

     Appends one chunk of audio and runs endpointing and decoding over the
    session buffer. The response lists the events produced by this
    append, in order. Appends to one session must be sequential; a
    concurrent append is rejected with 409.

    `audio` is base64. With `format: auto` (default) the bytes are a
    container the runtime can decode (WAV, Opus, MP3, FLAC, ...). With
    `format: pcm16` or `pcm_f32` the bytes are raw little-endian mono
    samples at `sample_rate`, which lets a client send microphone frames
    without re-encoding. Chunks of 250 ms to 1 s balance latency and
    decoder work.

    `commit: true` finalizes buffered speech even without trailing
    silence. It may be sent without `audio` to flush at the end of a
    recording.

    The framed attachment transport is accepted: send the JSON as the
    envelope metadata with `\"audio\": \"attachment:0\"` and the bytes as the
    single attachment.

    Args:
        session_id (str):
        body (InferenceTranscriptionAudioAppend):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceTranscriptionEventList | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        session_id=session_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: InferenceTranscriptionAudioAppend,
) -> InferenceError | InferenceTranscriptionEventList | InferenceTransientCapacityError | None:
    r"""Append audio to a streaming transcription session

     Appends one chunk of audio and runs endpointing and decoding over the
    session buffer. The response lists the events produced by this
    append, in order. Appends to one session must be sequential; a
    concurrent append is rejected with 409.

    `audio` is base64. With `format: auto` (default) the bytes are a
    container the runtime can decode (WAV, Opus, MP3, FLAC, ...). With
    `format: pcm16` or `pcm_f32` the bytes are raw little-endian mono
    samples at `sample_rate`, which lets a client send microphone frames
    without re-encoding. Chunks of 250 ms to 1 s balance latency and
    decoder work.

    `commit: true` finalizes buffered speech even without trailing
    silence. It may be sent without `audio` to flush at the end of a
    recording.

    The framed attachment transport is accepted: send the JSON as the
    envelope metadata with `\"audio\": \"attachment:0\"` and the bytes as the
    single attachment.

    Args:
        session_id (str):
        body (InferenceTranscriptionAudioAppend):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceTranscriptionEventList | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            session_id=session_id,
            client=client,
            body=body,
        )
    ).parsed
