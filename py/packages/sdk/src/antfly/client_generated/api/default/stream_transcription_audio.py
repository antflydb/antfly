from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_transcription_stream_message import InferenceTranscriptionStreamMessage
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...models.stream_transcription_audio_format import StreamTranscriptionAudioFormat
from ...types import UNSET, File, Response, Unset


def _get_kwargs(
    session_id: str,
    *,
    body: File,
    format_: StreamTranscriptionAudioFormat | Unset = UNSET,
    sample_rate: int | Unset = UNSET,
    commit: bool | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    params: dict[str, Any] = {}

    json_format_: str | Unset = UNSET
    if not isinstance(format_, Unset):
        json_format_ = format_.value

    params["format"] = json_format_

    params["sample_rate"] = sample_rate

    params["commit"] = commit

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/transcription/sessions/{session_id}/stream".format(
            session_id=quote(str(session_id), safe=""),
        ),
        "params": params,
    }

    _kwargs["content"] = body.payload

    headers["Content-Type"] = "application/octet-stream"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceTranscriptionStreamMessage | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceTranscriptionStreamMessage.from_dict(response.text)

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

    if response.status_code == 503:
        response_503 = InferenceTransientCapacityError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferenceTranscriptionStreamMessage | InferenceTransientCapacityError]:
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
    body: File,
    format_: StreamTranscriptionAudioFormat | Unset = UNSET,
    sample_rate: int | Unset = UNSET,
    commit: bool | Unset = UNSET,
) -> Response[InferenceError | InferenceTranscriptionStreamMessage | InferenceTransientCapacityError]:
    """Stream raw audio into a session and receive events as they occur

     Full-duplex transcription over one request. The request body is raw
    little-endian mono PCM (`format` selects 16-bit or float32 samples at
    `sample_rate`), sent as it is captured. The server decodes as chunks
    arrive and writes `transcription.event` messages on the response while
    the upload continues. At end of body, buffered speech is finalized
    when `commit` is true (the default).

    Over HTTP/2 the body is read incrementally. Over HTTP/1.1 the body is
    read after it has fully arrived, so use `/audio` appends there for
    live results. Appends to the same session are refused with 409 while
    a stream is open.

    Args:
        session_id (str):
        format_ (StreamTranscriptionAudioFormat | Unset):
        sample_rate (int | Unset):
        commit (bool | Unset):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceTranscriptionStreamMessage | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        session_id=session_id,
        body=body,
        format_=format_,
        sample_rate=sample_rate,
        commit=commit,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: File,
    format_: StreamTranscriptionAudioFormat | Unset = UNSET,
    sample_rate: int | Unset = UNSET,
    commit: bool | Unset = UNSET,
) -> InferenceError | InferenceTranscriptionStreamMessage | InferenceTransientCapacityError | None:
    """Stream raw audio into a session and receive events as they occur

     Full-duplex transcription over one request. The request body is raw
    little-endian mono PCM (`format` selects 16-bit or float32 samples at
    `sample_rate`), sent as it is captured. The server decodes as chunks
    arrive and writes `transcription.event` messages on the response while
    the upload continues. At end of body, buffered speech is finalized
    when `commit` is true (the default).

    Over HTTP/2 the body is read incrementally. Over HTTP/1.1 the body is
    read after it has fully arrived, so use `/audio` appends there for
    live results. Appends to the same session are refused with 409 while
    a stream is open.

    Args:
        session_id (str):
        format_ (StreamTranscriptionAudioFormat | Unset):
        sample_rate (int | Unset):
        commit (bool | Unset):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceTranscriptionStreamMessage | InferenceTransientCapacityError
    """

    return sync_detailed(
        session_id=session_id,
        client=client,
        body=body,
        format_=format_,
        sample_rate=sample_rate,
        commit=commit,
    ).parsed


async def asyncio_detailed(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: File,
    format_: StreamTranscriptionAudioFormat | Unset = UNSET,
    sample_rate: int | Unset = UNSET,
    commit: bool | Unset = UNSET,
) -> Response[InferenceError | InferenceTranscriptionStreamMessage | InferenceTransientCapacityError]:
    """Stream raw audio into a session and receive events as they occur

     Full-duplex transcription over one request. The request body is raw
    little-endian mono PCM (`format` selects 16-bit or float32 samples at
    `sample_rate`), sent as it is captured. The server decodes as chunks
    arrive and writes `transcription.event` messages on the response while
    the upload continues. At end of body, buffered speech is finalized
    when `commit` is true (the default).

    Over HTTP/2 the body is read incrementally. Over HTTP/1.1 the body is
    read after it has fully arrived, so use `/audio` appends there for
    live results. Appends to the same session are refused with 409 while
    a stream is open.

    Args:
        session_id (str):
        format_ (StreamTranscriptionAudioFormat | Unset):
        sample_rate (int | Unset):
        commit (bool | Unset):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceTranscriptionStreamMessage | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        session_id=session_id,
        body=body,
        format_=format_,
        sample_rate=sample_rate,
        commit=commit,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: File,
    format_: StreamTranscriptionAudioFormat | Unset = UNSET,
    sample_rate: int | Unset = UNSET,
    commit: bool | Unset = UNSET,
) -> InferenceError | InferenceTranscriptionStreamMessage | InferenceTransientCapacityError | None:
    """Stream raw audio into a session and receive events as they occur

     Full-duplex transcription over one request. The request body is raw
    little-endian mono PCM (`format` selects 16-bit or float32 samples at
    `sample_rate`), sent as it is captured. The server decodes as chunks
    arrive and writes `transcription.event` messages on the response while
    the upload continues. At end of body, buffered speech is finalized
    when `commit` is true (the default).

    Over HTTP/2 the body is read incrementally. Over HTTP/1.1 the body is
    read after it has fully arrived, so use `/audio` appends there for
    live results. Appends to the same session are refused with 409 while
    a stream is open.

    Args:
        session_id (str):
        format_ (StreamTranscriptionAudioFormat | Unset):
        sample_rate (int | Unset):
        commit (bool | Unset):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceTranscriptionStreamMessage | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            session_id=session_id,
            client=client,
            body=body,
            format_=format_,
            sample_rate=sample_rate,
            commit=commit,
        )
    ).parsed
