from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_transcription_session import InferenceTranscriptionSession
from ...models.inference_transcription_session_request import InferenceTranscriptionSessionRequest
from ...models.inference_transient_capacity_error import InferenceTransientCapacityError
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceTranscriptionSessionRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/transcription/sessions",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceTranscriptionSession | InferenceTransientCapacityError | None:
    if response.status_code == 200:
        response_200 = InferenceTranscriptionSession.from_dict(response.json())

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

    if response.status_code == 429:
        response_429 = InferenceError.from_dict(response.json())

        return response_429

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
) -> Response[InferenceError | InferenceTranscriptionSession | InferenceTransientCapacityError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceTranscriptionSessionRequest,
) -> Response[InferenceError | InferenceTranscriptionSession | InferenceTransientCapacityError]:
    """Open a streaming transcription session

     Creates a server-side session that accepts audio in chunks and returns
    transcript events as speech is endpointed. Append audio with
    `POST /transcription/sessions/{session_id}/audio`; each append runs
    voice activity detection over the buffered audio and returns the
    events it produced:

    - `partial`: the open speech segment decoded again. `stable_text` is
      the word prefix that agreed with the previous hypothesis and can be
      rendered as committed text.
    - `final`: a segment closed by `vad.min_silence_ms` of silence, by
      `max_segment_ms` of continuous speech, or by `commit: true`.

    Sessions expire after `ttl_seconds` without appends and are closed
    with `DELETE`.

    Args:
        body (InferenceTranscriptionSessionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceTranscriptionSession | InferenceTransientCapacityError]
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
    body: InferenceTranscriptionSessionRequest,
) -> InferenceError | InferenceTranscriptionSession | InferenceTransientCapacityError | None:
    """Open a streaming transcription session

     Creates a server-side session that accepts audio in chunks and returns
    transcript events as speech is endpointed. Append audio with
    `POST /transcription/sessions/{session_id}/audio`; each append runs
    voice activity detection over the buffered audio and returns the
    events it produced:

    - `partial`: the open speech segment decoded again. `stable_text` is
      the word prefix that agreed with the previous hypothesis and can be
      rendered as committed text.
    - `final`: a segment closed by `vad.min_silence_ms` of silence, by
      `max_segment_ms` of continuous speech, or by `commit: true`.

    Sessions expire after `ttl_seconds` without appends and are closed
    with `DELETE`.

    Args:
        body (InferenceTranscriptionSessionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceTranscriptionSession | InferenceTransientCapacityError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceTranscriptionSessionRequest,
) -> Response[InferenceError | InferenceTranscriptionSession | InferenceTransientCapacityError]:
    """Open a streaming transcription session

     Creates a server-side session that accepts audio in chunks and returns
    transcript events as speech is endpointed. Append audio with
    `POST /transcription/sessions/{session_id}/audio`; each append runs
    voice activity detection over the buffered audio and returns the
    events it produced:

    - `partial`: the open speech segment decoded again. `stable_text` is
      the word prefix that agreed with the previous hypothesis and can be
      rendered as committed text.
    - `final`: a segment closed by `vad.min_silence_ms` of silence, by
      `max_segment_ms` of continuous speech, or by `commit: true`.

    Sessions expire after `ttl_seconds` without appends and are closed
    with `DELETE`.

    Args:
        body (InferenceTranscriptionSessionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceTranscriptionSession | InferenceTransientCapacityError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceTranscriptionSessionRequest,
) -> InferenceError | InferenceTranscriptionSession | InferenceTransientCapacityError | None:
    """Open a streaming transcription session

     Creates a server-side session that accepts audio in chunks and returns
    transcript events as speech is endpointed. Append audio with
    `POST /transcription/sessions/{session_id}/audio`; each append runs
    voice activity detection over the buffered audio and returns the
    events it produced:

    - `partial`: the open speech segment decoded again. `stable_text` is
      the word prefix that agreed with the previous hypothesis and can be
      rendered as committed text.
    - `final`: a segment closed by `vad.min_silence_ms` of silence, by
      `max_segment_ms` of continuous speech, or by `commit: true`.

    Sessions expire after `ttl_seconds` without appends and are closed
    with `DELETE`.

    Args:
        body (InferenceTranscriptionSessionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceTranscriptionSession | InferenceTransientCapacityError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
