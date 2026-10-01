from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_transcription_stream_message import InferenceTranscriptionStreamMessage
from ...types import Response


def _get_kwargs(
    session_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/ai/v1/transcription/sessions/{session_id}/events".format(
            session_id=quote(str(session_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceTranscriptionStreamMessage | None:
    if response.status_code == 200:
        response_200 = InferenceTranscriptionStreamMessage.from_dict(response.text)

        return response_200

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 404:
        response_404 = InferenceError.from_dict(response.json())

        return response_404

    if response.status_code == 503:
        response_503 = InferenceError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferenceTranscriptionStreamMessage]:
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
) -> Response[InferenceError | InferenceTranscriptionStreamMessage]:
    """Subscribe to a session's transcript events

     Long-lived Server-Sent Events stream that pushes every `partial` and
    `final` event the session produces, whether they came from
    `POST .../audio` appends or a `POST .../stream` upload. Clients that
    append from one connection and render from another use this instead
    of reading the append responses. A `ping` is sent after 15 s of
    silence. The stream ends with `session.closed` and `[DONE]` when the
    session is deleted or expires.

    Args:
        session_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceTranscriptionStreamMessage]
    """

    kwargs = _get_kwargs(
        session_id=session_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> InferenceError | InferenceTranscriptionStreamMessage | None:
    """Subscribe to a session's transcript events

     Long-lived Server-Sent Events stream that pushes every `partial` and
    `final` event the session produces, whether they came from
    `POST .../audio` appends or a `POST .../stream` upload. Clients that
    append from one connection and render from another use this instead
    of reading the append responses. A `ping` is sent after 15 s of
    silence. The stream ends with `session.closed` and `[DONE]` when the
    session is deleted or expires.

    Args:
        session_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceTranscriptionStreamMessage
    """

    return sync_detailed(
        session_id=session_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[InferenceError | InferenceTranscriptionStreamMessage]:
    """Subscribe to a session's transcript events

     Long-lived Server-Sent Events stream that pushes every `partial` and
    `final` event the session produces, whether they came from
    `POST .../audio` appends or a `POST .../stream` upload. Clients that
    append from one connection and render from another use this instead
    of reading the append responses. A `ping` is sent after 15 s of
    silence. The stream ends with `session.closed` and `[DONE]` when the
    session is deleted or expires.

    Args:
        session_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceTranscriptionStreamMessage]
    """

    kwargs = _get_kwargs(
        session_id=session_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> InferenceError | InferenceTranscriptionStreamMessage | None:
    """Subscribe to a session's transcript events

     Long-lived Server-Sent Events stream that pushes every `partial` and
    `final` event the session produces, whether they came from
    `POST .../audio` appends or a `POST .../stream` upload. Clients that
    append from one connection and render from another use this instead
    of reading the append responses. A `ping` is sent after 15 s of
    silence. The stream ends with `session.closed` and `[DONE]` when the
    session is deleted or expires.

    Args:
        session_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceTranscriptionStreamMessage
    """

    return (
        await asyncio_detailed(
            session_id=session_id,
            client=client,
        )
    ).parsed
