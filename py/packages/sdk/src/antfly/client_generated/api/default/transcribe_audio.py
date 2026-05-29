from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_transcribe_request import InferenceTranscribeRequest
from ...models.inference_transcribe_response import InferenceTranscribeResponse
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceTranscribeRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/transcribe",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceTranscribeResponse | None:
    if response.status_code == 200:
        response_200 = InferenceTranscribeResponse.from_dict(response.json())

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
) -> Response[InferenceError | InferenceTranscribeResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceTranscribeRequest,
) -> Response[InferenceError | InferenceTranscribeResponse]:
    r"""Transcribe audio to text (speech-to-text)

     Transcribes audio to text using Speech2Seq models like Whisper, Wav2Vec2, or HuBERT.

    ## Models

    Models are auto-discovered from `models_dir/transcribers/` at startup.
    Use the `/api/models` endpoint to list available models.

    - **Whisper**: OpenAI's Whisper models (multilingual, automatic language detection)
    - **Wav2Vec2**: Facebook's Wav2Vec 2.0 models (English-focused)
    - **HuBERT**: Facebook's HuBERT models (self-supervised)

    ## Audio Input

    Audio data should be base64-encoded. Supported formats depend on the model:
    - WAV (recommended - raw PCM)
    - MP3
    - FLAC
    - M4A/AAC

    ## Example

    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"audio\": \"UklGRi...\"
    }
    ```

    With language hint:
    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"audio\": \"UklGRi...\",
      \"language\": \"en\"
    }
    ```

    Args:
        body (InferenceTranscribeRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceTranscribeResponse]
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
    body: InferenceTranscribeRequest,
) -> InferenceError | InferenceTranscribeResponse | None:
    r"""Transcribe audio to text (speech-to-text)

     Transcribes audio to text using Speech2Seq models like Whisper, Wav2Vec2, or HuBERT.

    ## Models

    Models are auto-discovered from `models_dir/transcribers/` at startup.
    Use the `/api/models` endpoint to list available models.

    - **Whisper**: OpenAI's Whisper models (multilingual, automatic language detection)
    - **Wav2Vec2**: Facebook's Wav2Vec 2.0 models (English-focused)
    - **HuBERT**: Facebook's HuBERT models (self-supervised)

    ## Audio Input

    Audio data should be base64-encoded. Supported formats depend on the model:
    - WAV (recommended - raw PCM)
    - MP3
    - FLAC
    - M4A/AAC

    ## Example

    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"audio\": \"UklGRi...\"
    }
    ```

    With language hint:
    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"audio\": \"UklGRi...\",
      \"language\": \"en\"
    }
    ```

    Args:
        body (InferenceTranscribeRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceTranscribeResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceTranscribeRequest,
) -> Response[InferenceError | InferenceTranscribeResponse]:
    r"""Transcribe audio to text (speech-to-text)

     Transcribes audio to text using Speech2Seq models like Whisper, Wav2Vec2, or HuBERT.

    ## Models

    Models are auto-discovered from `models_dir/transcribers/` at startup.
    Use the `/api/models` endpoint to list available models.

    - **Whisper**: OpenAI's Whisper models (multilingual, automatic language detection)
    - **Wav2Vec2**: Facebook's Wav2Vec 2.0 models (English-focused)
    - **HuBERT**: Facebook's HuBERT models (self-supervised)

    ## Audio Input

    Audio data should be base64-encoded. Supported formats depend on the model:
    - WAV (recommended - raw PCM)
    - MP3
    - FLAC
    - M4A/AAC

    ## Example

    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"audio\": \"UklGRi...\"
    }
    ```

    With language hint:
    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"audio\": \"UklGRi...\",
      \"language\": \"en\"
    }
    ```

    Args:
        body (InferenceTranscribeRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceTranscribeResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceTranscribeRequest,
) -> InferenceError | InferenceTranscribeResponse | None:
    r"""Transcribe audio to text (speech-to-text)

     Transcribes audio to text using Speech2Seq models like Whisper, Wav2Vec2, or HuBERT.

    ## Models

    Models are auto-discovered from `models_dir/transcribers/` at startup.
    Use the `/api/models` endpoint to list available models.

    - **Whisper**: OpenAI's Whisper models (multilingual, automatic language detection)
    - **Wav2Vec2**: Facebook's Wav2Vec 2.0 models (English-focused)
    - **HuBERT**: Facebook's HuBERT models (self-supervised)

    ## Audio Input

    Audio data should be base64-encoded. Supported formats depend on the model:
    - WAV (recommended - raw PCM)
    - MP3
    - FLAC
    - M4A/AAC

    ## Example

    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"audio\": \"UklGRi...\"
    }
    ```

    With language hint:
    ```json
    {
      \"model\": \"openai/whisper-tiny\",
      \"audio\": \"UklGRi...\",
      \"language\": \"en\"
    }
    ```

    Args:
        body (InferenceTranscribeRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceTranscribeResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
