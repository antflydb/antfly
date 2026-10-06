from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.chat_gpt_outcome import ChatGPTOutcome
from ...types import Response


def _get_kwargs(
    attempt_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/connections/chatgpt/attempts/{attempt_id}".format(
            attempt_id=quote(str(attempt_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | ChatGPTOutcome | None:
    if response.status_code == 200:
        response_200 = ChatGPTOutcome.from_dict(response.json())

        return response_200

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | ChatGPTOutcome]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    attempt_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | ChatGPTOutcome]:
    """getChatGPTAttempt

    Args:
        attempt_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ChatGPTOutcome]
    """

    kwargs = _get_kwargs(
        attempt_id=attempt_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    attempt_id: str,
    *,
    client: AuthenticatedClient,
) -> Any | ChatGPTOutcome | None:
    """getChatGPTAttempt

    Args:
        attempt_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ChatGPTOutcome
    """

    return sync_detailed(
        attempt_id=attempt_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    attempt_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | ChatGPTOutcome]:
    """getChatGPTAttempt

    Args:
        attempt_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ChatGPTOutcome]
    """

    kwargs = _get_kwargs(
        attempt_id=attempt_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    attempt_id: str,
    *,
    client: AuthenticatedClient,
) -> Any | ChatGPTOutcome | None:
    """getChatGPTAttempt

    Args:
        attempt_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ChatGPTOutcome
    """

    return (
        await asyncio_detailed(
            attempt_id=attempt_id,
            client=client,
        )
    ).parsed
