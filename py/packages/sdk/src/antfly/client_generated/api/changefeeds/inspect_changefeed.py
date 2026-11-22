from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ...client import AuthenticatedClient, Client
from ...models.changefeed_error import ChangefeedError
from ...models.changefeed_progress import ChangefeedProgress
from ...types import Response


def _get_kwargs(
    feed_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/changefeeds/{feed_id}".format(
            feed_id=quote(str(feed_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ChangefeedError | ChangefeedProgress:
    if response.status_code == 200:
        response_200 = ChangefeedProgress.from_dict(response.json())

        return response_200

    response_default = ChangefeedError.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ChangefeedError | ChangefeedProgress]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[ChangefeedError | ChangefeedProgress]:
    """Inspect a changefeed's lease and acknowledged progress

    Args:
        feed_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedError | ChangefeedProgress]
    """

    kwargs = _get_kwargs(
        feed_id=feed_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> ChangefeedError | ChangefeedProgress | None:
    """Inspect a changefeed's lease and acknowledged progress

    Args:
        feed_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedError | ChangefeedProgress
    """

    return sync_detailed(
        feed_id=feed_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[ChangefeedError | ChangefeedProgress]:
    """Inspect a changefeed's lease and acknowledged progress

    Args:
        feed_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedError | ChangefeedProgress]
    """

    kwargs = _get_kwargs(
        feed_id=feed_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> ChangefeedError | ChangefeedProgress | None:
    """Inspect a changefeed's lease and acknowledged progress

    Args:
        feed_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedError | ChangefeedProgress
    """

    return (
        await asyncio_detailed(
            feed_id=feed_id,
            client=client,
        )
    ).parsed
