from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ...client import AuthenticatedClient, Client
from ...models.changefeed_error import ChangefeedError
from ...types import Response


def _get_kwargs(
    feed_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/db/v1/changefeeds/{feed_id}".format(
            feed_id=quote(str(feed_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | ChangefeedError:
    if response.status_code == 204:
        response_204 = cast(Any, None)
        return response_204

    response_default = ChangefeedError.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | ChangefeedError]:
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
) -> Response[Any | ChangefeedError]:
    """Close a feed and release its retention grants

    Args:
        feed_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ChangefeedError]
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
) -> Any | ChangefeedError | None:
    """Close a feed and release its retention grants

    Args:
        feed_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ChangefeedError
    """

    return sync_detailed(
        feed_id=feed_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | ChangefeedError]:
    """Close a feed and release its retention grants

    Args:
        feed_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ChangefeedError]
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
) -> Any | ChangefeedError | None:
    """Close a feed and release its retention grants

    Args:
        feed_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ChangefeedError
    """

    return (
        await asyncio_detailed(
            feed_id=feed_id,
            client=client,
        )
    ).parsed
