from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ...client import AuthenticatedClient, Client
from ...models.changefeed_error import ChangefeedError
from ...models.changefeed_page import ChangefeedPage
from ...types import UNSET, Response, Unset


def _get_kwargs(
    feed_id: str,
    *,
    after: str,
    limit: int | Unset = 256,
    max_bytes: int | Unset = 1048576,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["after"] = after

    params["limit"] = limit

    params["max_bytes"] = max_bytes

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/changefeeds/{feed_id}/events".format(
            feed_id=quote(str(feed_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ChangefeedError | ChangefeedPage:
    if response.status_code == 200:
        response_200 = ChangefeedPage.from_dict(response.json())

        return response_200

    if response.status_code == 409:
        response_409 = ChangefeedError.from_dict(response.json())

        return response_409

    if response.status_code == 429:
        response_429 = ChangefeedError.from_dict(response.json())

        return response_429

    response_default = ChangefeedError.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ChangefeedError | ChangefeedPage]:
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
    after: str,
    limit: int | Unset = 256,
    max_bytes: int | Unset = 1048576,
) -> Response[ChangefeedError | ChangefeedPage]:
    """Read or replay a bounded changefeed page

    Args:
        feed_id (str):
        after (str):
        limit (int | Unset):  Default: 256.
        max_bytes (int | Unset):  Default: 1048576.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedError | ChangefeedPage]
    """

    kwargs = _get_kwargs(
        feed_id=feed_id,
        after=after,
        limit=limit,
        max_bytes=max_bytes,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
    after: str,
    limit: int | Unset = 256,
    max_bytes: int | Unset = 1048576,
) -> ChangefeedError | ChangefeedPage | None:
    """Read or replay a bounded changefeed page

    Args:
        feed_id (str):
        after (str):
        limit (int | Unset):  Default: 256.
        max_bytes (int | Unset):  Default: 1048576.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedError | ChangefeedPage
    """

    return sync_detailed(
        feed_id=feed_id,
        client=client,
        after=after,
        limit=limit,
        max_bytes=max_bytes,
    ).parsed


async def asyncio_detailed(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
    after: str,
    limit: int | Unset = 256,
    max_bytes: int | Unset = 1048576,
) -> Response[ChangefeedError | ChangefeedPage]:
    """Read or replay a bounded changefeed page

    Args:
        feed_id (str):
        after (str):
        limit (int | Unset):  Default: 256.
        max_bytes (int | Unset):  Default: 1048576.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedError | ChangefeedPage]
    """

    kwargs = _get_kwargs(
        feed_id=feed_id,
        after=after,
        limit=limit,
        max_bytes=max_bytes,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
    after: str,
    limit: int | Unset = 256,
    max_bytes: int | Unset = 1048576,
) -> ChangefeedError | ChangefeedPage | None:
    """Read or replay a bounded changefeed page

    Args:
        feed_id (str):
        after (str):
        limit (int | Unset):  Default: 256.
        max_bytes (int | Unset):  Default: 1048576.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedError | ChangefeedPage
    """

    return (
        await asyncio_detailed(
            feed_id=feed_id,
            client=client,
            after=after,
            limit=limit,
            max_bytes=max_bytes,
        )
    ).parsed
