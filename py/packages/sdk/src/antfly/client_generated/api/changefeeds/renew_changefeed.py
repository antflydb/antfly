from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ...client import AuthenticatedClient, Client
from ...models.changefeed_error import ChangefeedError
from ...models.changefeed_lease import ChangefeedLease
from ...types import Response


def _get_kwargs(
    feed_id: str,
    *,
    body: ChangefeedLease,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/changefeeds/{feed_id}/renew".format(
            feed_id=quote(str(feed_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ChangefeedError | ChangefeedLease:
    if response.status_code == 200:
        response_200 = ChangefeedLease.from_dict(response.json())

        return response_200

    if response.status_code == 409:
        response_409 = ChangefeedError.from_dict(response.json())

        return response_409

    response_default = ChangefeedError.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ChangefeedError | ChangefeedLease]:
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
    body: ChangefeedLease,
) -> Response[ChangefeedError | ChangefeedLease]:
    """Extend the retention lease for a streaming feed

    Args:
        feed_id (str):
        body (ChangefeedLease):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedError | ChangefeedLease]
    """

    kwargs = _get_kwargs(
        feed_id=feed_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: ChangefeedLease,
) -> ChangefeedError | ChangefeedLease | None:
    """Extend the retention lease for a streaming feed

    Args:
        feed_id (str):
        body (ChangefeedLease):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedError | ChangefeedLease
    """

    return sync_detailed(
        feed_id=feed_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: ChangefeedLease,
) -> Response[ChangefeedError | ChangefeedLease]:
    """Extend the retention lease for a streaming feed

    Args:
        feed_id (str):
        body (ChangefeedLease):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedError | ChangefeedLease]
    """

    kwargs = _get_kwargs(
        feed_id=feed_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    feed_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: ChangefeedLease,
) -> ChangefeedError | ChangefeedLease | None:
    """Extend the retention lease for a streaming feed

    Args:
        feed_id (str):
        body (ChangefeedLease):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedError | ChangefeedLease
    """

    return (
        await asyncio_detailed(
            feed_id=feed_id,
            client=client,
            body=body,
        )
    ).parsed
