from http import HTTPStatus
from typing import Any

import httpx

from ...client import AuthenticatedClient, Client
from ...models.changefeed_create import ChangefeedCreate
from ...models.changefeed_created import ChangefeedCreated
from ...models.changefeed_error import ChangefeedError
from ...types import Response


def _get_kwargs(
    *,
    body: ChangefeedCreate,
    idempotency_key: str,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    headers["Idempotency-Key"] = idempotency_key

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/changefeeds",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ChangefeedCreated | ChangefeedError:
    if response.status_code == 201:
        response_201 = ChangefeedCreated.from_dict(response.json())

        return response_201

    response_default = ChangefeedError.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ChangefeedCreated | ChangefeedError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: ChangefeedCreate,
    idempotency_key: str,
) -> Response[ChangefeedCreated | ChangefeedError]:
    """Create or resume enrollment for a durable changefeed

    Args:
        idempotency_key (str):
        body (ChangefeedCreate):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedCreated | ChangefeedError]
    """

    kwargs = _get_kwargs(
        body=body,
        idempotency_key=idempotency_key,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: ChangefeedCreate,
    idempotency_key: str,
) -> ChangefeedCreated | ChangefeedError | None:
    """Create or resume enrollment for a durable changefeed

    Args:
        idempotency_key (str):
        body (ChangefeedCreate):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedCreated | ChangefeedError
    """

    return sync_detailed(
        client=client,
        body=body,
        idempotency_key=idempotency_key,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: ChangefeedCreate,
    idempotency_key: str,
) -> Response[ChangefeedCreated | ChangefeedError]:
    """Create or resume enrollment for a durable changefeed

    Args:
        idempotency_key (str):
        body (ChangefeedCreate):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedCreated | ChangefeedError]
    """

    kwargs = _get_kwargs(
        body=body,
        idempotency_key=idempotency_key,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: ChangefeedCreate,
    idempotency_key: str,
) -> ChangefeedCreated | ChangefeedError | None:
    """Create or resume enrollment for a durable changefeed

    Args:
        idempotency_key (str):
        body (ChangefeedCreate):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedCreated | ChangefeedError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            idempotency_key=idempotency_key,
        )
    ).parsed
