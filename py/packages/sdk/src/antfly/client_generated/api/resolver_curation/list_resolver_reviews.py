from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.resolver_review_page import ResolverReviewPage
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    resolver_name: str,
    *,
    cursor: str | Unset = UNSET,
    limit: int | Unset = 64,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["limit"] = limit

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/resolvers/{resolver_name}/reviews".format(
            table_name=quote(str(table_name), safe=""),
            resolver_name=quote(str(resolver_name), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | ResolverReviewPage | None:
    if response.status_code == 200:
        response_200 = ResolverReviewPage.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 429:
        response_429 = cast(Any, None)
        return response_429

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | ResolverReviewPage]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    resolver_name: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    limit: int | Unset = 64,
) -> Response[Any | Error | ResolverReviewPage]:
    """list resolver reviews

    Args:
        table_name (str):
        resolver_name (str):
        cursor (str | Unset):
        limit (int | Unset):  Default: 64.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | ResolverReviewPage]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        resolver_name=resolver_name,
        cursor=cursor,
        limit=limit,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    resolver_name: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    limit: int | Unset = 64,
) -> Any | Error | ResolverReviewPage | None:
    """list resolver reviews

    Args:
        table_name (str):
        resolver_name (str):
        cursor (str | Unset):
        limit (int | Unset):  Default: 64.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | ResolverReviewPage
    """

    return sync_detailed(
        table_name=table_name,
        resolver_name=resolver_name,
        client=client,
        cursor=cursor,
        limit=limit,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    resolver_name: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    limit: int | Unset = 64,
) -> Response[Any | Error | ResolverReviewPage]:
    """list resolver reviews

    Args:
        table_name (str):
        resolver_name (str):
        cursor (str | Unset):
        limit (int | Unset):  Default: 64.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | ResolverReviewPage]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        resolver_name=resolver_name,
        cursor=cursor,
        limit=limit,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    resolver_name: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    limit: int | Unset = 64,
) -> Any | Error | ResolverReviewPage | None:
    """list resolver reviews

    Args:
        table_name (str):
        resolver_name (str):
        cursor (str | Unset):
        limit (int | Unset):  Default: 64.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | ResolverReviewPage
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            resolver_name=resolver_name,
            client=client,
            cursor=cursor,
            limit=limit,
        )
    ).parsed
