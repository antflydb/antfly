from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.entity_consumer_progress_page import EntityConsumerProgressPage
from ...models.error import Error
from ...types import UNSET, Response, Unset


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    key: str,
    *,
    limit: int | Unset = 64,
    cursor: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["limit"] = limit

    params["cursor"] = cursor

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/entities/{key}/progress".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            table_name=quote(str(table_name), safe=""),
            key=quote(str(key), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | EntityConsumerProgressPage | Error | None:
    if response.status_code == 200:
        response_200 = EntityConsumerProgressPage.from_dict(response.json())

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
) -> Response[Any | EntityConsumerProgressPage | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    limit: int | Unset = 64,
    cursor: str | Unset = UNSET,
) -> Response[Any | EntityConsumerProgressPage | Error]:
    """List a bounded page of entity consumer replay progress

     Pages enumerate resolver consumers at one catalog cut. Each page reads current native namespace
    heads and verifies them again after observing replay. A changed catalog or binding invalidates the
    cursor. Applied describes an individual consumer, not consumers on other pages.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        key (str):
        limit (int | Unset):  Default: 64.
        cursor (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | EntityConsumerProgressPage | Error]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        key=key,
        limit=limit,
        cursor=cursor,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    limit: int | Unset = 64,
    cursor: str | Unset = UNSET,
) -> Any | EntityConsumerProgressPage | Error | None:
    """List a bounded page of entity consumer replay progress

     Pages enumerate resolver consumers at one catalog cut. Each page reads current native namespace
    heads and verifies them again after observing replay. A changed catalog or binding invalidates the
    cursor. Applied describes an individual consumer, not consumers on other pages.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        key (str):
        limit (int | Unset):  Default: 64.
        cursor (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | EntityConsumerProgressPage | Error
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        key=key,
        client=client,
        limit=limit,
        cursor=cursor,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    limit: int | Unset = 64,
    cursor: str | Unset = UNSET,
) -> Response[Any | EntityConsumerProgressPage | Error]:
    """List a bounded page of entity consumer replay progress

     Pages enumerate resolver consumers at one catalog cut. Each page reads current native namespace
    heads and verifies them again after observing replay. A changed catalog or binding invalidates the
    cursor. Applied describes an individual consumer, not consumers on other pages.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        key (str):
        limit (int | Unset):  Default: 64.
        cursor (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | EntityConsumerProgressPage | Error]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        key=key,
        limit=limit,
        cursor=cursor,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    limit: int | Unset = 64,
    cursor: str | Unset = UNSET,
) -> Any | EntityConsumerProgressPage | Error | None:
    """List a bounded page of entity consumer replay progress

     Pages enumerate resolver consumers at one catalog cut. Each page reads current native namespace
    heads and verifies them again after observing replay. A changed catalog or binding invalidates the
    cursor. Applied describes an individual consumer, not consumers on other pages.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        key (str):
        limit (int | Unset):  Default: 64.
        cursor (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | EntityConsumerProgressPage | Error
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            key=key,
            client=client,
            limit=limit,
            cursor=cursor,
        )
    ).parsed
