from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...types import UNSET, Response, Unset


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    *,
    prefix: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["prefix"] = prefix

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | None:
    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_name: str,
    namespace_name: str,
    *,
    client: AuthenticatedClient,
    prefix: str | Unset = UNSET,
) -> Response[Error]:
    """List tables in namespace

     Lists table catalog objects under an explicit database and namespace.

    Args:
        database_name (str):
        namespace_name (str):
        prefix (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        prefix=prefix,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    *,
    client: AuthenticatedClient,
    prefix: str | Unset = UNSET,
) -> Error | None:
    """List tables in namespace

     Lists table catalog objects under an explicit database and namespace.

    Args:
        database_name (str):
        namespace_name (str):
        prefix (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        client=client,
        prefix=prefix,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    *,
    client: AuthenticatedClient,
    prefix: str | Unset = UNSET,
) -> Response[Error]:
    """List tables in namespace

     Lists table catalog objects under an explicit database and namespace.

    Args:
        database_name (str):
        namespace_name (str):
        prefix (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        prefix=prefix,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    *,
    client: AuthenticatedClient,
    prefix: str | Unset = UNSET,
) -> Error | None:
    """List tables in namespace

     Lists table catalog objects under an explicit database and namespace.

    Args:
        database_name (str):
        namespace_name (str):
        prefix (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            client=client,
            prefix=prefix,
        )
    ).parsed
