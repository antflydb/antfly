from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    tablespace_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/db/v1/tablespaces/{tablespace_name}".format(
            tablespace_name=quote(str(tablespace_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | Error | None:
    if response.status_code == 204:
        response_204 = cast(Any, None)
        return response_204

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


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Any | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | Error]:
    """Drop tablespace

     Drops a tablespace catalog object. Table placement by tablespace is fail-closed until native
    placement planning consumes tablespace policy.

    Args:
        tablespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error]
    """

    kwargs = _get_kwargs(
        tablespace_name=tablespace_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Any | Error | None:
    """Drop tablespace

     Drops a tablespace catalog object. Table placement by tablespace is fail-closed until native
    placement planning consumes tablespace policy.

    Args:
        tablespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error
    """

    return sync_detailed(
        tablespace_name=tablespace_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | Error]:
    """Drop tablespace

     Drops a tablespace catalog object. Table placement by tablespace is fail-closed until native
    placement planning consumes tablespace policy.

    Args:
        tablespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error]
    """

    kwargs = _get_kwargs(
        tablespace_name=tablespace_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
) -> Any | Error | None:
    """Drop tablespace

     Drops a tablespace catalog object. Table placement by tablespace is fail-closed until native
    placement planning consumes tablespace policy.

    Args:
        tablespace_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error
    """

    return (
        await asyncio_detailed(
            tablespace_name=tablespace_name,
            client=client,
        )
    ).parsed
