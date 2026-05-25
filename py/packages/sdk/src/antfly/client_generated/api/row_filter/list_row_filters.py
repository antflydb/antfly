from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.row_filter_entry import RowFilterEntry
from ...types import Response


def _get_kwargs(
    user_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/auth/v1/users/{user_name}/row-filters".format(
            user_name=quote(str(user_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | list[RowFilterEntry] | None:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = RowFilterEntry.from_dict(response_200_item_data)

            response_200.append(response_200_item)

        return response_200

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


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | list[RowFilterEntry]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    user_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | list[RowFilterEntry]]:
    """List row filters for a user

     Returns all row filter policies for the specified user.

    Args:
        user_name (str):  Example: johndoe.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | list[RowFilterEntry]]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    user_name: str,
    *,
    client: AuthenticatedClient,
) -> Error | list[RowFilterEntry] | None:
    """List row filters for a user

     Returns all row filter policies for the specified user.

    Args:
        user_name (str):  Example: johndoe.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | list[RowFilterEntry]
    """

    return sync_detailed(
        user_name=user_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    user_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | list[RowFilterEntry]]:
    """List row filters for a user

     Returns all row filter policies for the specified user.

    Args:
        user_name (str):  Example: johndoe.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | list[RowFilterEntry]]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    user_name: str,
    *,
    client: AuthenticatedClient,
) -> Error | list[RowFilterEntry] | None:
    """List row filters for a user

     Returns all row filter policies for the specified user.

    Args:
        user_name (str):  Example: johndoe.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | list[RowFilterEntry]
    """

    return (
        await asyncio_detailed(
            user_name=user_name,
            client=client,
        )
    ).parsed
