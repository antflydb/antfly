from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    user_name: str,
    key_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/auth/v1/users/{user_name}/api-keys/{key_id}".format(
            user_name=quote(str(user_name), safe=""),
            key_id=quote(str(key_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | Error | None:
    if response.status_code == 204:
        response_204 = cast(Any, None)
        return response_204

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
    user_name: str,
    key_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | Error]:
    """Delete an API key

     Permanently deletes the specified API key. Subsequent requests using this key will be rejected.

    Args:
        user_name (str):  Example: johndoe.
        key_id (str):  Example: aBcDeFgHiJkLmNoPqRsT.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        key_id=key_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    user_name: str,
    key_id: str,
    *,
    client: AuthenticatedClient,
) -> Any | Error | None:
    """Delete an API key

     Permanently deletes the specified API key. Subsequent requests using this key will be rejected.

    Args:
        user_name (str):  Example: johndoe.
        key_id (str):  Example: aBcDeFgHiJkLmNoPqRsT.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error
    """

    return sync_detailed(
        user_name=user_name,
        key_id=key_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    user_name: str,
    key_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | Error]:
    """Delete an API key

     Permanently deletes the specified API key. Subsequent requests using this key will be rejected.

    Args:
        user_name (str):  Example: johndoe.
        key_id (str):  Example: aBcDeFgHiJkLmNoPqRsT.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        key_id=key_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    user_name: str,
    key_id: str,
    *,
    client: AuthenticatedClient,
) -> Any | Error | None:
    """Delete an API key

     Permanently deletes the specified API key. Subsequent requests using this key will be rejected.

    Args:
        user_name (str):  Example: johndoe.
        key_id (str):  Example: aBcDeFgHiJkLmNoPqRsT.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error
    """

    return (
        await asyncio_detailed(
            user_name=user_name,
            key_id=key_id,
            client=client,
        )
    ).parsed
