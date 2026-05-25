from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...types import UNSET, Response


def _get_kwargs(
    user_name: str,
    *,
    role: str,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["role"] = role

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/auth/v1/users/{user_name}/roles".format(
            user_name=quote(str(user_name), safe=""),
        ),
        "params": params,
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
    user_name: str,
    *,
    client: AuthenticatedClient,
    role: str,
) -> Response[Any | Error]:
    """Remove role from user

     Removes a direct role or group membership from a user.

    Args:
        user_name (str):  Example: johndoe.
        role (str):  Example: role:tenant_reader.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        role=role,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    user_name: str,
    *,
    client: AuthenticatedClient,
    role: str,
) -> Any | Error | None:
    """Remove role from user

     Removes a direct role or group membership from a user.

    Args:
        user_name (str):  Example: johndoe.
        role (str):  Example: role:tenant_reader.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error
    """

    return sync_detailed(
        user_name=user_name,
        client=client,
        role=role,
    ).parsed


async def asyncio_detailed(
    user_name: str,
    *,
    client: AuthenticatedClient,
    role: str,
) -> Response[Any | Error]:
    """Remove role from user

     Removes a direct role or group membership from a user.

    Args:
        user_name (str):  Example: johndoe.
        role (str):  Example: role:tenant_reader.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        role=role,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    user_name: str,
    *,
    client: AuthenticatedClient,
    role: str,
) -> Any | Error | None:
    """Remove role from user

     Removes a direct role or group membership from a user.

    Args:
        user_name (str):  Example: johndoe.
        role (str):  Example: role:tenant_reader.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error
    """

    return (
        await asyncio_detailed(
            user_name=user_name,
            client=client,
            role=role,
        )
    ).parsed
