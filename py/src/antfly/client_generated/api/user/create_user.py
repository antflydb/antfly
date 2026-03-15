from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_user_request import CreateUserRequest
from ...models.error import Error
from ...models.user import User
from ...types import Response


def _get_kwargs(
    user_name: str,
    *,
    body: CreateUserRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/users/{user_name}",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, User]]:
    if response.status_code == 201:
        response_201 = User.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Error, User]]:
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
    body: CreateUserRequest,
) -> Response[Union[Error, User]]:
    """Create a new user

     Creates a new user with the given username and password. Username in path takes precedence.

    Args:
        user_name (str):  Example: johndoe.
        body (CreateUserRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, User]]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    user_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateUserRequest,
) -> Optional[Union[Error, User]]:
    """Create a new user

     Creates a new user with the given username and password. Username in path takes precedence.

    Args:
        user_name (str):  Example: johndoe.
        body (CreateUserRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, User]
    """

    return sync_detailed(
        user_name=user_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    user_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateUserRequest,
) -> Response[Union[Error, User]]:
    """Create a new user

     Creates a new user with the given username and password. Username in path takes precedence.

    Args:
        user_name (str):  Example: johndoe.
        body (CreateUserRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, User]]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    user_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateUserRequest,
) -> Optional[Union[Error, User]]:
    """Create a new user

     Creates a new user with the given username and password. Username in path takes precedence.

    Args:
        user_name (str):  Example: johndoe.
        body (CreateUserRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, User]
    """

    return (
        await asyncio_detailed(
            user_name=user_name,
            client=client,
            body=body,
        )
    ).parsed
