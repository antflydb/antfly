from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.permission import Permission
from ...models.success_message import SuccessMessage
from ...types import Response


def _get_kwargs(
    user_name: str,
    *,
    body: Permission,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/auth/v1/users/{user_name}/permissions".format(
            user_name=quote(str(user_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | SuccessMessage | None:
    if response.status_code == 201:
        response_201 = SuccessMessage.from_dict(response.json())

        return response_201

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


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | SuccessMessage]:
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
    body: Permission,
) -> Response[Error | SuccessMessage]:
    """Add permission to user

     Adds a new permission to a specific user.

    Args:
        user_name (str):  Example: johndoe.
        body (Permission):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | SuccessMessage]
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
    body: Permission,
) -> Error | SuccessMessage | None:
    """Add permission to user

     Adds a new permission to a specific user.

    Args:
        user_name (str):  Example: johndoe.
        body (Permission):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | SuccessMessage
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
    body: Permission,
) -> Response[Error | SuccessMessage]:
    """Add permission to user

     Adds a new permission to a specific user.

    Args:
        user_name (str):  Example: johndoe.
        body (Permission):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | SuccessMessage]
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
    body: Permission,
) -> Error | SuccessMessage | None:
    """Add permission to user

     Adds a new permission to a specific user.

    Args:
        user_name (str):  Example: johndoe.
        body (Permission):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | SuccessMessage
    """

    return (
        await asyncio_detailed(
            user_name=user_name,
            client=client,
            body=body,
        )
    ).parsed
