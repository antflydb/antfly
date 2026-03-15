from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.success_message import SuccessMessage
from ...models.update_password_request import UpdatePasswordRequest
from ...types import Response


def _get_kwargs(
    user_name: str,
    *,
    body: UpdatePasswordRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": f"/users/{user_name}/password",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, SuccessMessage]]:
    if response.status_code == 200:
        response_200 = SuccessMessage.from_dict(response.json())

        return response_200

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
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Error, SuccessMessage]]:
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
    body: UpdatePasswordRequest,
) -> Response[Union[Error, SuccessMessage]]:
    """Update user password

     Updates the password for a specific user.

    Args:
        user_name (str):  Example: johndoe.
        body (UpdatePasswordRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, SuccessMessage]]
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
    body: UpdatePasswordRequest,
) -> Optional[Union[Error, SuccessMessage]]:
    """Update user password

     Updates the password for a specific user.

    Args:
        user_name (str):  Example: johndoe.
        body (UpdatePasswordRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, SuccessMessage]
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
    body: UpdatePasswordRequest,
) -> Response[Union[Error, SuccessMessage]]:
    """Update user password

     Updates the password for a specific user.

    Args:
        user_name (str):  Example: johndoe.
        body (UpdatePasswordRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, SuccessMessage]]
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
    body: UpdatePasswordRequest,
) -> Optional[Union[Error, SuccessMessage]]:
    """Update user password

     Updates the password for a specific user.

    Args:
        user_name (str):  Example: johndoe.
        body (UpdatePasswordRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, SuccessMessage]
    """

    return (
        await asyncio_detailed(
            user_name=user_name,
            client=client,
            body=body,
        )
    ).parsed
