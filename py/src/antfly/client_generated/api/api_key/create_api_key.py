from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_key_with_secret import ApiKeyWithSecret
from ...models.create_api_key_request import CreateApiKeyRequest
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    user_name: str,
    *,
    body: CreateApiKeyRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/users/{user_name}/api-keys",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ApiKeyWithSecret, Error]]:
    if response.status_code == 201:
        response_201 = ApiKeyWithSecret.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = Error.from_dict(response.json())

        return response_403

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
) -> Response[Union[ApiKeyWithSecret, Error]]:
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
    body: CreateApiKeyRequest,
) -> Response[Union[ApiKeyWithSecret, Error]]:
    """Create a new API key

     Creates a new API key for the specified user. The cleartext secret is returned only in this
    response.

    Args:
        user_name (str):  Example: johndoe.
        body (CreateApiKeyRequest): Request to create a new API key.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ApiKeyWithSecret, Error]]
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
    body: CreateApiKeyRequest,
) -> Optional[Union[ApiKeyWithSecret, Error]]:
    """Create a new API key

     Creates a new API key for the specified user. The cleartext secret is returned only in this
    response.

    Args:
        user_name (str):  Example: johndoe.
        body (CreateApiKeyRequest): Request to create a new API key.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ApiKeyWithSecret, Error]
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
    body: CreateApiKeyRequest,
) -> Response[Union[ApiKeyWithSecret, Error]]:
    """Create a new API key

     Creates a new API key for the specified user. The cleartext secret is returned only in this
    response.

    Args:
        user_name (str):  Example: johndoe.
        body (CreateApiKeyRequest): Request to create a new API key.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ApiKeyWithSecret, Error]]
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
    body: CreateApiKeyRequest,
) -> Optional[Union[ApiKeyWithSecret, Error]]:
    """Create a new API key

     Creates a new API key for the specified user. The cleartext secret is returned only in this
    response.

    Args:
        user_name (str):  Example: johndoe.
        body (CreateApiKeyRequest): Request to create a new API key.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ApiKeyWithSecret, Error]
    """

    return (
        await asyncio_detailed(
            user_name=user_name,
            client=client,
            body=body,
        )
    ).parsed
