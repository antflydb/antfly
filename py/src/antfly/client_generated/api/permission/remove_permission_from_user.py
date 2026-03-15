from http import HTTPStatus
from typing import Any, Optional, Union, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.resource_type import ResourceType
from ...types import UNSET, Response


def _get_kwargs(
    user_name: str,
    *,
    resource: str,
    resource_type: ResourceType,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["resource"] = resource

    json_resource_type = resource_type.value
    params["resourceType"] = json_resource_type

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": f"/users/{user_name}/permissions",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Any, Error]]:
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


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Any, Error]]:
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
    resource: str,
    resource_type: ResourceType,
) -> Response[Union[Any, Error]]:
    """Remove permission from user

     Removes a specific permission rule from a user based on resource name and type.

    Args:
        user_name (str):  Example: johndoe.
        resource (str):  Example: orders_table.
        resource_type (ResourceType): Type of the resource, e.g., table, user, or global ('*').
            Example: table.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, Error]]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        resource=resource,
        resource_type=resource_type,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    user_name: str,
    *,
    client: AuthenticatedClient,
    resource: str,
    resource_type: ResourceType,
) -> Optional[Union[Any, Error]]:
    """Remove permission from user

     Removes a specific permission rule from a user based on resource name and type.

    Args:
        user_name (str):  Example: johndoe.
        resource (str):  Example: orders_table.
        resource_type (ResourceType): Type of the resource, e.g., table, user, or global ('*').
            Example: table.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, Error]
    """

    return sync_detailed(
        user_name=user_name,
        client=client,
        resource=resource,
        resource_type=resource_type,
    ).parsed


async def asyncio_detailed(
    user_name: str,
    *,
    client: AuthenticatedClient,
    resource: str,
    resource_type: ResourceType,
) -> Response[Union[Any, Error]]:
    """Remove permission from user

     Removes a specific permission rule from a user based on resource name and type.

    Args:
        user_name (str):  Example: johndoe.
        resource (str):  Example: orders_table.
        resource_type (ResourceType): Type of the resource, e.g., table, user, or global ('*').
            Example: table.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, Error]]
    """

    kwargs = _get_kwargs(
        user_name=user_name,
        resource=resource,
        resource_type=resource_type,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    user_name: str,
    *,
    client: AuthenticatedClient,
    resource: str,
    resource_type: ResourceType,
) -> Optional[Union[Any, Error]]:
    """Remove permission from user

     Removes a specific permission rule from a user based on resource name and type.

    Args:
        user_name (str):  Example: johndoe.
        resource (str):  Example: orders_table.
        resource_type (ResourceType): Type of the resource, e.g., table, user, or global ('*').
            Example: table.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, Error]
    """

    return (
        await asyncio_detailed(
            user_name=user_name,
            client=client,
            resource=resource,
            resource_type=resource_type,
        )
    ).parsed
