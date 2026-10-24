from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.memory_error import MemoryError_
from ...models.memory_result import MemoryResult
from ...types import Response, Unset


def _get_kwargs(
    space: str,
    id: str,
    *,
    x_antfly_database: str | Unset = "default",
    x_antfly_namespace: str | Unset = "public",
    x_antfly_memory_branch: str | Unset = "",
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(x_antfly_database, Unset):
        headers["X-Antfly-Database"] = x_antfly_database

    if not isinstance(x_antfly_namespace, Unset):
        headers["X-Antfly-Namespace"] = x_antfly_namespace

    if not isinstance(x_antfly_memory_branch, Unset):
        headers["X-Antfly-Memory-Branch"] = x_antfly_memory_branch

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/memory/v1/spaces/{space}/entities/{id}".format(
            space=quote(str(space), safe=""),
            id=quote(str(id), safe=""),
        ),
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> MemoryError_ | MemoryResult | None:
    if response.status_code == 200:
        response_200 = MemoryResult.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = MemoryError_.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = MemoryError_.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = MemoryError_.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = MemoryError_.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = MemoryError_.from_dict(response.json())

        return response_409

    if response.status_code == 413:
        response_413 = MemoryError_.from_dict(response.json())

        return response_413

    if response.status_code == 422:
        response_422 = MemoryError_.from_dict(response.json())

        return response_422

    if response.status_code == 429:
        response_429 = MemoryError_.from_dict(response.json())

        return response_429

    if response.status_code == 503:
        response_503 = MemoryError_.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[MemoryError_ | MemoryResult]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    space: str,
    id: str,
    *,
    client: AuthenticatedClient | Client,
    x_antfly_database: str | Unset = "default",
    x_antfly_namespace: str | Unset = "public",
    x_antfly_memory_branch: str | Unset = "",
) -> Response[MemoryError_ | MemoryResult]:
    """
    Args:
        space (str):
        id (str):
        x_antfly_database (str | Unset):  Default: 'default'.
        x_antfly_namespace (str | Unset):  Default: 'public'.
        x_antfly_memory_branch (str | Unset):  Default: ''.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MemoryError_ | MemoryResult]
    """

    kwargs = _get_kwargs(
        space=space,
        id=id,
        x_antfly_database=x_antfly_database,
        x_antfly_namespace=x_antfly_namespace,
        x_antfly_memory_branch=x_antfly_memory_branch,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    space: str,
    id: str,
    *,
    client: AuthenticatedClient | Client,
    x_antfly_database: str | Unset = "default",
    x_antfly_namespace: str | Unset = "public",
    x_antfly_memory_branch: str | Unset = "",
) -> MemoryError_ | MemoryResult | None:
    """
    Args:
        space (str):
        id (str):
        x_antfly_database (str | Unset):  Default: 'default'.
        x_antfly_namespace (str | Unset):  Default: 'public'.
        x_antfly_memory_branch (str | Unset):  Default: ''.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MemoryError_ | MemoryResult
    """

    return sync_detailed(
        space=space,
        id=id,
        client=client,
        x_antfly_database=x_antfly_database,
        x_antfly_namespace=x_antfly_namespace,
        x_antfly_memory_branch=x_antfly_memory_branch,
    ).parsed


async def asyncio_detailed(
    space: str,
    id: str,
    *,
    client: AuthenticatedClient | Client,
    x_antfly_database: str | Unset = "default",
    x_antfly_namespace: str | Unset = "public",
    x_antfly_memory_branch: str | Unset = "",
) -> Response[MemoryError_ | MemoryResult]:
    """
    Args:
        space (str):
        id (str):
        x_antfly_database (str | Unset):  Default: 'default'.
        x_antfly_namespace (str | Unset):  Default: 'public'.
        x_antfly_memory_branch (str | Unset):  Default: ''.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MemoryError_ | MemoryResult]
    """

    kwargs = _get_kwargs(
        space=space,
        id=id,
        x_antfly_database=x_antfly_database,
        x_antfly_namespace=x_antfly_namespace,
        x_antfly_memory_branch=x_antfly_memory_branch,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    space: str,
    id: str,
    *,
    client: AuthenticatedClient | Client,
    x_antfly_database: str | Unset = "default",
    x_antfly_namespace: str | Unset = "public",
    x_antfly_memory_branch: str | Unset = "",
) -> MemoryError_ | MemoryResult | None:
    """
    Args:
        space (str):
        id (str):
        x_antfly_database (str | Unset):  Default: 'default'.
        x_antfly_namespace (str | Unset):  Default: 'public'.
        x_antfly_memory_branch (str | Unset):  Default: ''.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MemoryError_ | MemoryResult
    """

    return (
        await asyncio_detailed(
            space=space,
            id=id,
            client=client,
            x_antfly_database=x_antfly_database,
            x_antfly_namespace=x_antfly_namespace,
            x_antfly_memory_branch=x_antfly_memory_branch,
        )
    ).parsed
