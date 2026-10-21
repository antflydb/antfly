from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.entity_curation_result import EntityCurationResult
from ...models.entity_distinct_request import EntityDistinctRequest
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    table_name: str,
    key: str,
    *,
    body: EntityDistinctRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/entities/{key}:distinct".format(
            table_name=quote(str(table_name), safe=""),
            key=quote(str(key), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | EntityCurationResult | Error | None:
    if response.status_code == 200:
        response_200 = EntityCurationResult.from_dict(response.json())

        return response_200

    if response.status_code == 202:
        response_202 = EntityCurationResult.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | EntityCurationResult | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    body: EntityDistinctRequest,
) -> Response[Any | EntityCurationResult | Error]:
    """pinEntitiesDistinct

    Args:
        table_name (str):
        key (str):
        body (EntityDistinctRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | EntityCurationResult | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    body: EntityDistinctRequest,
) -> Any | EntityCurationResult | Error | None:
    """pinEntitiesDistinct

    Args:
        table_name (str):
        key (str):
        body (EntityDistinctRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | EntityCurationResult | Error
    """

    return sync_detailed(
        table_name=table_name,
        key=key,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    body: EntityDistinctRequest,
) -> Response[Any | EntityCurationResult | Error]:
    """pinEntitiesDistinct

    Args:
        table_name (str):
        key (str):
        body (EntityDistinctRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | EntityCurationResult | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    body: EntityDistinctRequest,
) -> Any | EntityCurationResult | Error | None:
    """pinEntitiesDistinct

    Args:
        table_name (str):
        key (str):
        body (EntityDistinctRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | EntityCurationResult | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            key=key,
            client=client,
            body=body,
        )
    ).parsed
