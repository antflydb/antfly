from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.rename_catalog_resource_request import RenameCatalogResourceRequest
from ...types import Response


def _get_kwargs(
    tablespace_name: str,
    *,
    body: RenameCatalogResourceRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tablespaces/{tablespace_name}/rename".format(
            tablespace_name=quote(str(tablespace_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | None:
    if response.status_code == 204:
        return None

    if response.status_code == 400:
        return None

    if response.status_code == 404:
        return None

    if response.status_code == 409:
        return None

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Any]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
    body: RenameCatalogResourceRequest,
) -> Response[Any]:
    """Rename catalog resource

    Args:
        tablespace_name (str):
        body (RenameCatalogResourceRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        tablespace_name=tablespace_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


async def asyncio_detailed(
    tablespace_name: str,
    *,
    client: AuthenticatedClient,
    body: RenameCatalogResourceRequest,
) -> Response[Any]:
    """Rename catalog resource

    Args:
        tablespace_name (str):
        body (RenameCatalogResourceRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        tablespace_name=tablespace_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
