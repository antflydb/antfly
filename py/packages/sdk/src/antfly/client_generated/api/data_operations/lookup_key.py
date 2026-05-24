from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.lookup_key_response_200 import LookupKeyResponse200
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    key: str,
    *,
    fields: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["fields"] = fields

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/tables/{table_name}/lookup/{key}".format(
            table_name=quote(str(table_name), safe=""),
            key=quote(str(key), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | LookupKeyResponse200 | None:
    if response.status_code == 200:
        response_200 = LookupKeyResponse200.from_dict(response.json())

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
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | LookupKeyResponse200]:
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
    fields: str | Unset = UNSET,
) -> Response[Error | LookupKeyResponse200]:
    """Lookup a key in a table

    Args:
        table_name (str):
        key (str):
        fields (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | LookupKeyResponse200]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        fields=fields,
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
    fields: str | Unset = UNSET,
) -> Error | LookupKeyResponse200 | None:
    """Lookup a key in a table

    Args:
        table_name (str):
        key (str):
        fields (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | LookupKeyResponse200
    """

    return sync_detailed(
        table_name=table_name,
        key=key,
        client=client,
        fields=fields,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    fields: str | Unset = UNSET,
) -> Response[Error | LookupKeyResponse200]:
    """Lookup a key in a table

    Args:
        table_name (str):
        key (str):
        fields (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | LookupKeyResponse200]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        fields=fields,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    fields: str | Unset = UNSET,
) -> Error | LookupKeyResponse200 | None:
    """Lookup a key in a table

    Args:
        table_name (str):
        key (str):
        fields (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | LookupKeyResponse200
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            key=key,
            client=client,
            fields=fields,
        )
    ).parsed
