from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ...client import AuthenticatedClient, Client
from ...models.changefeed_error import ChangefeedError
from ...models.managed_source_configured import ManagedSourceConfigured
from ...models.managed_source_replacement import ManagedSourceReplacement
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: ManagedSourceReplacement,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/sources/managed".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ChangefeedError | ManagedSourceConfigured:
    if response.status_code == 200:
        response_200 = ManagedSourceConfigured.from_dict(response.json())

        return response_200

    response_default = ChangefeedError.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ChangefeedError | ManagedSourceConfigured]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ManagedSourceReplacement,
) -> Response[ChangefeedError | ManagedSourceConfigured]:
    """Replace managed-source configuration using the current table ID and source hash

    Args:
        table_name (str):
        body (ManagedSourceReplacement):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedError | ManagedSourceConfigured]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ManagedSourceReplacement,
) -> ChangefeedError | ManagedSourceConfigured | None:
    """Replace managed-source configuration using the current table ID and source hash

    Args:
        table_name (str):
        body (ManagedSourceReplacement):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedError | ManagedSourceConfigured
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ManagedSourceReplacement,
) -> Response[ChangefeedError | ManagedSourceConfigured]:
    """Replace managed-source configuration using the current table ID and source hash

    Args:
        table_name (str):
        body (ManagedSourceReplacement):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChangefeedError | ManagedSourceConfigured]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ManagedSourceReplacement,
) -> ChangefeedError | ManagedSourceConfigured | None:
    """Replace managed-source configuration using the current table ID and source hash

    Args:
        table_name (str):
        body (ManagedSourceReplacement):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChangefeedError | ManagedSourceConfigured
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
