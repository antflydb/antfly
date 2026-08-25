from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.reauthorize_table_destinations_response_200 import ReauthorizeTableDestinationsResponse200
from ...types import Response


def _get_kwargs(
    table_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/destination-authorization".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | ReauthorizeTableDestinationsResponse200 | None:
    if response.status_code == 200:
        response_200 = ReauthorizeTableDestinationsResponse200.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = Error.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 422:
        response_422 = Error.from_dict(response.json())

        return response_422

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | ReauthorizeTableDestinationsResponse200]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | ReauthorizeTableDestinationsResponse200]:
    """Adopt stored write destinations with the current credential

     Reauthorizes the table's existing CDC routes and graph resolver destinations
    using the caller's current permissions. This idempotent operation is intended
    for upgrading tables created before durable destination authorization was
    introduced, and for explicitly transferring destination ownership after a
    credential is rotated. The caller needs admin permission on the source table
    and write permission on every eventual destination table.

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | ReauthorizeTableDestinationsResponse200]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> Error | ReauthorizeTableDestinationsResponse200 | None:
    """Adopt stored write destinations with the current credential

     Reauthorizes the table's existing CDC routes and graph resolver destinations
    using the caller's current permissions. This idempotent operation is intended
    for upgrading tables created before durable destination authorization was
    introduced, and for explicitly transferring destination ownership after a
    credential is rotated. The caller needs admin permission on the source table
    and write permission on every eventual destination table.

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | ReauthorizeTableDestinationsResponse200
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | ReauthorizeTableDestinationsResponse200]:
    """Adopt stored write destinations with the current credential

     Reauthorizes the table's existing CDC routes and graph resolver destinations
    using the caller's current permissions. This idempotent operation is intended
    for upgrading tables created before durable destination authorization was
    introduced, and for explicitly transferring destination ownership after a
    credential is rotated. The caller needs admin permission on the source table
    and write permission on every eventual destination table.

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | ReauthorizeTableDestinationsResponse200]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> Error | ReauthorizeTableDestinationsResponse200 | None:
    """Adopt stored write destinations with the current credential

     Reauthorizes the table's existing CDC routes and graph resolver destinations
    using the caller's current permissions. This idempotent operation is intended
    for upgrading tables created before durable destination authorization was
    introduced, and for explicitly transferring destination ownership after a
    credential is rotated. The caller needs admin permission on the source table
    and write permission on every eventual destination table.

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | ReauthorizeTableDestinationsResponse200
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
        )
    ).parsed
