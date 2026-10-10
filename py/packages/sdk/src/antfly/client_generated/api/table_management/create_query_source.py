from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_query_source_body import CreateQuerySourceBody
from ...models.error import Error
from ...models.saved_query_source import SavedQuerySource
from ...types import Response


def _get_kwargs(
    source_name: str,
    *,
    body: CreateQuerySourceBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/sources/{source_name}".format(
            source_name=quote(str(source_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | SavedQuerySource | None:
    if response.status_code == 201:
        response_201 = SavedQuerySource.from_dict(response.json())

        return response_201

    if response.status_code == 202:
        response_202 = cast(Any, None)
        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | SavedQuerySource]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    source_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateQuerySourceBody,
) -> Response[Any | Error | SavedQuerySource]:
    """Create saved query source

     Creates an immutable union or keyed overlay in the Antfly metadata catalog. Reads require permission
    on the saved name and every input table. Drop and recreate to change a definition; existing cursors
    cannot switch incarnations.

    Args:
        source_name (str):
        body (CreateQuerySourceBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | SavedQuerySource]
    """

    kwargs = _get_kwargs(
        source_name=source_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    source_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateQuerySourceBody,
) -> Any | Error | SavedQuerySource | None:
    """Create saved query source

     Creates an immutable union or keyed overlay in the Antfly metadata catalog. Reads require permission
    on the saved name and every input table. Drop and recreate to change a definition; existing cursors
    cannot switch incarnations.

    Args:
        source_name (str):
        body (CreateQuerySourceBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | SavedQuerySource
    """

    return sync_detailed(
        source_name=source_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    source_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateQuerySourceBody,
) -> Response[Any | Error | SavedQuerySource]:
    """Create saved query source

     Creates an immutable union or keyed overlay in the Antfly metadata catalog. Reads require permission
    on the saved name and every input table. Drop and recreate to change a definition; existing cursors
    cannot switch incarnations.

    Args:
        source_name (str):
        body (CreateQuerySourceBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | SavedQuerySource]
    """

    kwargs = _get_kwargs(
        source_name=source_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    source_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateQuerySourceBody,
) -> Any | Error | SavedQuerySource | None:
    """Create saved query source

     Creates an immutable union or keyed overlay in the Antfly metadata catalog. Reads require permission
    on the saved name and every input table. Drop and recreate to change a definition; existing cursors
    cannot switch incarnations.

    Args:
        source_name (str):
        body (CreateQuerySourceBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | SavedQuerySource
    """

    return (
        await asyncio_detailed(
            source_name=source_name,
            client=client,
            body=body,
        )
    ).parsed
