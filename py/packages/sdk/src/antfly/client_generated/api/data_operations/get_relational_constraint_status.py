from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.relational_constraint_status import RelationalConstraintStatus
from ...types import Response


def _get_kwargs(
    table_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/constraints/status".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | RelationalConstraintStatus | None:
    if response.status_code == 200:
        response_200 = RelationalConstraintStatus.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | RelationalConstraintStatus]:
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
) -> Response[Any | Error | RelationalConstraintStatus]:
    """Read distributed unique and foreign-key validation coverage

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | RelationalConstraintStatus]
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
) -> Any | Error | RelationalConstraintStatus | None:
    """Read distributed unique and foreign-key validation coverage

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | RelationalConstraintStatus
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | Error | RelationalConstraintStatus]:
    """Read distributed unique and foreign-key validation coverage

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | RelationalConstraintStatus]
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
) -> Any | Error | RelationalConstraintStatus | None:
    """Read distributed unique and foreign-key validation coverage

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | RelationalConstraintStatus
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
        )
    ).parsed
