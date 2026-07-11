from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.backup_request import BackupRequest
from ...models.error import Error
from ...models.restore_table_response_200 import RestoreTableResponse200
from ...models.restore_table_response_202_type_0 import RestoreTableResponse202Type0
from ...models.restore_table_response_202_type_1 import RestoreTableResponse202Type1
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: BackupRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/restore".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | RestoreTableResponse200 | RestoreTableResponse202Type0 | RestoreTableResponse202Type1 | None:
    if response.status_code == 200:
        response_200 = RestoreTableResponse200.from_dict(response.json())

        return response_200

    if response.status_code == 202:

        def _parse_response_202(data: object) -> RestoreTableResponse202Type0 | RestoreTableResponse202Type1:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_202_type_0 = RestoreTableResponse202Type0.from_dict(data)

                return response_202_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_202_type_1 = RestoreTableResponse202Type1.from_dict(data)

            return response_202_type_1

        response_202 = _parse_response_202(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | RestoreTableResponse200 | RestoreTableResponse202Type0 | RestoreTableResponse202Type1]:
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
    body: BackupRequest,
) -> Response[Error | RestoreTableResponse200 | RestoreTableResponse202Type0 | RestoreTableResponse202Type1]:
    """Restore a table from backup

    Args:
        table_name (str):
        body (BackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RestoreTableResponse200 | RestoreTableResponse202Type0 | RestoreTableResponse202Type1]
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
    client: AuthenticatedClient,
    body: BackupRequest,
) -> Error | RestoreTableResponse200 | RestoreTableResponse202Type0 | RestoreTableResponse202Type1 | None:
    """Restore a table from backup

    Args:
        table_name (str):
        body (BackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RestoreTableResponse200 | RestoreTableResponse202Type0 | RestoreTableResponse202Type1
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: BackupRequest,
) -> Response[Error | RestoreTableResponse200 | RestoreTableResponse202Type0 | RestoreTableResponse202Type1]:
    """Restore a table from backup

    Args:
        table_name (str):
        body (BackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RestoreTableResponse200 | RestoreTableResponse202Type0 | RestoreTableResponse202Type1]
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
    client: AuthenticatedClient,
    body: BackupRequest,
) -> Error | RestoreTableResponse200 | RestoreTableResponse202Type0 | RestoreTableResponse202Type1 | None:
    """Restore a table from backup

    Args:
        table_name (str):
        body (BackupRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RestoreTableResponse200 | RestoreTableResponse202Type0 | RestoreTableResponse202Type1
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
