from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.restore_job import RestoreJob
from ...models.restore_request import RestoreRequest
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    *,
    body: RestoreRequest,
    idempotency_key: str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(idempotency_key, Unset):
        headers["Idempotency-Key"] = idempotency_key

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


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | RestoreJob | None:
    if response.status_code == 202:
        response_202 = RestoreJob.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = Error.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Error | RestoreJob]:
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
    body: RestoreRequest,
    idempotency_key: str | Unset = UNSET,
) -> Response[Error | RestoreJob]:
    """Restore a table from backup

    Args:
        table_name (str):
        idempotency_key (str | Unset):
        body (RestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RestoreJob]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
        idempotency_key=idempotency_key,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RestoreRequest,
    idempotency_key: str | Unset = UNSET,
) -> Error | RestoreJob | None:
    """Restore a table from backup

    Args:
        table_name (str):
        idempotency_key (str | Unset):
        body (RestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RestoreJob
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        body=body,
        idempotency_key=idempotency_key,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RestoreRequest,
    idempotency_key: str | Unset = UNSET,
) -> Response[Error | RestoreJob]:
    """Restore a table from backup

    Args:
        table_name (str):
        idempotency_key (str | Unset):
        body (RestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RestoreJob]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
        idempotency_key=idempotency_key,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RestoreRequest,
    idempotency_key: str | Unset = UNSET,
) -> Error | RestoreJob | None:
    """Restore a table from backup

    Args:
        table_name (str):
        idempotency_key (str | Unset):
        body (RestoreRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RestoreJob
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
            idempotency_key=idempotency_key,
        )
    ).parsed
