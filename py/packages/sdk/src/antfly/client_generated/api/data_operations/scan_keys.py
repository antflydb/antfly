from http import HTTPStatus
from io import BytesIO
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.scan_keys_request import ScanKeysRequest
from ...types import UNSET, File, Response, Unset


def _get_kwargs(
    table_name: str,
    *,
    body: ScanKeysRequest | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/documents".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | File | None:
    if response.status_code == 200:
        response_200 = File(payload=BytesIO(response.content))

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


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Error | File]:
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
    body: ScanKeysRequest | Unset = UNSET,
) -> Response[Error | File]:
    """Scan documents in a table within a key range

     Scans keys in a table within an optional key range and returns them as
    newline-delimited JSON (NDJSON). Each line contains a JSON object with
    the `_id` document identifier and optionally projected document fields. This is useful for
    iterating through all keys in a table or a subset of keys within a range.

    Args:
        table_name (str):
        body (ScanKeysRequest | Unset): Request to scan keys in a table within a key range.
            If no range is specified, scans all keys in the table.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | File]
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
    body: ScanKeysRequest | Unset = UNSET,
) -> Error | File | None:
    """Scan documents in a table within a key range

     Scans keys in a table within an optional key range and returns them as
    newline-delimited JSON (NDJSON). Each line contains a JSON object with
    the `_id` document identifier and optionally projected document fields. This is useful for
    iterating through all keys in a table or a subset of keys within a range.

    Args:
        table_name (str):
        body (ScanKeysRequest | Unset): Request to scan keys in a table within a key range.
            If no range is specified, scans all keys in the table.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | File
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
    body: ScanKeysRequest | Unset = UNSET,
) -> Response[Error | File]:
    """Scan documents in a table within a key range

     Scans keys in a table within an optional key range and returns them as
    newline-delimited JSON (NDJSON). Each line contains a JSON object with
    the `_id` document identifier and optionally projected document fields. This is useful for
    iterating through all keys in a table or a subset of keys within a range.

    Args:
        table_name (str):
        body (ScanKeysRequest | Unset): Request to scan keys in a table within a key range.
            If no range is specified, scans all keys in the table.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | File]
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
    body: ScanKeysRequest | Unset = UNSET,
) -> Error | File | None:
    """Scan documents in a table within a key range

     Scans keys in a table within an optional key range and returns them as
    newline-delimited JSON (NDJSON). Each line contains a JSON object with
    the `_id` document identifier and optionally projected document fields. This is useful for
    iterating through all keys in a table or a subset of keys within a range.

    Args:
        table_name (str):
        body (ScanKeysRequest | Unset): Request to scan keys in a table within a key range.
            If no range is specified, scans all keys in the table.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | File
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
