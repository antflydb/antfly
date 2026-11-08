from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.privacy_scan import PrivacyScan
from ...models.privacy_scan_request import PrivacyScanRequest
from ...models.start_table_privacy_scan_response_403 import StartTablePrivacyScanResponse403
from ...models.start_table_privacy_scan_response_409 import StartTablePrivacyScanResponse409
from ...models.start_table_privacy_scan_response_412 import StartTablePrivacyScanResponse412
from ...models.start_table_privacy_scan_response_422 import StartTablePrivacyScanResponse422
from ...models.start_table_privacy_scan_response_428 import StartTablePrivacyScanResponse428
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: PrivacyScanRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/privacy/scans".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    Error
    | PrivacyScan
    | StartTablePrivacyScanResponse403
    | StartTablePrivacyScanResponse409
    | StartTablePrivacyScanResponse412
    | StartTablePrivacyScanResponse422
    | StartTablePrivacyScanResponse428
    | None
):
    if response.status_code == 202:
        response_202 = PrivacyScan.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = StartTablePrivacyScanResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = StartTablePrivacyScanResponse409.from_dict(response.json())

        return response_409

    if response.status_code == 412:
        response_412 = StartTablePrivacyScanResponse412.from_dict(response.json())

        return response_412

    if response.status_code == 422:
        response_422 = StartTablePrivacyScanResponse422.from_dict(response.json())

        return response_422

    if response.status_code == 428:
        response_428 = StartTablePrivacyScanResponse428.from_dict(response.json())

        return response_428

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


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    Error
    | PrivacyScan
    | StartTablePrivacyScanResponse403
    | StartTablePrivacyScanResponse409
    | StartTablePrivacyScanResponse412
    | StartTablePrivacyScanResponse422
    | StartTablePrivacyScanResponse428
]:
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
    body: PrivacyScanRequest,
) -> Response[
    Error
    | PrivacyScan
    | StartTablePrivacyScanResponse403
    | StartTablePrivacyScanResponse409
    | StartTablePrivacyScanResponse412
    | StartTablePrivacyScanResponse422
    | StartTablePrivacyScanResponse428
]:
    """Start a durable privacy rescan

    Args:
        table_name (str):
        body (PrivacyScanRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | PrivacyScan | StartTablePrivacyScanResponse403 | StartTablePrivacyScanResponse409 | StartTablePrivacyScanResponse412 | StartTablePrivacyScanResponse422 | StartTablePrivacyScanResponse428]
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
    body: PrivacyScanRequest,
) -> (
    Error
    | PrivacyScan
    | StartTablePrivacyScanResponse403
    | StartTablePrivacyScanResponse409
    | StartTablePrivacyScanResponse412
    | StartTablePrivacyScanResponse422
    | StartTablePrivacyScanResponse428
    | None
):
    """Start a durable privacy rescan

    Args:
        table_name (str):
        body (PrivacyScanRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | PrivacyScan | StartTablePrivacyScanResponse403 | StartTablePrivacyScanResponse409 | StartTablePrivacyScanResponse412 | StartTablePrivacyScanResponse422 | StartTablePrivacyScanResponse428
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
    body: PrivacyScanRequest,
) -> Response[
    Error
    | PrivacyScan
    | StartTablePrivacyScanResponse403
    | StartTablePrivacyScanResponse409
    | StartTablePrivacyScanResponse412
    | StartTablePrivacyScanResponse422
    | StartTablePrivacyScanResponse428
]:
    """Start a durable privacy rescan

    Args:
        table_name (str):
        body (PrivacyScanRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | PrivacyScan | StartTablePrivacyScanResponse403 | StartTablePrivacyScanResponse409 | StartTablePrivacyScanResponse412 | StartTablePrivacyScanResponse422 | StartTablePrivacyScanResponse428]
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
    body: PrivacyScanRequest,
) -> (
    Error
    | PrivacyScan
    | StartTablePrivacyScanResponse403
    | StartTablePrivacyScanResponse409
    | StartTablePrivacyScanResponse412
    | StartTablePrivacyScanResponse422
    | StartTablePrivacyScanResponse428
    | None
):
    """Start a durable privacy rescan

    Args:
        table_name (str):
        body (PrivacyScanRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | PrivacyScan | StartTablePrivacyScanResponse403 | StartTablePrivacyScanResponse409 | StartTablePrivacyScanResponse412 | StartTablePrivacyScanResponse422 | StartTablePrivacyScanResponse428
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
