from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.get_namespace_table_privacy_scan_response_403 import GetNamespaceTablePrivacyScanResponse403
from ...models.get_namespace_table_privacy_scan_response_412 import GetNamespaceTablePrivacyScanResponse412
from ...models.get_namespace_table_privacy_scan_response_422 import GetNamespaceTablePrivacyScanResponse422
from ...models.get_namespace_table_privacy_scan_response_428 import GetNamespaceTablePrivacyScanResponse428
from ...models.privacy_scan import PrivacyScan
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    scan_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/privacy/scans/{scan_id}".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            table_name=quote(str(table_name), safe=""),
            scan_id=quote(str(scan_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    Error
    | GetNamespaceTablePrivacyScanResponse403
    | GetNamespaceTablePrivacyScanResponse412
    | GetNamespaceTablePrivacyScanResponse422
    | GetNamespaceTablePrivacyScanResponse428
    | PrivacyScan
    | None
):
    if response.status_code == 200:
        response_200 = PrivacyScan.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = GetNamespaceTablePrivacyScanResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 412:
        response_412 = GetNamespaceTablePrivacyScanResponse412.from_dict(response.json())

        return response_412

    if response.status_code == 422:
        response_422 = GetNamespaceTablePrivacyScanResponse422.from_dict(response.json())

        return response_422

    if response.status_code == 428:
        response_428 = GetNamespaceTablePrivacyScanResponse428.from_dict(response.json())

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
    | GetNamespaceTablePrivacyScanResponse403
    | GetNamespaceTablePrivacyScanResponse412
    | GetNamespaceTablePrivacyScanResponse422
    | GetNamespaceTablePrivacyScanResponse428
    | PrivacyScan
]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    scan_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[
    Error
    | GetNamespaceTablePrivacyScanResponse403
    | GetNamespaceTablePrivacyScanResponse412
    | GetNamespaceTablePrivacyScanResponse422
    | GetNamespaceTablePrivacyScanResponse428
    | PrivacyScan
]:
    """Read privacy scan progress

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        scan_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GetNamespaceTablePrivacyScanResponse403 | GetNamespaceTablePrivacyScanResponse412 | GetNamespaceTablePrivacyScanResponse422 | GetNamespaceTablePrivacyScanResponse428 | PrivacyScan]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        scan_id=scan_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    table_name: str,
    scan_id: str,
    *,
    client: AuthenticatedClient,
) -> (
    Error
    | GetNamespaceTablePrivacyScanResponse403
    | GetNamespaceTablePrivacyScanResponse412
    | GetNamespaceTablePrivacyScanResponse422
    | GetNamespaceTablePrivacyScanResponse428
    | PrivacyScan
    | None
):
    """Read privacy scan progress

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        scan_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GetNamespaceTablePrivacyScanResponse403 | GetNamespaceTablePrivacyScanResponse412 | GetNamespaceTablePrivacyScanResponse422 | GetNamespaceTablePrivacyScanResponse428 | PrivacyScan
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        scan_id=scan_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    scan_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[
    Error
    | GetNamespaceTablePrivacyScanResponse403
    | GetNamespaceTablePrivacyScanResponse412
    | GetNamespaceTablePrivacyScanResponse422
    | GetNamespaceTablePrivacyScanResponse428
    | PrivacyScan
]:
    """Read privacy scan progress

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        scan_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GetNamespaceTablePrivacyScanResponse403 | GetNamespaceTablePrivacyScanResponse412 | GetNamespaceTablePrivacyScanResponse422 | GetNamespaceTablePrivacyScanResponse428 | PrivacyScan]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        scan_id=scan_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    table_name: str,
    scan_id: str,
    *,
    client: AuthenticatedClient,
) -> (
    Error
    | GetNamespaceTablePrivacyScanResponse403
    | GetNamespaceTablePrivacyScanResponse412
    | GetNamespaceTablePrivacyScanResponse422
    | GetNamespaceTablePrivacyScanResponse428
    | PrivacyScan
    | None
):
    """Read privacy scan progress

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        scan_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GetNamespaceTablePrivacyScanResponse403 | GetNamespaceTablePrivacyScanResponse412 | GetNamespaceTablePrivacyScanResponse422 | GetNamespaceTablePrivacyScanResponse428 | PrivacyScan
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            scan_id=scan_id,
            client=client,
        )
    ).parsed
