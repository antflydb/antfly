from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.privacy_findings_page import PrivacyFindingsPage
from ...models.privacy_findings_request import PrivacyFindingsRequest
from ...models.query_namespace_table_privacy_findings_response_403 import QueryNamespaceTablePrivacyFindingsResponse403
from ...models.query_namespace_table_privacy_findings_response_412 import QueryNamespaceTablePrivacyFindingsResponse412
from ...models.query_namespace_table_privacy_findings_response_422 import QueryNamespaceTablePrivacyFindingsResponse422
from ...models.query_namespace_table_privacy_findings_response_428 import QueryNamespaceTablePrivacyFindingsResponse428
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    body: PrivacyFindingsRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/privacy/findings".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
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
    | PrivacyFindingsPage
    | QueryNamespaceTablePrivacyFindingsResponse403
    | QueryNamespaceTablePrivacyFindingsResponse412
    | QueryNamespaceTablePrivacyFindingsResponse422
    | QueryNamespaceTablePrivacyFindingsResponse428
    | None
):
    if response.status_code == 200:
        response_200 = PrivacyFindingsPage.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = QueryNamespaceTablePrivacyFindingsResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 412:
        response_412 = QueryNamespaceTablePrivacyFindingsResponse412.from_dict(response.json())

        return response_412

    if response.status_code == 422:
        response_422 = QueryNamespaceTablePrivacyFindingsResponse422.from_dict(response.json())

        return response_422

    if response.status_code == 428:
        response_428 = QueryNamespaceTablePrivacyFindingsResponse428.from_dict(response.json())

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
    | PrivacyFindingsPage
    | QueryNamespaceTablePrivacyFindingsResponse403
    | QueryNamespaceTablePrivacyFindingsResponse412
    | QueryNamespaceTablePrivacyFindingsResponse422
    | QueryNamespaceTablePrivacyFindingsResponse428
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
    *,
    client: AuthenticatedClient,
    body: PrivacyFindingsRequest,
) -> Response[
    Error
    | PrivacyFindingsPage
    | QueryNamespaceTablePrivacyFindingsResponse403
    | QueryNamespaceTablePrivacyFindingsResponse412
    | QueryNamespaceTablePrivacyFindingsResponse422
    | QueryNamespaceTablePrivacyFindingsResponse428
]:
    """List current privacy findings with body filters

     Equivalent to GET findings, with filters and continuation in JSON so large document identifiers do
    not exceed URL limits.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (PrivacyFindingsRequest): Findings filters and opaque continuation in the request
            body, avoiding URL limits for large document identifiers.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | PrivacyFindingsPage | QueryNamespaceTablePrivacyFindingsResponse403 | QueryNamespaceTablePrivacyFindingsResponse412 | QueryNamespaceTablePrivacyFindingsResponse422 | QueryNamespaceTablePrivacyFindingsResponse428]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: PrivacyFindingsRequest,
) -> (
    Error
    | PrivacyFindingsPage
    | QueryNamespaceTablePrivacyFindingsResponse403
    | QueryNamespaceTablePrivacyFindingsResponse412
    | QueryNamespaceTablePrivacyFindingsResponse422
    | QueryNamespaceTablePrivacyFindingsResponse428
    | None
):
    """List current privacy findings with body filters

     Equivalent to GET findings, with filters and continuation in JSON so large document identifiers do
    not exceed URL limits.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (PrivacyFindingsRequest): Findings filters and opaque continuation in the request
            body, avoiding URL limits for large document identifiers.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | PrivacyFindingsPage | QueryNamespaceTablePrivacyFindingsResponse403 | QueryNamespaceTablePrivacyFindingsResponse412 | QueryNamespaceTablePrivacyFindingsResponse422 | QueryNamespaceTablePrivacyFindingsResponse428
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: PrivacyFindingsRequest,
) -> Response[
    Error
    | PrivacyFindingsPage
    | QueryNamespaceTablePrivacyFindingsResponse403
    | QueryNamespaceTablePrivacyFindingsResponse412
    | QueryNamespaceTablePrivacyFindingsResponse422
    | QueryNamespaceTablePrivacyFindingsResponse428
]:
    """List current privacy findings with body filters

     Equivalent to GET findings, with filters and continuation in JSON so large document identifiers do
    not exceed URL limits.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (PrivacyFindingsRequest): Findings filters and opaque continuation in the request
            body, avoiding URL limits for large document identifiers.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | PrivacyFindingsPage | QueryNamespaceTablePrivacyFindingsResponse403 | QueryNamespaceTablePrivacyFindingsResponse412 | QueryNamespaceTablePrivacyFindingsResponse422 | QueryNamespaceTablePrivacyFindingsResponse428]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: PrivacyFindingsRequest,
) -> (
    Error
    | PrivacyFindingsPage
    | QueryNamespaceTablePrivacyFindingsResponse403
    | QueryNamespaceTablePrivacyFindingsResponse412
    | QueryNamespaceTablePrivacyFindingsResponse422
    | QueryNamespaceTablePrivacyFindingsResponse428
    | None
):
    """List current privacy findings with body filters

     Equivalent to GET findings, with filters and continuation in JSON so large document identifiers do
    not exceed URL limits.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (PrivacyFindingsRequest): Findings filters and opaque continuation in the request
            body, avoiding URL limits for large document identifiers.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | PrivacyFindingsPage | QueryNamespaceTablePrivacyFindingsResponse403 | QueryNamespaceTablePrivacyFindingsResponse412 | QueryNamespaceTablePrivacyFindingsResponse422 | QueryNamespaceTablePrivacyFindingsResponse428
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
