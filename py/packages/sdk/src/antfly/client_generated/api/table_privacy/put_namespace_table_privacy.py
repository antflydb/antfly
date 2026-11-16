from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.privacy_policy import PrivacyPolicy
from ...models.privacy_status import PrivacyStatus
from ...models.put_namespace_table_privacy_response_403 import PutNamespaceTablePrivacyResponse403
from ...models.put_namespace_table_privacy_response_412 import PutNamespaceTablePrivacyResponse412
from ...models.put_namespace_table_privacy_response_422 import PutNamespaceTablePrivacyResponse422
from ...models.put_namespace_table_privacy_response_428 import PutNamespaceTablePrivacyResponse428
from ...types import UNSET, Response, Unset


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    body: PrivacyPolicy,
    if_match: str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(if_match, Unset):
        headers["If-Match"] = if_match

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/privacy".format(
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
    | PrivacyStatus
    | PutNamespaceTablePrivacyResponse403
    | PutNamespaceTablePrivacyResponse412
    | PutNamespaceTablePrivacyResponse422
    | PutNamespaceTablePrivacyResponse428
    | None
):
    if response.status_code == 202:
        response_202 = PrivacyStatus.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = PutNamespaceTablePrivacyResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 412:
        response_412 = PutNamespaceTablePrivacyResponse412.from_dict(response.json())

        return response_412

    if response.status_code == 422:
        response_422 = PutNamespaceTablePrivacyResponse422.from_dict(response.json())

        return response_422

    if response.status_code == 428:
        response_428 = PutNamespaceTablePrivacyResponse428.from_dict(response.json())

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
    | PrivacyStatus
    | PutNamespaceTablePrivacyResponse403
    | PutNamespaceTablePrivacyResponse412
    | PutNamespaceTablePrivacyResponse422
    | PutNamespaceTablePrivacyResponse428
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
    body: PrivacyPolicy,
    if_match: str | Unset = UNSET,
) -> Response[
    Error
    | PrivacyStatus
    | PutNamespaceTablePrivacyResponse403
    | PutNamespaceTablePrivacyResponse412
    | PutNamespaceTablePrivacyResponse422
    | PutNamespaceTablePrivacyResponse428
]:
    """Apply table privacy policy

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        if_match (str | Unset):
        body (PrivacyPolicy):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | PrivacyStatus | PutNamespaceTablePrivacyResponse403 | PutNamespaceTablePrivacyResponse412 | PutNamespaceTablePrivacyResponse422 | PutNamespaceTablePrivacyResponse428]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        body=body,
        if_match=if_match,
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
    body: PrivacyPolicy,
    if_match: str | Unset = UNSET,
) -> (
    Error
    | PrivacyStatus
    | PutNamespaceTablePrivacyResponse403
    | PutNamespaceTablePrivacyResponse412
    | PutNamespaceTablePrivacyResponse422
    | PutNamespaceTablePrivacyResponse428
    | None
):
    """Apply table privacy policy

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        if_match (str | Unset):
        body (PrivacyPolicy):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | PrivacyStatus | PutNamespaceTablePrivacyResponse403 | PutNamespaceTablePrivacyResponse412 | PutNamespaceTablePrivacyResponse422 | PutNamespaceTablePrivacyResponse428
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        client=client,
        body=body,
        if_match=if_match,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: PrivacyPolicy,
    if_match: str | Unset = UNSET,
) -> Response[
    Error
    | PrivacyStatus
    | PutNamespaceTablePrivacyResponse403
    | PutNamespaceTablePrivacyResponse412
    | PutNamespaceTablePrivacyResponse422
    | PutNamespaceTablePrivacyResponse428
]:
    """Apply table privacy policy

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        if_match (str | Unset):
        body (PrivacyPolicy):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | PrivacyStatus | PutNamespaceTablePrivacyResponse403 | PutNamespaceTablePrivacyResponse412 | PutNamespaceTablePrivacyResponse422 | PutNamespaceTablePrivacyResponse428]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        body=body,
        if_match=if_match,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: PrivacyPolicy,
    if_match: str | Unset = UNSET,
) -> (
    Error
    | PrivacyStatus
    | PutNamespaceTablePrivacyResponse403
    | PutNamespaceTablePrivacyResponse412
    | PutNamespaceTablePrivacyResponse422
    | PutNamespaceTablePrivacyResponse428
    | None
):
    """Apply table privacy policy

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        if_match (str | Unset):
        body (PrivacyPolicy):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | PrivacyStatus | PutNamespaceTablePrivacyResponse403 | PutNamespaceTablePrivacyResponse412 | PutNamespaceTablePrivacyResponse422 | PutNamespaceTablePrivacyResponse428
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            client=client,
            body=body,
            if_match=if_match,
        )
    ).parsed
