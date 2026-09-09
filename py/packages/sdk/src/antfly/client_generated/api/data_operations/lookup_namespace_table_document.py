from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.lookup_namespace_table_document_response_200 import LookupNamespaceTableDocumentResponse200
from ...types import UNSET, Response, Unset


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    key: str,
    *,
    fields: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["fields"] = fields

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/documents/{key}".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            table_name=quote(str(table_name), safe=""),
            key=quote(str(key), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | LookupNamespaceTableDocumentResponse200 | None:
    if response.status_code == 200:
        response_200 = LookupNamespaceTableDocumentResponse200.from_dict(response.json())

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

    if response.status_code == 501:
        response_501 = cast(Any, None)
        return response_501

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | LookupNamespaceTableDocumentResponse200]:
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
    key: str,
    *,
    client: AuthenticatedClient,
    fields: str | Unset = UNSET,
) -> Response[Any | Error | LookupNamespaceTableDocumentResponse200]:
    """Retrieve a document by key from an explicit namespace table

     Retrieves a document through an explicit database and namespace route. While storage APIs are still
    bare-table-name based, the server fails closed when the resolved catalog table does not map to a
    unique physical table name.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        key (str):
        fields (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | LookupNamespaceTableDocumentResponse200]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        key=key,
        fields=fields,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    fields: str | Unset = UNSET,
) -> Any | Error | LookupNamespaceTableDocumentResponse200 | None:
    """Retrieve a document by key from an explicit namespace table

     Retrieves a document through an explicit database and namespace route. While storage APIs are still
    bare-table-name based, the server fails closed when the resolved catalog table does not map to a
    unique physical table name.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        key (str):
        fields (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | LookupNamespaceTableDocumentResponse200
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        key=key,
        client=client,
        fields=fields,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    fields: str | Unset = UNSET,
) -> Response[Any | Error | LookupNamespaceTableDocumentResponse200]:
    """Retrieve a document by key from an explicit namespace table

     Retrieves a document through an explicit database and namespace route. While storage APIs are still
    bare-table-name based, the server fails closed when the resolved catalog table does not map to a
    unique physical table name.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        key (str):
        fields (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | LookupNamespaceTableDocumentResponse200]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        key=key,
        fields=fields,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    table_name: str,
    key: str,
    *,
    client: AuthenticatedClient,
    fields: str | Unset = UNSET,
) -> Any | Error | LookupNamespaceTableDocumentResponse200 | None:
    """Retrieve a document by key from an explicit namespace table

     Retrieves a document through an explicit database and namespace route. While storage APIs are still
    bare-table-name based, the server fails closed when the resolved catalog table does not map to a
    unique physical table name.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        key (str):
        fields (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | LookupNamespaceTableDocumentResponse200
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            key=key,
            client=client,
            fields=fields,
        )
    ).parsed
