from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.batch_response import BatchResponse
from ...models.error import Error
from ...models.rows_batch_request import RowsBatchRequest
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    body: RowsBatchRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/rows/batch".format(
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
) -> Any | BatchResponse | Error | None:
    if response.status_code == 201:
        response_201 = BatchResponse.from_dict(response.json())

        return response_201

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
) -> Response[Any | BatchResponse | Error]:
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
    body: RowsBatchRequest,
) -> Response[Any | BatchResponse | Error]:
    """Perform structured relational row writes on an explicit namespace table

     Relational row batch endpoint for an explicit database and namespace table. While storage APIs are
    still bare-table-name based, the server fails closed when the resolved catalog table does not map to
    a unique physical table name.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (RowsBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | BatchResponse | Error]
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
    body: RowsBatchRequest,
) -> Any | BatchResponse | Error | None:
    """Perform structured relational row writes on an explicit namespace table

     Relational row batch endpoint for an explicit database and namespace table. While storage APIs are
    still bare-table-name based, the server fails closed when the resolved catalog table does not map to
    a unique physical table name.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (RowsBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | BatchResponse | Error
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
    body: RowsBatchRequest,
) -> Response[Any | BatchResponse | Error]:
    """Perform structured relational row writes on an explicit namespace table

     Relational row batch endpoint for an explicit database and namespace table. While storage APIs are
    still bare-table-name based, the server fails closed when the resolved catalog table does not map to
    a unique physical table name.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (RowsBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | BatchResponse | Error]
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
    body: RowsBatchRequest,
) -> Any | BatchResponse | Error | None:
    """Perform structured relational row writes on an explicit namespace table

     Relational row batch endpoint for an explicit database and namespace table. While storage APIs are
    still bare-table-name based, the server fails closed when the resolved catalog table does not map to
    a unique physical table name.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (RowsBatchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | BatchResponse | Error
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
