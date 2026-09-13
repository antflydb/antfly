from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.committed_mutation_outcome import CommittedMutationOutcome
from ...models.create_table_request import CreateTableRequest
from ...models.error import Error
from ...models.table_status import TableStatus
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    body: CreateTableRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}".format(
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
) -> Any | CommittedMutationOutcome | Error | TableStatus | None:
    if response.status_code == 201:
        response_201 = TableStatus.from_dict(response.json())

        return response_201

    if response.status_code == 202:
        response_202 = CommittedMutationOutcome.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 501:
        response_501 = cast(Any, None)
        return response_501

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | CommittedMutationOutcome | Error | TableStatus]:
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
    body: CreateTableRequest,
) -> Response[Any | CommittedMutationOutcome | Error | TableStatus]:
    """Create namespace table

     Creates a table through an explicit database and namespace route.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (CreateTableRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | CommittedMutationOutcome | Error | TableStatus]
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
    body: CreateTableRequest,
) -> Any | CommittedMutationOutcome | Error | TableStatus | None:
    """Create namespace table

     Creates a table through an explicit database and namespace route.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (CreateTableRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | CommittedMutationOutcome | Error | TableStatus
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
    body: CreateTableRequest,
) -> Response[Any | CommittedMutationOutcome | Error | TableStatus]:
    """Create namespace table

     Creates a table through an explicit database and namespace route.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (CreateTableRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | CommittedMutationOutcome | Error | TableStatus]
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
    body: CreateTableRequest,
) -> Any | CommittedMutationOutcome | Error | TableStatus | None:
    """Create namespace table

     Creates a table through an explicit database and namespace route.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (CreateTableRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | CommittedMutationOutcome | Error | TableStatus
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
