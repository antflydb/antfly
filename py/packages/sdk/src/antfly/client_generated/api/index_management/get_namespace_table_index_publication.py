from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.index_publication_status import IndexPublicationStatus
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    index_name: str,
    *,
    x_antfly_index_publication_token: str,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    headers["X-Antfly-Index-Publication-Token"] = x_antfly_index_publication_token

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/indexes/{index_name}/publication".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            table_name=quote(str(table_name), safe=""),
            index_name=quote(str(index_name), safe=""),
        ),
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | IndexPublicationStatus | None:
    if response.status_code == 200:
        response_200 = IndexPublicationStatus.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 426:
        response_426 = cast(Any, None)
        return response_426

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | IndexPublicationStatus]:
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
    index_name: str,
    *,
    client: AuthenticatedClient,
    x_antfly_index_publication_token: str,
) -> Response[Any | Error | IndexPublicationStatus]:
    """Observe an exact index publication in an explicit namespace

     Requires administration of the current logical table and its retained recovery token. Unknown
    admission is not permission to replay DDL; published is historical job completion, not current index
    query readiness.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        index_name (str):
        x_antfly_index_publication_token (str): Opaque versioned recovery handle binding a
            metadata incarnation, immutable table identity, publication ID and plan commitment. This
            is not authentication or permission to execute, cancel, adopt a root or replay DDL. Retain
            it before admission and use current table authorization when observing the publication.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | IndexPublicationStatus]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        index_name=index_name,
        x_antfly_index_publication_token=x_antfly_index_publication_token,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    x_antfly_index_publication_token: str,
) -> Any | Error | IndexPublicationStatus | None:
    """Observe an exact index publication in an explicit namespace

     Requires administration of the current logical table and its retained recovery token. Unknown
    admission is not permission to replay DDL; published is historical job completion, not current index
    query readiness.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        index_name (str):
        x_antfly_index_publication_token (str): Opaque versioned recovery handle binding a
            metadata incarnation, immutable table identity, publication ID and plan commitment. This
            is not authentication or permission to execute, cancel, adopt a root or replay DDL. Retain
            it before admission and use current table authorization when observing the publication.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | IndexPublicationStatus
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        index_name=index_name,
        client=client,
        x_antfly_index_publication_token=x_antfly_index_publication_token,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    x_antfly_index_publication_token: str,
) -> Response[Any | Error | IndexPublicationStatus]:
    """Observe an exact index publication in an explicit namespace

     Requires administration of the current logical table and its retained recovery token. Unknown
    admission is not permission to replay DDL; published is historical job completion, not current index
    query readiness.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        index_name (str):
        x_antfly_index_publication_token (str): Opaque versioned recovery handle binding a
            metadata incarnation, immutable table identity, publication ID and plan commitment. This
            is not authentication or permission to execute, cancel, adopt a root or replay DDL. Retain
            it before admission and use current table authorization when observing the publication.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | IndexPublicationStatus]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        index_name=index_name,
        x_antfly_index_publication_token=x_antfly_index_publication_token,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    x_antfly_index_publication_token: str,
) -> Any | Error | IndexPublicationStatus | None:
    """Observe an exact index publication in an explicit namespace

     Requires administration of the current logical table and its retained recovery token. Unknown
    admission is not permission to replay DDL; published is historical job completion, not current index
    query readiness.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        index_name (str):
        x_antfly_index_publication_token (str): Opaque versioned recovery handle binding a
            metadata incarnation, immutable table identity, publication ID and plan commitment. This
            is not authentication or permission to execute, cancel, adopt a root or replay DDL. Retain
            it before admission and use current table authorization when observing the publication.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | IndexPublicationStatus
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            index_name=index_name,
            client=client,
            x_antfly_index_publication_token=x_antfly_index_publication_token,
        )
    ).parsed
