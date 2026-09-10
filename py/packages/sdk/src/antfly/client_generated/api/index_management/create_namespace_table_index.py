from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_algebraic_index_request import CreateAlgebraicIndexRequest
from ...models.create_embeddings_index_request import CreateEmbeddingsIndexRequest
from ...models.create_full_text_index_request import CreateFullTextIndexRequest
from ...models.create_graph_index_request import CreateGraphIndexRequest
from ...models.created_algebraic_index import CreatedAlgebraicIndex
from ...models.created_embeddings_index import CreatedEmbeddingsIndex
from ...models.created_full_text_index import CreatedFullTextIndex
from ...models.created_graph_index import CreatedGraphIndex
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    index_name: str,
    *,
    body: CreateAlgebraicIndexRequest
    | CreateEmbeddingsIndexRequest
    | CreateFullTextIndexRequest
    | CreateGraphIndexRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/indexes/{index_name}".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            table_name=quote(str(table_name), safe=""),
            index_name=quote(str(index_name), safe=""),
        ),
    }

    if isinstance(body, CreateFullTextIndexRequest):
        _kwargs["json"] = body.to_dict()
    elif isinstance(body, CreateEmbeddingsIndexRequest):
        _kwargs["json"] = body.to_dict()
    elif isinstance(body, CreateGraphIndexRequest):
        _kwargs["json"] = body.to_dict()
    else:
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error | None:
    if response.status_code == 201:

        def _parse_response_201(
            data: object,
        ) -> CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_created_index_type_0 = CreatedFullTextIndex.from_dict(data)

                return componentsschemas_created_index_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_created_index_type_1 = CreatedEmbeddingsIndex.from_dict(data)

                return componentsschemas_created_index_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_created_index_type_2 = CreatedGraphIndex.from_dict(data)

                return componentsschemas_created_index_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_created_index_type_3 = CreatedAlgebraicIndex.from_dict(data)

            return componentsschemas_created_index_type_3

        response_201 = _parse_response_201(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

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
) -> Response[Any | CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error]:
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
    body: CreateAlgebraicIndexRequest
    | CreateEmbeddingsIndexRequest
    | CreateFullTextIndexRequest
    | CreateGraphIndexRequest,
) -> Response[Any | CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error]:
    """Add an index to an explicit namespace table

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        index_name (str):
        body (CreateAlgebraicIndexRequest | CreateEmbeddingsIndexRequest |
            CreateFullTextIndexRequest | CreateGraphIndexRequest): Type-safe configuration for a new
            index. The index name is owned by the request path.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        index_name=index_name,
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
    index_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateAlgebraicIndexRequest
    | CreateEmbeddingsIndexRequest
    | CreateFullTextIndexRequest
    | CreateGraphIndexRequest,
) -> Any | CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error | None:
    """Add an index to an explicit namespace table

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        index_name (str):
        body (CreateAlgebraicIndexRequest | CreateEmbeddingsIndexRequest |
            CreateFullTextIndexRequest | CreateGraphIndexRequest): Type-safe configuration for a new
            index. The index name is owned by the request path.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        index_name=index_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateAlgebraicIndexRequest
    | CreateEmbeddingsIndexRequest
    | CreateFullTextIndexRequest
    | CreateGraphIndexRequest,
) -> Response[Any | CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error]:
    """Add an index to an explicit namespace table

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        index_name (str):
        body (CreateAlgebraicIndexRequest | CreateEmbeddingsIndexRequest |
            CreateFullTextIndexRequest | CreateGraphIndexRequest): Type-safe configuration for a new
            index. The index name is owned by the request path.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        index_name=index_name,
        body=body,
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
    body: CreateAlgebraicIndexRequest
    | CreateEmbeddingsIndexRequest
    | CreateFullTextIndexRequest
    | CreateGraphIndexRequest,
) -> Any | CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error | None:
    """Add an index to an explicit namespace table

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        index_name (str):
        body (CreateAlgebraicIndexRequest | CreateEmbeddingsIndexRequest |
            CreateFullTextIndexRequest | CreateGraphIndexRequest): Type-safe configuration for a new
            index. The index name is owned by the request path.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            index_name=index_name,
            client=client,
            body=body,
        )
    ).parsed
