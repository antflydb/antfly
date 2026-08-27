from http import HTTPStatus
from typing import Any
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
from ...models.index_mutation_service_unavailable_error import IndexMutationServiceUnavailableError
from ...models.storage_resource_exhausted_error import StorageResourceExhaustedError
from ...models.unsupported_index_capability_error import UnsupportedIndexCapabilityError
from ...types import Response


def _get_kwargs(
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
        "url": "/db/v1/tables/{table_name}/indexes/{index_name}".format(
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
) -> (
    CreatedAlgebraicIndex
    | CreatedEmbeddingsIndex
    | CreatedFullTextIndex
    | CreatedGraphIndex
    | Error
    | Error
    | UnsupportedIndexCapabilityError
    | IndexMutationServiceUnavailableError
    | StorageResourceExhaustedError
    | None
):
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

        def _parse_response_400(data: object) -> Error | UnsupportedIndexCapabilityError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_400_type_0 = UnsupportedIndexCapabilityError.from_dict(data)

                return response_400_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_400_type_1 = Error.from_dict(data)

            return response_400_type_1

        response_400 = _parse_response_400(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 405:
        response_405 = Error.from_dict(response.json())

        return response_405

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 422:
        response_422 = Error.from_dict(response.json())

        return response_422

    if response.status_code == 429:
        response_429 = StorageResourceExhaustedError.from_dict(response.json())

        return response_429

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = IndexMutationServiceUnavailableError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    CreatedAlgebraicIndex
    | CreatedEmbeddingsIndex
    | CreatedFullTextIndex
    | CreatedGraphIndex
    | Error
    | Error
    | UnsupportedIndexCapabilityError
    | IndexMutationServiceUnavailableError
    | StorageResourceExhaustedError
]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateAlgebraicIndexRequest
    | CreateEmbeddingsIndexRequest
    | CreateFullTextIndexRequest
    | CreateGraphIndexRequest,
) -> Response[
    CreatedAlgebraicIndex
    | CreatedEmbeddingsIndex
    | CreatedFullTextIndex
    | CreatedGraphIndex
    | Error
    | Error
    | UnsupportedIndexCapabilityError
    | IndexMutationServiceUnavailableError
    | StorageResourceExhaustedError
]:
    """Add an index to a table

    Args:
        table_name (str):
        index_name (str):
        body (CreateAlgebraicIndexRequest | CreateEmbeddingsIndexRequest |
            CreateFullTextIndexRequest | CreateGraphIndexRequest): Type-safe configuration for a new
            index. The index name is owned by the request path.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error | Error | UnsupportedIndexCapabilityError | IndexMutationServiceUnavailableError | StorageResourceExhaustedError]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        index_name=index_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateAlgebraicIndexRequest
    | CreateEmbeddingsIndexRequest
    | CreateFullTextIndexRequest
    | CreateGraphIndexRequest,
) -> (
    CreatedAlgebraicIndex
    | CreatedEmbeddingsIndex
    | CreatedFullTextIndex
    | CreatedGraphIndex
    | Error
    | Error
    | UnsupportedIndexCapabilityError
    | IndexMutationServiceUnavailableError
    | StorageResourceExhaustedError
    | None
):
    """Add an index to a table

    Args:
        table_name (str):
        index_name (str):
        body (CreateAlgebraicIndexRequest | CreateEmbeddingsIndexRequest |
            CreateFullTextIndexRequest | CreateGraphIndexRequest): Type-safe configuration for a new
            index. The index name is owned by the request path.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error | Error | UnsupportedIndexCapabilityError | IndexMutationServiceUnavailableError | StorageResourceExhaustedError
    """

    return sync_detailed(
        table_name=table_name,
        index_name=index_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateAlgebraicIndexRequest
    | CreateEmbeddingsIndexRequest
    | CreateFullTextIndexRequest
    | CreateGraphIndexRequest,
) -> Response[
    CreatedAlgebraicIndex
    | CreatedEmbeddingsIndex
    | CreatedFullTextIndex
    | CreatedGraphIndex
    | Error
    | Error
    | UnsupportedIndexCapabilityError
    | IndexMutationServiceUnavailableError
    | StorageResourceExhaustedError
]:
    """Add an index to a table

    Args:
        table_name (str):
        index_name (str):
        body (CreateAlgebraicIndexRequest | CreateEmbeddingsIndexRequest |
            CreateFullTextIndexRequest | CreateGraphIndexRequest): Type-safe configuration for a new
            index. The index name is owned by the request path.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error | Error | UnsupportedIndexCapabilityError | IndexMutationServiceUnavailableError | StorageResourceExhaustedError]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        index_name=index_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateAlgebraicIndexRequest
    | CreateEmbeddingsIndexRequest
    | CreateFullTextIndexRequest
    | CreateGraphIndexRequest,
) -> (
    CreatedAlgebraicIndex
    | CreatedEmbeddingsIndex
    | CreatedFullTextIndex
    | CreatedGraphIndex
    | Error
    | Error
    | UnsupportedIndexCapabilityError
    | IndexMutationServiceUnavailableError
    | StorageResourceExhaustedError
    | None
):
    """Add an index to a table

    Args:
        table_name (str):
        index_name (str):
        body (CreateAlgebraicIndexRequest | CreateEmbeddingsIndexRequest |
            CreateFullTextIndexRequest | CreateGraphIndexRequest): Type-safe configuration for a new
            index. The index name is owned by the request path.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex | Error | Error | UnsupportedIndexCapabilityError | IndexMutationServiceUnavailableError | StorageResourceExhaustedError
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            index_name=index_name,
            client=client,
            body=body,
        )
    ).parsed
