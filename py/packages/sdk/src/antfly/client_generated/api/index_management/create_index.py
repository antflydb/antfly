from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.algebraic_index_config import AlgebraicIndexConfig
from ...models.embeddings_index_config import EmbeddingsIndexConfig
from ...models.error import Error
from ...models.full_text_index_config import FullTextIndexConfig
from ...models.graph_index_config import GraphIndexConfig
from ...types import Response


def _get_kwargs(
    table_name: str,
    index_name: str,
    *,
    body: AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig | GraphIndexConfig,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/indexes/{index_name}".format(
            table_name=quote(str(table_name), safe=""),
            index_name=quote(str(index_name), safe=""),
        ),
    }

    if isinstance(body, FullTextIndexConfig):
        _kwargs["json"] = body.to_dict()
    elif isinstance(body, EmbeddingsIndexConfig):
        _kwargs["json"] = body.to_dict()
    elif isinstance(body, GraphIndexConfig):
        _kwargs["json"] = body.to_dict()
    else:
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | Error | None:
    if response.status_code == 201:
        response_201 = cast(Any, None)
        return response_201

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Any | Error]:
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
    body: AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig | GraphIndexConfig,
) -> Response[Any | Error]:
    """Add an index to a table

    Args:
        table_name (str):
        index_name (str):
        body (AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig |
            GraphIndexConfig): Configuration for an index

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error]
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
    body: AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig | GraphIndexConfig,
) -> Any | Error | None:
    """Add an index to a table

    Args:
        table_name (str):
        index_name (str):
        body (AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig |
            GraphIndexConfig): Configuration for an index

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error
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
    body: AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig | GraphIndexConfig,
) -> Response[Any | Error]:
    """Add an index to a table

    Args:
        table_name (str):
        index_name (str):
        body (AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig |
            GraphIndexConfig): Configuration for an index

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error]
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
    body: AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig | GraphIndexConfig,
) -> Any | Error | None:
    """Add an index to a table

    Args:
        table_name (str):
        index_name (str):
        body (AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig |
            GraphIndexConfig): Configuration for an index

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            index_name=index_name,
            client=client,
            body=body,
        )
    ).parsed
