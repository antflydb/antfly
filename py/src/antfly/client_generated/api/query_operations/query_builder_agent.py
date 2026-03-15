from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.query_builder_request import QueryBuilderRequest
from ...models.query_builder_result import QueryBuilderResult
from ...types import Response


def _get_kwargs(
    *,
    body: QueryBuilderRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/agents/query-builder",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, QueryBuilderResult]]:
    if response.status_code == 200:
        response_200 = QueryBuilderResult.from_dict(response.json())

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Error, QueryBuilderResult]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: QueryBuilderRequest,
) -> Response[Union[Error, QueryBuilderResult]]:
    """Build a search query from natural language

     Uses an LLM to translate natural language search intent into a structured Bleve query.
    The generated query can be used directly in the QueryRequest.full_text_search or filter_query
    fields.

    This endpoint is useful for:
    - Building queries from user descriptions
    - Generating example queries for a table's schema
    - Agentic retrieval in RAG pipelines

    Args:
        body (QueryBuilderRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, QueryBuilderResult]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    body: QueryBuilderRequest,
) -> Optional[Union[Error, QueryBuilderResult]]:
    """Build a search query from natural language

     Uses an LLM to translate natural language search intent into a structured Bleve query.
    The generated query can be used directly in the QueryRequest.full_text_search or filter_query
    fields.

    This endpoint is useful for:
    - Building queries from user descriptions
    - Generating example queries for a table's schema
    - Agentic retrieval in RAG pipelines

    Args:
        body (QueryBuilderRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, QueryBuilderResult]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: QueryBuilderRequest,
) -> Response[Union[Error, QueryBuilderResult]]:
    """Build a search query from natural language

     Uses an LLM to translate natural language search intent into a structured Bleve query.
    The generated query can be used directly in the QueryRequest.full_text_search or filter_query
    fields.

    This endpoint is useful for:
    - Building queries from user descriptions
    - Generating example queries for a table's schema
    - Agentic retrieval in RAG pipelines

    Args:
        body (QueryBuilderRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, QueryBuilderResult]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: QueryBuilderRequest,
) -> Optional[Union[Error, QueryBuilderResult]]:
    """Build a search query from natural language

     Uses an LLM to translate natural language search intent into a structured Bleve query.
    The generated query can be used directly in the QueryRequest.full_text_search or filter_query
    fields.

    This endpoint is useful for:
    - Building queries from user descriptions
    - Generating example queries for a table's schema
    - Agentic retrieval in RAG pipelines

    Args:
        body (QueryBuilderRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, QueryBuilderResult]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
