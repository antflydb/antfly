from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.inference_capacity_error import InferenceCapacityError
from ...models.query_builder_request import QueryBuilderRequest
from ...models.query_builder_result import QueryBuilderResult
from ...models.query_temporarily_unavailable_error import QueryTemporarilyUnavailableError
from ...types import Response


def _get_kwargs(
    *,
    body: QueryBuilderRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/agents/query-builder",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryBuilderResult | None:
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

    if response.status_code == 503:

        def _parse_response_503(data: object) -> InferenceCapacityError | QueryTemporarilyUnavailableError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_503_type_0 = QueryTemporarilyUnavailableError.from_dict(data)

                return response_503_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_503_type_1 = InferenceCapacityError.from_dict(data)

            return response_503_type_1

        response_503 = _parse_response_503(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryBuilderResult]:
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
) -> Response[Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryBuilderResult]:
    """Build a search query from natural language

     Uses an LLM to translate natural language search intent into a structured Antfly query.
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
        Response[Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryBuilderResult]
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
) -> Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryBuilderResult | None:
    """Build a search query from natural language

     Uses an LLM to translate natural language search intent into a structured Antfly query.
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
        Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryBuilderResult
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: QueryBuilderRequest,
) -> Response[Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryBuilderResult]:
    """Build a search query from natural language

     Uses an LLM to translate natural language search intent into a structured Antfly query.
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
        Response[Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryBuilderResult]
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
) -> Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryBuilderResult | None:
    """Build a search query from natural language

     Uses an LLM to translate natural language search intent into a structured Antfly query.
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
        Error | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryBuilderResult
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
