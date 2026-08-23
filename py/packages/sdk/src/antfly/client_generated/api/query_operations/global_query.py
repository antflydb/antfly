from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.exact_sort_error import ExactSortError
from ...models.graph_anchor_filter_requires_index_error import GraphAnchorFilterRequiresIndexError
from ...models.graph_cross_range_mode_unsupported_error import GraphCrossRangeModeUnsupportedError
from ...models.graph_distinct_budget_exceeded_error import GraphDistinctBudgetExceededError
from ...models.graph_match_operation_limit_exceeded_error import GraphMatchOperationLimitExceededError
from ...models.hierarchy_cursor_stale_error import HierarchyCursorStaleError
from ...models.query_candidate_budget_exceeded_error import QueryCandidateBudgetExceededError
from ...models.query_request import QueryRequest
from ...models.query_responses import QueryResponses
from ...models.query_temporarily_unavailable_error import QueryTemporarilyUnavailableError
from ...models.table_storage_unreadable_error import TableStorageUnreadableError
from ...types import File, Response


def _get_kwargs(
    *,
    body: QueryRequest | File,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/query",
    }

    if isinstance(body, QueryRequest):
        _kwargs["json"] = body.to_dict()

        headers["Content-Type"] = "application/json"
    if isinstance(body, File):
        _kwargs["content"] = body.payload

        headers["Content-Type"] = "application/x-ndjson"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphCrossRangeModeUnsupportedError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | QueryResponses
    | QueryTemporarilyUnavailableError
    | None
):
    if response.status_code == 200:
        response_200 = QueryResponses.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 409:
        response_409 = HierarchyCursorStaleError.from_dict(response.json())

        return response_409

    if response.status_code == 422:

        def _parse_response_422(
            data: object,
        ) -> (
            ExactSortError
            | GraphAnchorFilterRequiresIndexError
            | GraphCrossRangeModeUnsupportedError
            | GraphDistinctBudgetExceededError
            | GraphMatchOperationLimitExceededError
            | QueryCandidateBudgetExceededError
        ):
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_0 = ExactSortError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_1 = QueryCandidateBudgetExceededError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_2 = GraphDistinctBudgetExceededError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_3 = GraphAnchorFilterRequiresIndexError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_4 = GraphCrossRangeModeUnsupportedError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_query_unprocessable_error_type_5 = GraphMatchOperationLimitExceededError.from_dict(data)

            return componentsschemas_query_unprocessable_error_type_5

        response_422 = _parse_response_422(response.json())

        return response_422

    if response.status_code == 500:

        def _parse_response_500(data: object) -> Error | TableStorageUnreadableError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_500_type_0 = Error.from_dict(data)

                return response_500_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_500_type_1 = TableStorageUnreadableError.from_dict(data)

            return response_500_type_1

        response_500 = _parse_response_500(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = QueryTemporarilyUnavailableError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphCrossRangeModeUnsupportedError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | QueryResponses
    | QueryTemporarilyUnavailableError
]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: QueryRequest | File,
) -> Response[
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphCrossRangeModeUnsupportedError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | QueryResponses
    | QueryTemporarilyUnavailableError
]:
    r"""Perform a global query

     Executes a query across all relevant tables and shards based on the query content.

    ## Query Examples

    **Full-text search:**
    ```json
    {
      \"table\": \"wikipedia\",
      \"full_text_search\": {\"query\": \"body:computer\"},
      \"limit\": 10
    }
    ```

    **Semantic search:**
    ```json
    {
      \"table\": \"articles\",
      \"semantic_search\": \"artificial intelligence applications\",
      \"indexes\": [\"title_body_embedding\"],
      \"limit\": 20
    }
    ```

    **Hybrid search (RRF):**
    ```json
    {
      \"table\": \"products\",
      \"full_text_search\": {\"query\": \"laptop gaming\"},
      \"semantic_search\": \"high performance gaming computers\",
      \"indexes\": [\"product_embedding\"],
      \"filter_query\": {\"query\": \"+price:<2000 +in_stock:true\"},
      \"fields\": [\"name\", \"price\", \"description\"],
      \"limit\": 15
    }
    ```

    **With filtering:**
    ```json
    {
      \"table\": \"users\",
      \"filter_prefix\": \"tenant:acme:\",
      \"full_text_search\": {\"query\": \"active:true\"},
      \"exclusion_query\": {\"query\": \"status:deleted\"},
      \"limit\": 50
    }
    ```

    **NDJSON format:**
    For bulk queries, send multiple queries as NDJSON with `Content-Type: application/x-ndjson`.
    Each line must end with `\n`:
    ```
    {\"table\":\"wiki\",\"semantic_search\":\"AI\",\"indexes\":[\"emb\"],\"limit\":5}
    {\"table\":\"docs\",\"full_text_search\":{\"query\":\"tutorial\"},\"limit\":10}
    ```

    Args:
        body (QueryRequest):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | Error | TableStorageUnreadableError | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphCrossRangeModeUnsupportedError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | QueryCandidateBudgetExceededError | HierarchyCursorStaleError | QueryResponses | QueryTemporarilyUnavailableError]
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
    body: QueryRequest | File,
) -> (
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphCrossRangeModeUnsupportedError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | QueryResponses
    | QueryTemporarilyUnavailableError
    | None
):
    r"""Perform a global query

     Executes a query across all relevant tables and shards based on the query content.

    ## Query Examples

    **Full-text search:**
    ```json
    {
      \"table\": \"wikipedia\",
      \"full_text_search\": {\"query\": \"body:computer\"},
      \"limit\": 10
    }
    ```

    **Semantic search:**
    ```json
    {
      \"table\": \"articles\",
      \"semantic_search\": \"artificial intelligence applications\",
      \"indexes\": [\"title_body_embedding\"],
      \"limit\": 20
    }
    ```

    **Hybrid search (RRF):**
    ```json
    {
      \"table\": \"products\",
      \"full_text_search\": {\"query\": \"laptop gaming\"},
      \"semantic_search\": \"high performance gaming computers\",
      \"indexes\": [\"product_embedding\"],
      \"filter_query\": {\"query\": \"+price:<2000 +in_stock:true\"},
      \"fields\": [\"name\", \"price\", \"description\"],
      \"limit\": 15
    }
    ```

    **With filtering:**
    ```json
    {
      \"table\": \"users\",
      \"filter_prefix\": \"tenant:acme:\",
      \"full_text_search\": {\"query\": \"active:true\"},
      \"exclusion_query\": {\"query\": \"status:deleted\"},
      \"limit\": 50
    }
    ```

    **NDJSON format:**
    For bulk queries, send multiple queries as NDJSON with `Content-Type: application/x-ndjson`.
    Each line must end with `\n`:
    ```
    {\"table\":\"wiki\",\"semantic_search\":\"AI\",\"indexes\":[\"emb\"],\"limit\":5}
    {\"table\":\"docs\",\"full_text_search\":{\"query\":\"tutorial\"},\"limit\":10}
    ```

    Args:
        body (QueryRequest):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | Error | TableStorageUnreadableError | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphCrossRangeModeUnsupportedError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | QueryCandidateBudgetExceededError | HierarchyCursorStaleError | QueryResponses | QueryTemporarilyUnavailableError
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: QueryRequest | File,
) -> Response[
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphCrossRangeModeUnsupportedError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | QueryResponses
    | QueryTemporarilyUnavailableError
]:
    r"""Perform a global query

     Executes a query across all relevant tables and shards based on the query content.

    ## Query Examples

    **Full-text search:**
    ```json
    {
      \"table\": \"wikipedia\",
      \"full_text_search\": {\"query\": \"body:computer\"},
      \"limit\": 10
    }
    ```

    **Semantic search:**
    ```json
    {
      \"table\": \"articles\",
      \"semantic_search\": \"artificial intelligence applications\",
      \"indexes\": [\"title_body_embedding\"],
      \"limit\": 20
    }
    ```

    **Hybrid search (RRF):**
    ```json
    {
      \"table\": \"products\",
      \"full_text_search\": {\"query\": \"laptop gaming\"},
      \"semantic_search\": \"high performance gaming computers\",
      \"indexes\": [\"product_embedding\"],
      \"filter_query\": {\"query\": \"+price:<2000 +in_stock:true\"},
      \"fields\": [\"name\", \"price\", \"description\"],
      \"limit\": 15
    }
    ```

    **With filtering:**
    ```json
    {
      \"table\": \"users\",
      \"filter_prefix\": \"tenant:acme:\",
      \"full_text_search\": {\"query\": \"active:true\"},
      \"exclusion_query\": {\"query\": \"status:deleted\"},
      \"limit\": 50
    }
    ```

    **NDJSON format:**
    For bulk queries, send multiple queries as NDJSON with `Content-Type: application/x-ndjson`.
    Each line must end with `\n`:
    ```
    {\"table\":\"wiki\",\"semantic_search\":\"AI\",\"indexes\":[\"emb\"],\"limit\":5}
    {\"table\":\"docs\",\"full_text_search\":{\"query\":\"tutorial\"},\"limit\":10}
    ```

    Args:
        body (QueryRequest):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | Error | TableStorageUnreadableError | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphCrossRangeModeUnsupportedError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | QueryCandidateBudgetExceededError | HierarchyCursorStaleError | QueryResponses | QueryTemporarilyUnavailableError]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: QueryRequest | File,
) -> (
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphCrossRangeModeUnsupportedError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | QueryResponses
    | QueryTemporarilyUnavailableError
    | None
):
    r"""Perform a global query

     Executes a query across all relevant tables and shards based on the query content.

    ## Query Examples

    **Full-text search:**
    ```json
    {
      \"table\": \"wikipedia\",
      \"full_text_search\": {\"query\": \"body:computer\"},
      \"limit\": 10
    }
    ```

    **Semantic search:**
    ```json
    {
      \"table\": \"articles\",
      \"semantic_search\": \"artificial intelligence applications\",
      \"indexes\": [\"title_body_embedding\"],
      \"limit\": 20
    }
    ```

    **Hybrid search (RRF):**
    ```json
    {
      \"table\": \"products\",
      \"full_text_search\": {\"query\": \"laptop gaming\"},
      \"semantic_search\": \"high performance gaming computers\",
      \"indexes\": [\"product_embedding\"],
      \"filter_query\": {\"query\": \"+price:<2000 +in_stock:true\"},
      \"fields\": [\"name\", \"price\", \"description\"],
      \"limit\": 15
    }
    ```

    **With filtering:**
    ```json
    {
      \"table\": \"users\",
      \"filter_prefix\": \"tenant:acme:\",
      \"full_text_search\": {\"query\": \"active:true\"},
      \"exclusion_query\": {\"query\": \"status:deleted\"},
      \"limit\": 50
    }
    ```

    **NDJSON format:**
    For bulk queries, send multiple queries as NDJSON with `Content-Type: application/x-ndjson`.
    Each line must end with `\n`:
    ```
    {\"table\":\"wiki\",\"semantic_search\":\"AI\",\"indexes\":[\"emb\"],\"limit\":5}
    {\"table\":\"docs\",\"full_text_search\":{\"query\":\"tutorial\"},\"limit\":10}
    ```

    Args:
        body (QueryRequest):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | Error | TableStorageUnreadableError | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphCrossRangeModeUnsupportedError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | QueryCandidateBudgetExceededError | HierarchyCursorStaleError | QueryResponses | QueryTemporarilyUnavailableError
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
